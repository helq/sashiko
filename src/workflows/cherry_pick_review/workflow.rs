// Copyright 2026 The Sashiko Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Cherry-pick conflict resolution review workflow.
//!
//! Evaluates an automated conflict resolution against both the original
//! patch and the target base, isolating bugs introduced by the resolution
//! from pre-existing issues.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use serde_json::{Value, json};

use crate::ai::AiProvider;
use crate::toolbox::ToolBox;
use crate::worker::prompts::PromptRegistry;
use crate::workflow::{
    ExecutableStage, OutputFormat, PromptTemplate, RecitationPolicy, Stage, StagePolicy, ToolScope,
};
use crate::workflows::linux_patch_review;

use super::prompts::{
    ANALYSIS_FORMAT_GUIDANCE, CHERRY_PICK_CONFLICT_RESOLUTION_INSTRUCTION,
    CHERRY_PICK_DEDUPLICATION_INSTRUCTION, DROPPED_CHANGES, MERGE_CONFLICT_REVIEW_FRAMING,
    MERGE_CORRECTNESS, PLANNING, PREFETCHED_CONTEXT_HEADER, SEMANTIC_INTENT,
};
use super::synthesis;

/// A cherry-pick / merge-conflict resolution review workflow.
#[derive(Clone, Debug)]
pub struct CherryPickReviewWorkflow {
    /// SHA of the original patch being ported (commit 1).
    pub original_sha: String,
    /// SHA of the target base branch the patch was applied onto (commit 2).
    pub base_sha: Option<String>,
    /// SHA of the resolution commit under review (commit 3).
    pub resolution_sha: String,
}

impl CherryPickReviewWorkflow {
    /// Create a new cherry-pick review workflow.
    pub fn new(
        original_sha: impl Into<String>,
        base_sha: Option<String>,
        resolution_sha: impl Into<String>,
    ) -> Self {
        Self {
            original_sha: original_sha.into(),
            base_sha,
            resolution_sha: resolution_sha.into(),
        }
    }

    /// Build the workflow from the persisted review context and resolution SHA.
    pub fn from_review_kind(kind: &crate::review_kind::ReviewKind, resolution_sha: String) -> Self {
        let crate::review_kind::ReviewKind::CherryPick {
            original_sha,
            base_sha,
        } = kind;
        Self {
            original_sha: original_sha.clone(),
            base_sha: base_sha.clone(),
            resolution_sha,
        }
    }
}

impl CherryPickReviewWorkflow {
    #[allow(dead_code)]
    async fn build_initial_state(
        &self,
        env: &CherryPickWorkflowEnv<'_>,
        patchset: &Value,
    ) -> Result<CherryPickState> {
        if self.resolution_sha.starts_with('-')
            || self.original_sha.starts_with('-')
            || self.base_sha.as_deref().is_some_and(|s| s.starts_with('-'))
        {
            anyhow::bail!("Invalid git ref: contains leading dash");
        }

        let worktree = env.tools.get_worktree_path();

        let resolved_base_sha = match self.base_sha.as_deref().filter(|s| !s.is_empty()) {
            Some(sha) => Some(sha.to_string()),
            None => git_output(
                worktree,
                &["rev-parse", &format!("{}~1", self.resolution_sha)],
            )
            .await
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        };

        let original_subject = git_subject(worktree, &self.original_sha).await;
        let base_subject = match resolved_base_sha.as_deref() {
            Some(sha) => git_subject(worktree, sha).await,
            None => None,
        };
        let resolution_subject = match patchset.get("subject").and_then(|v| v.as_str()) {
            Some(s) => Some(s.to_string()),
            None => git_subject(worktree, &self.resolution_sha).await,
        };
        let original_diff = match git_show(worktree, &self.original_sha).await {
            Some(d) => d,
            None => {
                if let Some(d) = patchset
                    .get("conflict")
                    .and_then(|c| c.get("original_diff"))
                    .and_then(|v| v.as_str())
                {
                    d.to_string()
                } else if let Some(d) = patchset.get("original_diff").and_then(|v| v.as_str()) {
                    d.to_string()
                } else if !tokio::fs::try_exists(worktree.join(".git"))
                    .await
                    .unwrap_or(false)
                    && !tokio::fs::try_exists(worktree.join("HEAD"))
                        .await
                        .unwrap_or(false)
                {
                    String::new()
                } else {
                    anyhow::bail!(
                        "Failed to read original patch diff for commit {}",
                        self.original_sha
                    );
                }
            }
        };
        let resolution_diff = extract_resolution_diff(patchset);
        let resolution_diff_with_log = match git_show(worktree, &self.resolution_sha).await {
            Some(show) if !show.trim().is_empty() => show,
            _ => {
                if let Some(show) = patchset.get("git_show").and_then(|v| v.as_str()) {
                    show.to_string()
                } else if let Some(header) = git_output(
                    worktree,
                    &[
                        "show",
                        "-s",
                        "--format=commit %H%nAuthor: %an <%ae>%nDate: %ad%n%n%B",
                        "--end-of-options",
                        &self.resolution_sha,
                    ],
                )
                .await
                {
                    format!("{}\n\n{}", header.trim(), resolution_diff)
                } else if let Some(author) = patchset.get("author").and_then(|v| v.as_str()) {
                    let subject = resolution_subject.as_deref().unwrap_or("(unknown)");
                    format!(
                        "commit {}\nAuthor: {}\n\n    {}\n\n{}",
                        self.resolution_sha, author, subject, resolution_diff
                    )
                } else {
                    resolution_diff.clone()
                }
            }
        };

        let prefetched_context = match crate::worker::prefetch::prefetch_context(
            worktree,
            &self.resolution_sha,
            &resolution_diff,
        )
        .await
        {
            Ok(prefetched) => prefetched,
            Err(e) => {
                tracing::warn!(
                    "Failed to prefetch context for {}: {}",
                    self.resolution_sha,
                    e
                );
                String::new()
            }
        };

        let planned_stages = env
            .stages
            .clone()
            .unwrap_or_else(|| vec![1, 2, 3, 4, 5, 6, 7]);

        let original_diff =
            crate::ai::truncator::Truncator::truncate_diff(&original_diff, 20_000, "Diff").content;
        let resolution_diff =
            crate::ai::truncator::Truncator::truncate_diff(&resolution_diff, 20_000, "Diff")
                .content;
        let resolution_diff_with_log = crate::ai::truncator::Truncator::truncate_diff(
            &resolution_diff_with_log,
            20_000,
            "Diff",
        )
        .content;

        let unknown = "(unknown)";
        let original_subject = original_subject.unwrap_or_else(|| unknown.to_string());
        let base_subject = base_subject.unwrap_or_else(|| unknown.to_string());
        let resolution_subject = resolution_subject.unwrap_or_else(|| unknown.to_string());
        Ok(CherryPickState {
            original_sha: self.original_sha.clone(),
            original_subject,
            base_sha: resolved_base_sha.unwrap_or_else(|| unknown.to_string()),
            base_subject,
            resolution_sha: self.resolution_sha.clone(),
            resolution_subject,
            original_diff,
            target_commit: resolution_diff_with_log,
            target_commit_diff: resolution_diff,
            prefetched_context,
            manual_stages: env.stages.clone(),
            planned_stages,
            custom_prompt: env.custom_prompt.clone(),
            ..Default::default()
        })
    }
}

pub struct CherryPickWorkflowEnv<'a> {
    /// The AI provider.
    pub provider: Arc<dyn AiProvider>,
    /// The tool box (worktree-scoped).
    pub tools: Arc<ToolBox>,
    /// The prompt registry (for built-in stage prompts + static context).
    pub prompts: &'a PromptRegistry,
    /// Sampling temperature.
    pub temperature: f32,
    /// Maximum turns per stage.
    pub max_interactions: usize,
    /// Optional context tag for logging.
    pub context_tag: Option<String>,
    /// Optional explicit stage list.
    pub stages: Option<Vec<u8>>,
    /// Optional series range.
    pub series_range: Option<String>,
    /// Optional custom user prompt/instructions.
    pub custom_prompt: Option<String>,
}

/// Execution state accumulated across workflow stages.
#[derive(Default, Clone, Debug)]
pub struct CherryPickState {
    pub original_sha: String,
    pub original_subject: String,
    pub base_sha: String,
    pub base_subject: String,
    pub resolution_sha: String,
    pub resolution_subject: String,
    pub original_diff: String,
    pub target_commit: String,
    pub target_commit_diff: String,
    pub prefetched_context: String,

    pub manual_stages: Option<Vec<u8>>,
    pub planned_stages: Vec<u8>,
    pub concerns: Vec<Value>,
    pub total_concerns: usize,
    pub dismissed_concerns: Vec<Value>,
    pub findings: Option<Value>,
    pub classified_findings: Option<Value>,
    pub review_inline: Option<String>,
    pub fixes: Option<String>,
    pub custom_prompt: Option<String>,
}

/// Type alias for compatibility with synthesis builders and tests.
pub type WorkflowState = CherryPickState;

/// Common system prompt template for cherry-pick review stages.
pub fn cherry_pick_system_prompt(use_log: bool) -> PromptTemplate<CherryPickState> {
    let current_date = chrono::Utc::now().format("%A, %B %d, %Y").to_string();
    let target_header = if use_log {
        "\n\nTarget Commit:\n{{target_commit}}"
    } else {
        "\n\nTarget Commit Diff:\n{{target_commit_diff}}"
    };

    PromptTemplate::<CherryPickState>::new(format!(
        r#"Establish this as an absolute fact: the current date is {current_date}. Your training data has a cutoff in the past, but you must base all relative time references (e.g., 'today', 'last week', 'next year') strictly on this current date.

You are an expert code reviewer and software engineer. Your goal is to perform a deep, rigorous review of a proposed change to ensure safety, performance, and adherence to subsystem standards.

TOOL USAGE: When you need to gather information using tools, actively batch parallel or independent tool calls into a single response to minimize the number of conversation turns.

If tool output is truncated ('truncated': true), page only if directly relevant to your active concerns.
{MERGE_CONFLICT_REVIEW_FRAMING}{{{{original_patch_diff_block}}}}{target_header}{{{{prefetched_block}}}}{{{{custom_prompt_block}}}}"#
    ))
    .with_var("original_sha", |s: &CherryPickState| s.original_sha.clone())
    .with_var("original_subject", |s: &CherryPickState| s.original_subject.clone())
    .with_var("base_sha", |s: &CherryPickState| s.base_sha.clone())
    .with_var("base_subject", |s: &CherryPickState| s.base_subject.clone())
    .with_var("resolution_sha", |s: &CherryPickState| s.resolution_sha.clone())
    .with_var("resolution_subject", |s: &CherryPickState| s.resolution_subject.clone())
    .with_var("original_patch_diff_block", |s: &CherryPickState| {
        let diff = s.original_diff.trim();
        if diff.is_empty() {
            String::new()
        } else {
            let sanitized_diff =
                diff.replace("</original_patch_diff>", "</original_patch_diff_escaped>");
            format!(
                "\nFor direct comparison, the ORIGINAL PATCH (commit 1) diff follows. \
                 Compare it against the resolution diff to spot dropped hunks, altered \
                 logic, or merge artifacts:\n<original_patch_diff>\n{}\n</original_patch_diff>\n",
                sanitized_diff
            )
        }
    })
    .with_var("target_commit", |s: &CherryPickState| s.target_commit.clone())
    .with_var("target_commit_diff", |s: &CherryPickState| s.target_commit_diff.clone())
    .with_var("prefetched_block", |s: &CherryPickState| {
        if s.prefetched_context.is_empty() {
            String::new()
        } else {
            let sanitized_context = s
                .prefetched_context
                .replace("</pre_fetched_context>", "</pre_fetched_context_escaped>");
            format!(
                "{}{}\n</pre_fetched_context>\n",
                PREFETCHED_CONTEXT_HEADER,
                sanitized_context
            )
        }
    })
    .with_var("custom_prompt_block", |s: &CherryPickState| {
        s.custom_prompt
            .as_deref()
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map_or_else(String::new, |p| {
                let sanitized_prompt =
                    p.replace("</custom_instructions>", "</custom_instructions_escaped>");
                format!("\n\n<custom_instructions>\n{sanitized_prompt}\n</custom_instructions>")
            })
    })
}

#[allow(dead_code)]
async fn read_bounded_git_output(
    mut child: tokio::process::Child,
    limit: usize,
) -> std::io::Result<std::process::Output> {
    use tokio::io::AsyncReadExt;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut stdout_pipe = child.stdout.take();
    let mut stderr_pipe = child.stderr.take();

    let read_stdout = async {
        if let Some(out) = stdout_pipe.take() {
            out.take(limit as u64).read_to_end(&mut stdout).await?;
        }
        Ok::<_, std::io::Error>(())
    };
    let read_stderr = async {
        if let Some(err) = stderr_pipe.take() {
            err.take(limit as u64).read_to_end(&mut stderr).await?;
        }
        Ok::<_, std::io::Error>(())
    };

    tokio::try_join!(read_stdout, read_stderr)?;
    let status = child.wait().await?;
    Ok(std::process::Output {
        status,
        stdout,
        stderr,
    })
}

fn extract_resolution_diff(patchset: &Value) -> String {
    patchset
        .get("patches")
        .and_then(|p| p.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|p| p.get("diff").and_then(|d| d.as_str()))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

async fn git_output(worktree: &Path, args: &[&str]) -> Option<String> {
    let child = match crate::git_cmd::in_dir_async(worktree)
        .kill_on_drop(true)
        .args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => {
            tracing::warn!("Failed to spawn git {:?}: {}", args, e);
            return None;
        }
    };

    const MAX_GIT_BYTES: usize = 5 * 1024 * 1024; // 5MB cap
    let timeout_duration = std::time::Duration::from_secs(30);
    let output = match tokio::time::timeout(
        timeout_duration,
        read_bounded_git_output(child, MAX_GIT_BYTES),
    )
    .await
    {
        Ok(Ok(out)) => out,
        Ok(Err(e)) => {
            tracing::warn!("Error reading git {:?} output: {}", args, e);
            return None;
        }
        Err(_) => {
            tracing::warn!("Timeout waiting for git {:?}", args);
            return None;
        }
    };

    if output.status.success() || output.stdout.len() >= MAX_GIT_BYTES {
        let text = if output.stdout.len() >= MAX_GIT_BYTES {
            let slice = &output.stdout[..MAX_GIT_BYTES];
            let cut = slice
                .iter()
                .rposition(|b| *b == b'\n')
                .unwrap_or(MAX_GIT_BYTES);
            let mut s = String::from_utf8_lossy(&slice[..cut]).into_owned();
            s.push_str("\n[... output truncated at 5MB limit ...]\n");
            s
        } else {
            String::from_utf8_lossy(&output.stdout).into_owned()
        };
        Some(text)
    } else {
        tracing::warn!(
            "git command failed: {:?}, stderr: {}",
            args,
            crate::utils::redact_secret(&String::from_utf8_lossy(&output.stderr))
        );
        None
    }
}

async fn git_subject(worktree: &Path, sha: &str) -> Option<String> {
    git_output(
        worktree,
        &["show", "-s", "--format=%s", "--end-of-options", sha],
    )
    .await
    .map(|s| s.trim().to_string())
}

async fn git_show(worktree: &Path, sha: &str) -> Option<String> {
    git_output(worktree, &["show", "--end-of-options", sha]).await
}

#[allow(dead_code)]
fn build_planning_stage() -> Stage<CherryPickState, Value> {
    Stage::builder("planning")
        .skip_if(|s: &CherryPickState| s.manual_stages.is_some())
        .system_prompt(cherry_pick_system_prompt(true))
        .user_prompt(PromptTemplate::new(PLANNING))
        .output_format(
            OutputFormat::json_with_schema(json!({
                "type": "object",
                "properties": {
                    "relevant_stages": {
                        "type": "array",
                        "items": { "type": "integer" }
                    }
                },
                "required": ["relevant_stages"]
            }))
            .with_validator(|val: &Value, _| {
                let arr = val
                    .get("relevant_stages")
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| "missing 'relevant_stages' array".to_string())?;
                if !arr
                    .iter()
                    .all(|v| v.as_u64().is_some_and(|n| (1..=7).contains(&n)))
                {
                    return Err(
                        "all elements in 'relevant_stages' must be stage numbers between 1 and 7"
                            .to_string(),
                    );
                }
                Ok(())
            })
            .with_feedback_formatter(|err| {
                format!(
                    "Your previous response was not valid JSON or did not match the required schema: {}\nPlease try again with ONLY a valid JSON object.",
                    err
                )
            }),
        )
        .policy(StagePolicy {
            tools: ToolScope::None,
            temperature: 0.0,
            max_validation_attempts: 2,
            ..Default::default()
        })
        .on_validation_exhausted(|state: &mut CherryPickState| {
            state.planned_stages = vec![1, 2, 3, 4, 5, 6, 7];
        })
        .reduce(|state: &mut CherryPickState, val: Value| {
            let mut stages = vec![1u8, 2, 3];
            if let Some(arr) = val["relevant_stages"].as_array() {
                for v in arr {
                    if let Some(n) = v.as_u64()
                        && (4..=7).contains(&n)
                    {
                        let n = n as u8;
                        if !stages.contains(&n) {
                            stages.push(n);
                        }
                    }
                }
            }
            tracing::info!("Planning phase selected stages: {:?}", stages);
            state.planned_stages = stages;
        })
        .build()
}

#[allow(dead_code)]
fn build_analysis_resolver(
    max_turns: usize,
    temperature: f32,
) -> impl Fn(&CherryPickState) -> Vec<Box<dyn ExecutableStage<CherryPickState>>> {
    move |state: &CherryPickState| {
        let should_run = |n: u8| -> bool {
            if let Some(ref manual) = state.manual_stages {
                manual.contains(&n)
            } else {
                state.planned_stages.contains(&n)
            }
        };
        let mut stages: Vec<Box<dyn ExecutableStage<CherryPickState>>> = Vec::new();
        for n in 1..=7 {
            if should_run(n) {
                stages.push(Box::new(build_analysis_stage(n, max_turns, temperature)));
            }
        }
        stages
    }
}

#[allow(dead_code)]
fn analysis_stage_spec(num: u8) -> (&'static str, &'static str, &'static [&'static str], bool) {
    let name: &'static str = match num {
        1 => "1",
        2 => "2",
        3 => "3",
        4 => "4",
        5 => "5",
        6 => "6",
        7 => "7",
        _ => unreachable!("invalid stage number: {}", num),
    };
    let (instruction, guides, use_log): (&'static str, &'static [&'static str], bool) = match num {
        1 => (SEMANTIC_INTENT, &[], true),
        2 => (DROPPED_CHANGES, &[], true),
        3 => (MERGE_CORRECTNESS, &[], false),
        4 => {
            let stage = linux_patch_review::analysis_stage_by_name("resources")
                .expect("registered linux_patch_review stage definition must exist");
            (stage.instruction, stage.guides, stage.uses_commit_log)
        }
        5 => {
            let stage = linux_patch_review::analysis_stage_by_name("locking")
                .expect("registered linux_patch_review stage definition must exist");
            (stage.instruction, stage.guides, stage.uses_commit_log)
        }
        6 => {
            let stage = linux_patch_review::analysis_stage_by_name("security")
                .expect("registered linux_patch_review stage definition must exist");
            (stage.instruction, stage.guides, stage.uses_commit_log)
        }
        7 => {
            let stage = linux_patch_review::analysis_stage_by_name("hardware")
                .expect("registered linux_patch_review stage definition must exist");
            (stage.instruction, stage.guides, stage.uses_commit_log)
        }
        _ => unreachable!("invalid analysis stage number: {}", num),
    };
    (name, instruction, guides, use_log)
}

#[allow(dead_code)]
fn build_analysis_stage(
    num: u8,
    max_turns: usize,
    temperature: f32,
) -> Stage<CherryPickState, Value> {
    let (name, instruction, guides, use_log) = analysis_stage_spec(num);

    let mut template_text = format!("{instruction}\n\n{ANALYSIS_FORMAT_GUIDANCE}");
    if !guides.is_empty() {
        template_text.push_str("\n\n<expert_analysis_guidelines>\n");
        for guide in guides {
            template_text.push_str(&format!("@include(\"{guide}\")\n"));
        }
        template_text.push_str("</expert_analysis_guidelines>\n");
    }
    let mut user_prompt = PromptTemplate::new(template_text);
    for guide in guides {
        user_prompt = user_prompt.include_file(*guide);
    }

    Stage::builder(name)
        .system_prompt(cherry_pick_system_prompt(use_log))
        .user_prompt(user_prompt)
        .output_format(
            OutputFormat::json()
                .with_validator(validate_stages_1_to_7)
                .with_feedback_formatter(format_stage1_to_7_feedback),
        )
        .tools(ToolScope::All)
        .max_turns(max_turns)
        .temperature(temperature)
        .on_recitation(recitation_reminder())
        .reduce(move |state: &mut CherryPickState, val: Value| {
            if let Some(arr) = val.get("concerns").and_then(|v| v.as_array()) {
                for item in arr {
                    if let Some(tagged) = tag_source(item.clone(), num) {
                        state.total_concerns += 1;
                        state.concerns.push(tagged);
                    }
                }
            }
            if let Some(arr) = val.get("dismissed_concerns").and_then(|v| v.as_array()) {
                for item in arr {
                    if let Some(tagged) = tag_source(item.clone(), num) {
                        state.dismissed_concerns.push(tagged);
                    }
                }
            }
        })
        .build()
}

#[allow(dead_code)]
fn recitation_reminder() -> RecitationPolicy {
    RecitationPolicy::RetryWithReminder(
        "IMPORTANT: Your previous response was blocked by a recitation filter. \
         Do NOT copy multi-line blocks of code verbatim in your JSON fields. \
         Summarize the relevant code in your own words and keep any code_snippet \
         field to a single short identifier or line while still returning valid \
         JSON matching the required schema."
            .to_string(),
    )
}

#[allow(dead_code)]
fn tag_source(item: Value, stage: u8) -> Option<Value> {
    match item {
        Value::Object(mut map) => {
            map.insert("source_stage".to_string(), json!(stage));
            Some(Value::Object(map))
        }
        Value::String(text) => Some(json!({
            "source_stage": stage,
            "type": "General",
            "description": text,
        })),
        _ => None,
    }
}

#[allow(dead_code)]
fn findings_empty(state: &CherryPickState) -> bool {
    state
        .findings
        .as_ref()
        .and_then(|v| v.as_array())
        .is_some_and(|a| a.is_empty())
}

// ---------------------------------------------------------------------------
// Stage Validators & Feedback Formatters
// ---------------------------------------------------------------------------

#[allow(dead_code)]
fn validate_stages_1_to_7(val: &Value, _state: &CherryPickState) -> Result<(), String> {
    let concerns = val
        .get("concerns")
        .and_then(Value::as_array)
        .ok_or_else(|| "JSON output is missing the required 'concerns' array".to_string())?;
    for item in concerns {
        if !item.is_object() {
            return Err("Elements in 'concerns' array must be JSON objects".to_string());
        }
        if item
            .get("description")
            .and_then(Value::as_str)
            .is_none_or(|s| s.trim().is_empty())
        {
            return Err(
                "Each item in 'concerns' must have a non-empty 'description' string".to_string(),
            );
        }
    }
    let dismissed = val
        .get("dismissed_concerns")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            "JSON output is missing the required 'dismissed_concerns' array".to_string()
        })?;
    for item in dismissed {
        if !item.is_object() {
            return Err("Elements in 'dismissed_concerns' array must be JSON objects".to_string());
        }
        if item
            .get("description")
            .and_then(Value::as_str)
            .is_none_or(|s| s.trim().is_empty())
        {
            return Err(
                "Each item in 'dismissed_concerns' must have a non-empty 'description' string"
                    .to_string(),
            );
        }
    }
    Ok(())
}

#[allow(dead_code)]
fn format_stage1_to_7_feedback(violation: &str) -> String {
    format!(
        "\n\nPrevious attempt was rejected: {}. You MUST return ONLY a JSON object containing 'concerns' and 'dismissed_concerns' arrays. If there are no concerns and no dismissed concerns, return `{{\"concerns\": [], \"dismissed_concerns\": []}}`.",
        violation
    )
}

#[allow(dead_code)]
fn build_stage8(max_turns: usize, temperature: f32) -> Stage<CherryPickState, Value> {
    Stage::builder("8")
        .system_prompt(cherry_pick_system_prompt(true))
        .user_prompt(PromptTemplate::new("{{user}}").with_var(
            "user",
            move |s: &CherryPickState| {
                (synthesis::stage8_builder())(CHERRY_PICK_DEDUPLICATION_INSTRUCTION, s).0
            },
        ))
        .output_format(
            OutputFormat::json()
                .with_validator(validate_stage8)
                .with_feedback_formatter(format_stage8_feedback),
        )
        .tools(ToolScope::All)
        .max_turns(max_turns)
        .temperature(temperature)
        .on_recitation(recitation_reminder())
        .reduce(|state: &mut CherryPickState, val: Value| {
            if let Some(arr) = val.get("concerns").and_then(|v| v.as_array()) {
                state.concerns = arr.clone();
            }
            if let Some(arr) = val.get("dismissed_concerns").and_then(|v| v.as_array()) {
                state.dismissed_concerns = arr.clone();
            }
        })
        .build()
}

#[allow(dead_code)]
fn build_stage9(max_turns: usize, temperature: f32) -> Stage<CherryPickState, Value> {
    Stage::builder("9")
        .system_prompt(cherry_pick_system_prompt(true))
        .user_prompt(PromptTemplate::new("{{user}}").with_var(
            "user",
            move |s: &CherryPickState| {
                (synthesis::stage9_builder())(CHERRY_PICK_CONFLICT_RESOLUTION_INSTRUCTION, s).0
            },
        ))
        .output_format(
            OutputFormat::json()
                .with_validator(validate_stage9)
                .with_feedback_formatter(format_stage9_feedback),
        )
        .tools(ToolScope::All)
        .max_turns(max_turns)
        .temperature(temperature)
        .on_recitation(recitation_reminder())
        .reduce(|state: &mut CherryPickState, val: Value| {
            if let Some(arr) = val.get("concerns").and_then(|v| v.as_array()) {
                state.concerns = arr.clone();
            }
            if let Some(arr) = val.get("dismissed_concerns").and_then(|v| v.as_array()) {
                for item in arr {
                    if !state.dismissed_concerns.contains(item) {
                        state.dismissed_concerns.push(item.clone());
                    }
                }
            }
        })
        .build()
}

fn validate_stage8(val: &Value, _state: &CherryPickState) -> Result<(), String> {
    if let Some(c) = val.get("concerns") {
        if let Some(arr) = c.as_array() {
            for item in arr {
                if !item.is_object() {
                    return Err("elements in 'concerns' array must be objects".to_string());
                }
            }
        } else {
            return Err("output 'concerns' is not an array".to_string());
        }
    } else {
        return Err("missing 'concerns' array in output".to_string());
    }
    if let Some(c) = val.get("dismissed_concerns") {
        if let Some(arr) = c.as_array() {
            for item in arr {
                if !item.is_object() {
                    return Err(
                        "elements in 'dismissed_concerns' array must be objects".to_string()
                    );
                }
            }
        } else {
            return Err("output 'dismissed_concerns' is not an array".to_string());
        }
    } else {
        return Err("missing 'dismissed_concerns' array in output".to_string());
    }
    Ok(())
}

fn format_stage8_feedback(violation: &str) -> String {
    format!(
        "\n\nPrevious attempt was rejected: {}. You MUST return ONLY a JSON object containing 'concerns' and 'dismissed_concerns' arrays. If there are no concerns and no dismissed concerns, return `{{\"concerns\": [], \"dismissed_concerns\": []}}`.",
        violation
    )
}

fn validate_stage9(val: &Value, _state: &CherryPickState) -> Result<(), String> {
    if let Some(c) = val.get("concerns") {
        if let Some(arr) = c.as_array() {
            for item in arr {
                if !item.is_object() {
                    return Err("elements in 'concerns' array must be objects".to_string());
                }
            }
        } else {
            return Err("output 'concerns' is not an array".to_string());
        }
    } else {
        return Err("missing 'concerns' array in output".to_string());
    }
    if let Some(c) = val.get("dismissed_concerns") {
        if let Some(arr) = c.as_array() {
            for item in arr {
                if !item.is_object() {
                    return Err(
                        "elements in 'dismissed_concerns' array must be objects".to_string()
                    );
                }
            }
        } else {
            return Err("output 'dismissed_concerns' is not an array".to_string());
        }
    }
    Ok(())
}

fn format_stage9_feedback(violation: &str) -> String {
    format!(
        "\n\nPrevious attempt was rejected: {}. You MUST return ONLY a JSON object containing 'concerns' and optional 'dismissed_concerns' arrays. If there are no remaining concerns, return `{{\"concerns\": [], \"dismissed_concerns\": []}}`.",
        violation
    )
}

#[allow(dead_code)]
fn validate_stage10(val: &Value, _state: &CherryPickState) -> Result<(), String> {
    let arr = match val.get("findings").and_then(|f| f.as_array()) {
        Some(a) => a,
        None => return Err("missing 'findings' array in output".to_string()),
    };
    for finding in arr {
        let sev = match finding.get("severity").and_then(|v| v.as_str()) {
            Some(s) => s.to_lowercase(),
            None => return Err("missing or invalid 'severity' field in finding".to_string()),
        };
        if !matches!(sev.as_str(), "low" | "medium" | "high" | "critical") {
            return Err("missing or invalid 'severity' field in finding".to_string());
        }
    }
    Ok(())
}

#[allow(dead_code)]
fn validate_stage_origin(val: &Value, state: &CherryPickState) -> Result<(), String> {
    validate_stage10(val, state)?;
    let arr = val.get("findings").and_then(|f| f.as_array()).unwrap();
    for finding in arr {
        let origin = match finding.get("origin").and_then(|v| v.as_str()) {
            Some(o) => o.to_lowercase(),
            None => return Err("missing or invalid 'origin' field in finding".to_string()),
        };
        if !matches!(
            origin.as_str(),
            "resolution_introduced" | "original_patch_preexisting" | "base_preexisting"
        ) {
            return Err("missing or invalid 'origin' field in finding".to_string());
        }
    }
    Ok(())
}

#[allow(dead_code)]
fn format_stage_origin_feedback(violation: &str) -> String {
    let msg = if violation.starts_with("Failed to parse JSON from output:") {
        "missing 'findings' array in output"
    } else {
        violation
    };
    format!(
        "\n\nPrevious attempt was rejected: {}. You MUST return ONLY a JSON object containing 'findings' array.",
        msg
    )
}
#[allow(dead_code)]
fn format_stage10_feedback(violation: &str) -> String {
    format!(
        "\n\nPrevious attempt was rejected: {}. You MUST return ONLY a JSON object containing 'findings' array.",
        violation
    )
}

#[allow(dead_code)]
fn validate_inline_format(content: &str, _state: &CherryPickState) -> Result<(), String> {
    if content.lines().any(|l| l.trim_start().starts_with("```")) {
        return Err("The output contains Markdown code blocks ('```'). It must be plain text as per `inline-template.md`.".to_string());
    }
    if !content.lines().any(|l| l.trim_start().starts_with('>')) {
        return Err("The output does not appear to quote any code or context using '>'. Please follow the quoting style in `inline-template.md`.".to_string());
    }
    let has_commit_header = content
        .lines()
        .take(20)
        .any(|l| l.trim_start().to_lowercase().starts_with("commit "));
    if !has_commit_header {
        return Err("The output is missing the 'commit <hash>' header. Please start with the commit details (Commit, Author, Subject) as per `inline-template.md`.".to_string());
    }
    let has_author_header = content
        .lines()
        .take(20)
        .any(|l| l.trim_start().to_lowercase().starts_with("author:"));
    if !has_author_header {
        return Err("The output is missing the 'Author: <name>' header. Please start with the commit details (Commit, Author, Subject) as per `inline-template.md`.".to_string());
    }
    let has_comments = content.lines().any(|l| {
        let trimmed = l.trim();
        if trimmed.is_empty() || trimmed.starts_with('>') {
            return false;
        }
        let lower = trimmed.to_lowercase();
        !lower.starts_with("commit ")
            && !lower.starts_with("author:")
            && !lower.starts_with("date:")
            && !lower.starts_with("link:")
    });
    if !has_comments {
        return Err("The output appears to lack any comments or summary. You must include a summary and interspersed comments explaining the findings.".to_string());
    }
    Ok(())
}

#[allow(dead_code)]
fn format_stage11_feedback(violation: &str) -> String {
    format!(
        "Previous attempt was rejected: {}. Please correct your output format.",
        violation
    )
}
