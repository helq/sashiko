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

use serde_json::Value;

use crate::ai::AiProvider;
use crate::toolbox::ToolBox;
use crate::worker::prompts::PromptRegistry;

/// Declarative workflow definition for reviewing cherry-pick conflict resolutions.
#[derive(Debug, Clone)]
pub struct CherryPickReviewWorkflow {
    pub original_sha: String,
    pub base_sha: Option<String>,
    pub resolution_sha: String,
    pub stages: Option<Vec<String>>,
}

impl CherryPickReviewWorkflow {
    pub fn new(original_sha: impl Into<String>, resolution_sha: impl Into<String>) -> Self {
        Self {
            original_sha: original_sha.into(),
            base_sha: None,
            resolution_sha: resolution_sha.into(),
            stages: None,
        }
    }

    pub fn from_review_kind(
        kind: &crate::review_kind::ReviewKind,
        resolution_sha: impl Into<String>,
    ) -> Self {
        match kind {
            crate::review_kind::ReviewKind::CherryPick {
                original_sha,
                base_sha,
            } => Self {
                original_sha: original_sha.clone(),
                base_sha: base_sha.clone(),
                resolution_sha: resolution_sha.into(),
                stages: None,
            },
        }
    }

    pub fn with_base_sha(mut self, base_sha: impl Into<String>) -> Self {
        self.base_sha = Some(base_sha.into());
        self
    }

    pub fn with_stages(mut self, stages: Option<Vec<String>>) -> Self {
        self.stages = stages;
        self
    }

    #[allow(dead_code, unused_variables)]
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

        Ok(CherryPickState {
            original_sha: self.original_sha.clone(),
            resolution_sha: self.resolution_sha.clone(),
            original_diff,
            target_commit: resolution_diff_with_log,
            target_commit_diff: resolution_diff,
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
