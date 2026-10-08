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

#[allow(dead_code)]
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

#[allow(dead_code)]
async fn git_subject(worktree: &Path, sha: &str) -> Option<String> {
    git_output(
        worktree,
        &["show", "-s", "--format=%s", "--end-of-options", sha],
    )
    .await
    .map(|s| s.trim().to_string())
}

#[allow(dead_code)]
async fn git_show(worktree: &Path, sha: &str) -> Option<String> {
    git_output(worktree, &["show", "--end-of-options", sha]).await
}
