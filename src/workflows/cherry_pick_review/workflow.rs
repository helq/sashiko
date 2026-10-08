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
