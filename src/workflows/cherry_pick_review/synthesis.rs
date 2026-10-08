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

//! Prompt builders for cherry-pick synthesis stages (8, 9, 10, origin, 11).
//!
//! Each synthesis stage takes the instruction text and produces a user prompt
//! that injects the accumulated concerns/findings and carries a per-stage
//! output schema. These templates are copied verbatim from the original
//! implementation so the workflow reproduces its behaviour exactly.

use super::workflow::CherryPickState;

pub use super::prompts::ANALYSIS_FORMAT_GUIDANCE;

/// Builds the `(user_prompt, clean_user_prompt)` for a synthesis stage from its
/// instruction text and the state accumulated so far.
pub type StagePromptBuilder = Box<dyn Fn(&str, &CherryPickState) -> (String, String) + Send + Sync>;

#[allow(dead_code)]
fn concerns_json(state: &CherryPickState) -> String {
    serde_json::to_string_pretty(&state.concerns).unwrap_or_default()
}

#[allow(dead_code)]
fn dismissed_json(state: &CherryPickState) -> String {
    serde_json::to_string_pretty(&state.dismissed_concerns).unwrap_or_default()
}

#[allow(dead_code)]
fn findings_json(state: &CherryPickState) -> String {
    match &state.findings {
        Some(v) => serde_json::to_string_pretty(v).unwrap_or_default(),
        None => "[]".to_string(),
    }
}
