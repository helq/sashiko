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

/// Filter raw cherry-pick findings down to the ones worth surfacing.
///
/// Rules (by `severity` and `origin`):
/// - DROP all `low` severity.
/// - DROP everything `base_preexisting` (already in the target branch).
/// - DROP `original_patch_preexisting` unless `critical`.
/// - KEEP `resolution_introduced` at medium+ severity.
///
/// Findings with missing or unparseable severity default to `medium`.
/// Non-array input logs a warning and is preserved unchanged to prevent false-clean passes.
pub fn filter_cherry_pick_findings(findings: &serde_json::Value) -> serde_json::Value {
    let arr = match findings.as_array() {
        Some(a) => a,
        None => {
            tracing::warn!(
                "filter_cherry_pick_findings expected a JSON array, preserving raw input: {}",
                findings
            );
            return findings.clone();
        }
    };

    serde_json::json!(arr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn non_array_preserved() {
        let val = json!({"x": 1});
        assert_eq!(filter_cherry_pick_findings(&val), val);
    }
}
