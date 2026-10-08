TodoWrite compatibility: vendored prompts may ask you to add tasks or suspected bugs to TodoWrite. Do not call or mention TodoWrite. Treat those instructions as an internal checklist only. If that checklist identifies a concrete suspected bug, carry it forward as a JSON concern with file, function_or_symbol, line when known, triggering condition, and evidence. Do not output generic checklist progress as a concern.

Once you have gathered sufficient information, return ONLY a JSON object with "concerns" and "dismissed_concerns" arrays.
If you find no concerns and no dismissed concerns, return `{"concerns": [], "dismissed_concerns": []}`.
If you find concerns, each must be an object with:
- "type": A short category string.
- "description": A clear description of the problem.
- "reasoning": A step-by-step explanation.
- "locations": An array of objects, each containing "file", "function_or_symbol", "line", "code_snippet", and "why_this_location_matters". Use `null` for "file", "function_or_symbol", or "line" when an issue is non-local or the exact value is not known. Do not invent line numbers; use `line: null` when the exact lines are not known and explain the triggering condition in "reasoning".
(Do NOT attempt to classify whether an issue is pre-existing; origin classification is performed by dedicated downstream stages).

Use the "dismissed_concerns" array ONLY for candidate concerns that you considered plausible, investigated, and disproved with concrete evidence. This is especially important when you first suspect a concern and then follow the evidence chain proving that it does NOT apply.
If you find dismissed_concerns, each must use the same item schema as concerns:
- "type": A short category string.
- "description": The candidate concern that was investigated and disproved.
- "reasoning": A step-by-step explanation of the evidence proving the candidate concern does not apply.
- "locations": An array of objects, each containing "file", "function_or_symbol", "line", "code_snippet", and "why_this_location_matters". Use `null` for unknown values. Do not invent line numbers.

CRITICAL REVIEW DIRECTIVE: Do NOT dismiss concerns just because you assume the surrounding system or caller handles it perfectly. Do not be overly charitable to the existing code. If there is a missing initialization, an unhandled edge case, or a brittle logic flow, report it as a concern immediately. Assume the worst-case scenario where external inputs and caller states are malformed.

Example:
```json
{
  "concerns": [
    {
      "type": "Issue Category",
      "description": "What is wrong.",
      "reasoning": "Why it is wrong.",
      "locations": [
        {
          "file": "path/to/file.c",
          "function_or_symbol": "function_name",
          "line": 120,
          "code_snippet": "problematic_code();",
          "why_this_location_matters": "This is where the newly allocated resource is dropped on the error path."
        }
      ]
    }
  ],
  "dismissed_concerns": [
    {
      "type": "Issue Category",
      "description": "Possible missing cleanup when foo_init() fails after bar_alloc().",
      "reasoning": "The concrete code path or ordering that proves this candidate concern does not apply.",
      "locations": [
        {
          "file": "path/to/file.c",
          "function_or_symbol": "function_name",
          "line": 145,
          "code_snippet": "safe_code_path();",
          "why_this_location_matters": "This is where the cleanup path proves the candidate leak does not apply."
        }
      ]
    }
  ]
}
```
