# shellcheck shell=bash

append_decision_entry() {
  local decisions_path="$1"
  local run_id="$2"
  local report_path="$3"
  local updated_at="$4"
  local latest_ingest_path="$5"
  local quality_status=""
  local passed=""
  local total=""
  local sparse_ratio=""
  local overloaded_ratio=""
  local marker_coverage=""
  local marker_accuracy=""
  local paragraph_accuracy=""
  local structural_invariants=""
  local asil_alignment=""
  local last_decision_id="D-0000"
  local next_number=""
  local next_decision_id=""
  local last_raw_number=""

  if [[ -f "$decisions_path" && -s "$decisions_path" ]]; then
    last_decision_id="$(jq -s -r '.[-1].decision_id // "D-0000"' "$decisions_path")"
  fi

  last_raw_number="${last_decision_id#D-}"
  next_number=$((10#${last_raw_number} + 1))
  next_decision_id="$(printf 'D-%04d' "$next_number")"

  quality_status="$(jq -r '.status // "unknown"' "$report_path")"
  passed="$(jq -r '.summary.passed // 0' "$report_path")"
  total="$(jq -r '.summary.total_checks // 0' "$report_path")"
  sparse_ratio="$(jq -r '.table_quality_scorecard.table_sparse_row_ratio // "n/a"' "$report_path")"
  overloaded_ratio="$(jq -r '.table_quality_scorecard.table_overloaded_row_ratio // "n/a"' "$report_path")"
  marker_coverage="$(jq -r '(.checks[] | select(.check_id == "Q-015").result) // "n/a"' "$report_path")"
  marker_accuracy="$(jq -r '(.checks[] | select(.check_id == "Q-016").result) // "n/a"' "$report_path")"
  paragraph_accuracy="$(jq -r '(.checks[] | select(.check_id == "Q-017").result) // "n/a"' "$report_path")"
  structural_invariants="$(jq -r '(.checks[] | select(.check_id == "Q-018").result) // "n/a"' "$report_path")"
  asil_alignment="$(jq -r '(.checks[] | select(.check_id == "Q-019").result) // "n/a"' "$report_path")"

  jq -cn \
    --arg timestamp "$updated_at" \
    --arg decision_id "$next_decision_id" \
    --arg run_id "$run_id" \
    --arg quality_status "$quality_status" \
    --arg passed "$passed" \
    --arg total "$total" \
    --arg sparse_ratio "$sparse_ratio" \
    --arg overloaded_ratio "$overloaded_ratio" \
    --arg marker_coverage "$marker_coverage" \
    --arg marker_accuracy "$marker_accuracy" \
    --arg paragraph_accuracy "$paragraph_accuracy" \
    --arg structural_invariants "$structural_invariants" \
    --arg asil_alignment "$asil_alignment" \
    --arg ingest_manifest "$(basename "$latest_ingest_path")" \
    '{
      timestamp: $timestamp,
      decision_id: $decision_id,
      context: "C9 local quality artifact refresh",
      options_considered: [
        "refresh artifacts manually",
        "refresh artifacts with deterministic gate script"
      ],
      selected_option: "refresh artifacts with deterministic gate script",
      rationale: "Keeps ingest/query/validate evidence synchronized with run-state and quality thresholds.",
      impact: (
        "Run " + $run_id
        + " refreshed via " + $ingest_manifest
        + "; quality status=" + $quality_status
        + " (" + $passed + "/" + $total + " checks passed)"
        + "; sparse=" + $sparse_ratio
        + ", overloaded=" + $overloaded_ratio
        + ", Q-015=" + $marker_coverage
        + ", Q-016=" + $marker_accuracy
        + ", Q-017=" + $paragraph_accuracy
        + ", Q-018=" + $structural_invariants
        + ", Q-019=" + $asil_alignment
      )
    }' >> "$decisions_path"
  printf '\n' >> "$decisions_path"
}
