#!/usr/bin/env bash
set -euo pipefail

CACHE_ROOT="${CACHE_ROOT:-.cache/iso26262}"
PART="${PART:-6}"
MAX_PAGES="${MAX_PAGES:-60}"
PHASE_ID="${PHASE_ID:-phase-7}"
PHASE_NAME="${PHASE_NAME:-Phase 7 - Regression and determinism}"
BASE_BRANCH="${BASE_BRANCH:-main}"
UPDATE_DECISIONS="${UPDATE_DECISIONS:-1}"

log() {
  printf '[refresh] %s\n' "$*"
}

fail() {
  printf '[refresh][FAIL] %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    fail "required command not found: $1"
  fi
}

latest_ingest_manifest() {
  local manifest_dir="$1"
  local latest=""
  local candidate=""

  for candidate in "$manifest_dir"/ingest_run_*.json; do
    if [[ ! -e "$candidate" ]]; then
      continue
    fi

    if [[ -z "$latest" || "$candidate" > "$latest" ]]; then
      latest="$candidate"
    fi
  done

  if [[ -z "$latest" ]]; then
    fail "no ingest_run_*.json files found under ${manifest_dir}"
  fi

  printf '%s\n' "$latest"
}

utc_now() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

write_run_state() {
  local run_state_path="$1"
  local run_id="$2"
  local current_step="$3"
  local status="$4"
  local last_successful_command="$5"
  local next_planned_command="$6"
  local last_successful_artifact="$7"
  local updated_at="$8"
  local started_at=""
  local active_branch=""
  local last_commit=""

  if [[ -f "$run_state_path" ]]; then
    started_at="$(jq -r '.started_at // empty' "$run_state_path")"
  fi
  if [[ -z "$started_at" ]]; then
    started_at="$updated_at"
  fi

  active_branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || printf 'main')"
  last_commit="$(git rev-parse --short HEAD 2>/dev/null || printf '')"

  jq -n \
    --arg run_id "$run_id" \
    --arg phase "$PHASE_NAME" \
    --arg phase_id "$PHASE_ID" \
    --arg current_step "$current_step" \
    --arg status "$status" \
    --arg base_branch "$BASE_BRANCH" \
    --arg active_branch "$active_branch" \
    --arg commit_mode "mainline" \
    --arg last_commit "$last_commit" \
    --arg last_successful_command "$last_successful_command" \
    --arg next_planned_command "$next_planned_command" \
    --arg started_at "$started_at" \
    --arg updated_at "$updated_at" \
    --arg last_successful_artifact "$last_successful_artifact" \
    '{
      manifest_version: 1,
      active_run_id: $run_id,
      current_phase: $phase,
      phase_id: $phase_id,
      current_step: $current_step,
      status: $status,
      base_branch: $base_branch,
      active_branch: $active_branch,
      commit_mode: $commit_mode,
      last_commit: $last_commit,
      last_successful_command: $last_successful_command,
      next_planned_command: $next_planned_command,
      started_at: $started_at,
      updated_at: $updated_at,
      last_successful_artifact: $last_successful_artifact
    }' > "$run_state_path"
}

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
      )
    }' >> "$decisions_path"
  printf '\n' >> "$decisions_path"
}

require_cmd cargo
require_cmd jq
require_cmd git

MANIFEST_DIR="${CACHE_ROOT}/manifests"
RUN_STATE_PATH="${MANIFEST_DIR}/run_state.json"
DECISIONS_PATH="${MANIFEST_DIR}/decisions_log.jsonl"
REPORT_PATH="${MANIFEST_DIR}/extraction_quality_report.json"

mkdir -p "$MANIFEST_DIR"

log "Running gate command bundle"
cargo check
cargo run -- ingest --cache-root "$CACHE_ROOT" --target-part "$PART" --max-pages-per-doc "$MAX_PAGES"

LATEST_INGEST_PATH="$(latest_ingest_manifest "$MANIFEST_DIR")"
RUN_ID="$(jq -r '.run_id // empty' "$LATEST_INGEST_PATH")"
if [[ -z "$RUN_ID" ]]; then
  fail "latest ingest manifest is missing run_id: ${LATEST_INGEST_PATH}"
fi

UPDATED_AT="$(utc_now)"
write_run_state \
  "$RUN_STATE_PATH" \
  "$RUN_ID" \
  "R08-QUALITY-REPORT" \
  "running" \
  "cargo run -- ingest --cache-root ${CACHE_ROOT} --target-part ${PART} --max-pages-per-doc ${MAX_PAGES}" \
  "cargo run -- validate --cache-root ${CACHE_ROOT}" \
  "manifest:$(basename "$LATEST_INGEST_PATH")" \
  "$UPDATED_AT"

cargo run -- query --cache-root "$CACHE_ROOT" --query "9.1" --part "$PART" --with-ancestors --with-descendants --json --limit 3 >/dev/null
cargo run -- query --cache-root "$CACHE_ROOT" --query "Table 3" --part "$PART" --with-ancestors --with-descendants --json --limit 1 >/dev/null
cargo run -- validate --cache-root "$CACHE_ROOT"

if [[ ! -f "$REPORT_PATH" ]]; then
  fail "quality report not found at ${REPORT_PATH}"
fi

if ! jq -e '.status == "passed" and .summary.failed == 0 and .summary.pending == 0' "$REPORT_PATH" >/dev/null; then
  fail "quality report did not pass all checks"
fi

UPDATED_AT="$(utc_now)"
write_run_state \
  "$RUN_STATE_PATH" \
  "$RUN_ID" \
  "R09-ARTIFACT-REFRESH" \
  "completed" \
  "scripts/refresh_quality_artifacts.sh" \
  "Monitor quality trend and extend marker/paragraph gold references as needed" \
  "report:$(basename "$REPORT_PATH")" \
  "$UPDATED_AT"

if [[ "$UPDATE_DECISIONS" == "1" ]]; then
  append_decision_entry "$DECISIONS_PATH" "$RUN_ID" "$REPORT_PATH" "$UPDATED_AT" "$LATEST_INGEST_PATH"
  log "Appended decision log entry to ${DECISIONS_PATH}"
fi

log "Refreshed manifests and run-state artifacts successfully"
log "Latest ingest manifest: ${LATEST_INGEST_PATH}"
log "Quality report: ${REPORT_PATH}"
log "Run state: ${RUN_STATE_PATH}"
