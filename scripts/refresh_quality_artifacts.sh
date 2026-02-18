#!/usr/bin/env bash
set -euo pipefail

CACHE_ROOT="${CACHE_ROOT:-.cache/iso26262}"
PART="${PART:-6}"
MAX_PAGES="${MAX_PAGES:-60}"
FULL_TARGET_SET="${FULL_TARGET_SET:-0}"
TARGET_PARTS="${TARGET_PARTS:-2 6 8 9}"
FULL_MAX_PAGES="${FULL_MAX_PAGES:-0}"
SEMANTIC_MODEL_ID="${SEMANTIC_MODEL_ID:-miniLM-L6-v2-local-v1}"
PHASE_ID="${PHASE_ID:-phase-8}"
PHASE_NAME="${PHASE_NAME:-Phase 8 - Deterministic runbook and crash recovery}"
BASE_BRANCH="${BASE_BRANCH:-main}"
UPDATE_DECISIONS="${UPDATE_DECISIONS:-1}"
RUNBOOK_VERSION="${RUNBOOK_VERSION:-1.0}"
EXPECTED_DB_SCHEMA_VERSION="${EXPECTED_DB_SCHEMA_VERSION:-0.4.0}"
REBUILD_ON_COMPAT_MISMATCH="${REBUILD_ON_COMPAT_MISMATCH:-0}"
ALLOW_BLOCKED_RESUME="${ALLOW_BLOCKED_RESUME:-0}"

MANIFEST_DIR="${CACHE_ROOT}/manifests"
RUN_STATE_PATH="${MANIFEST_DIR}/run_state.json"
DECISIONS_PATH="${MANIFEST_DIR}/decisions_log.jsonl"
REPORT_PATH="${MANIFEST_DIR}/extraction_quality_report.json"
TARGET_SECTIONS_JSON="${MANIFEST_DIR}/target_sections.json"
TARGET_SECTIONS_CSV="${MANIFEST_DIR}/target_sections.csv"
TRACEABILITY_PATH="${MANIFEST_DIR}/traceability_matrix.csv"
DB_PATH="${CACHE_ROOT}/iso26262_index.sqlite"

CURRENT_STEP="R00-PREFLIGHT"
START_STEP="R04-TARGET-REFRESH"
RESUME_FROM_STEP=""
RUN_ID=""
ENGINE_VERSION=""
COMPAT_STATUS="unknown"
COMPAT_REASON=""
LATEST_INGEST_PATH=""
LAST_SUCCESSFUL_COMMAND=""
NEXT_PLANNED_COMMAND=""
LAST_SUCCESSFUL_ARTIFACT=""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/refresh/env.sh"
source "${SCRIPT_DIR}/lib/refresh/state.sh"
source "${SCRIPT_DIR}/lib/refresh/compatibility.sh"
source "${SCRIPT_DIR}/lib/refresh/steps.sh"
source "${SCRIPT_DIR}/lib/refresh/decisions.sh"

trap 'handle_error $LINENO' ERR

require_cmd cargo
require_cmd jq
require_cmd git

mkdir -p "$MANIFEST_DIR"

resolve_engine_version
ensure_run_id
resolve_resume_behavior

CURRENT_STEP="R00-PREFLIGHT"
ensure_mainline_branch
LAST_SUCCESSFUL_COMMAND="git rev-parse --abbrev-ref HEAD"
LAST_SUCCESSFUL_ARTIFACT="branch:$(current_branch)"

CURRENT_STEP="R01-CONFIG-VALIDATION"
ensure_config_path
NEXT_PLANNED_COMMAND="R04 target refresh"
write_running_state "$CURRENT_STEP" "$NEXT_PLANNED_COMMAND"

CURRENT_STEP="R02-DIRECTORY-CHECK"
mkdir -p "$CACHE_ROOT" "$MANIFEST_DIR"
LAST_SUCCESSFUL_COMMAND="mkdir -p ${CACHE_ROOT} ${MANIFEST_DIR}"
LAST_SUCCESSFUL_ARTIFACT="path:${MANIFEST_DIR}"

CURRENT_STEP="R03-COMPATIBILITY-CHECK"
evaluate_compatibility
NEXT_PLANNED_COMMAND="R04 target refresh"
write_running_state "$CURRENT_STEP" "$NEXT_PLANNED_COMMAND"

if should_run_step "R04-TARGET-REFRESH"; then
  CURRENT_STEP="R04-TARGET-REFRESH"
  NEXT_PLANNED_COMMAND="cargo run -- inventory --cache-root ${CACHE_ROOT}"
  write_running_state "$CURRENT_STEP" "$NEXT_PLANNED_COMMAND"
  cargo run -- inventory --cache-root "$CACHE_ROOT"
  refresh_target_sections_artifacts
fi

if should_run_step "R05-INGEST"; then
  CURRENT_STEP="R05-INGEST"
  ingest_cmd=(cargo run -- ingest --cache-root "$CACHE_ROOT")
  if [[ "$FULL_TARGET_SET" == "1" ]]; then
    read -r -a target_parts_array <<< "$TARGET_PARTS"
    if [[ "${#target_parts_array[@]}" -eq 0 ]]; then
      fail "R05 ingest failed: TARGET_PARTS is empty while FULL_TARGET_SET=1"
    fi
    for target_part in "${target_parts_array[@]}"; do
      ingest_cmd+=(--target-part "$target_part")
    done
    if [[ "$FULL_MAX_PAGES" != "0" && -n "$FULL_MAX_PAGES" ]]; then
      ingest_cmd+=(--max-pages-per-doc "$FULL_MAX_PAGES")
    fi
  else
    ingest_cmd+=(--target-part "$PART" --max-pages-per-doc "$MAX_PAGES")
  fi

  ingest_cmd_display="${ingest_cmd[*]}"
  NEXT_PLANNED_COMMAND="$ingest_cmd_display"
  write_running_state "$CURRENT_STEP" "$NEXT_PLANNED_COMMAND"
  cargo check
  "${ingest_cmd[@]}"

  LATEST_INGEST_PATH="$(latest_ingest_manifest "$MANIFEST_DIR")"
  annotate_ingest_manifest_rebuild_reason "$LATEST_INGEST_PATH"
  RUN_ID="$(jq -r '.run_id // empty' "$LATEST_INGEST_PATH")"
  if [[ -z "$RUN_ID" ]]; then
    fail "latest ingest manifest is missing run_id: ${LATEST_INGEST_PATH}"
  fi

  LAST_SUCCESSFUL_COMMAND="$ingest_cmd_display"
  LAST_SUCCESSFUL_ARTIFACT="manifest:$(basename "$LATEST_INGEST_PATH")"
fi

if [[ -z "$LATEST_INGEST_PATH" && -d "$MANIFEST_DIR" ]]; then
  if compgen -G "${MANIFEST_DIR}/ingest_run_*.json" >/dev/null; then
    LATEST_INGEST_PATH="$(latest_ingest_manifest "$MANIFEST_DIR")"
    RUN_ID="$(jq -r '.run_id // empty' "$LATEST_INGEST_PATH")"
  fi
fi

if should_run_step "R06-VALIDATE"; then
  CURRENT_STEP="R06-VALIDATE"
  NEXT_PLANNED_COMMAND="cargo run -- validate --cache-root ${CACHE_ROOT}"
  write_running_state "$CURRENT_STEP" "$NEXT_PLANNED_COMMAND"
  cargo run -- query --cache-root "$CACHE_ROOT" --query "9.1" --part "$PART" --with-ancestors --with-descendants --json --limit 3 >/dev/null
  cargo run -- query --cache-root "$CACHE_ROOT" --query "Table 3" --part "$PART" --with-ancestors --with-descendants --json --limit 1 >/dev/null
  cargo run -- embed --cache-root "$CACHE_ROOT" --model-id "$SEMANTIC_MODEL_ID" --refresh-mode missing-or-stale --batch-size 64
  cargo run -- validate --cache-root "$CACHE_ROOT"

  if [[ ! -f "$REPORT_PATH" ]]; then
    fail "quality report not found at ${REPORT_PATH}"
  fi

  if [[ "$FULL_TARGET_SET" == "1" ]]; then
    if ! jq -e '.status == "passed" and .summary.failed == 0 and .summary.pending == 0' "$REPORT_PATH" >/dev/null; then
      fail "quality report did not pass all checks in full-target mode"
    fi
  else
    if ! jq -e '([.checks[] | select(.check_id != "Q-022" and .result != "pass")] | length) == 0 and .summary.pending == 0' "$REPORT_PATH" >/dev/null; then
      fail "quality report failed checks other than Q-022 in quick mode"
    fi
  fi

  LAST_SUCCESSFUL_COMMAND="cargo run -- validate --cache-root ${CACHE_ROOT}"
  LAST_SUCCESSFUL_ARTIFACT="report:$(basename "$REPORT_PATH")"
fi

if should_run_step "R07-TRACEABILITY"; then
  CURRENT_STEP="R07-TRACEABILITY"
  NEXT_PLANNED_COMMAND="ensure traceability_matrix.csv"
  write_running_state "$CURRENT_STEP" "$NEXT_PLANNED_COMMAND"
  ensure_traceability_matrix
fi

if should_run_step "R08-QUALITY-REPORT"; then
  CURRENT_STEP="R08-QUALITY-REPORT"
  NEXT_PLANNED_COMMAND="R09 artifact refresh and decision logging"
  write_running_state "$CURRENT_STEP" "$NEXT_PLANNED_COMMAND"

  if [[ ! -f "$REPORT_PATH" ]]; then
    fail "R08 quality report step missing report file: ${REPORT_PATH}"
  fi
fi

CURRENT_STEP="R09-ARTIFACT-REFRESH"
NEXT_PLANNED_COMMAND="Monitor quality trend and extend gold references as needed"
write_completed_state "$CURRENT_STEP" "$NEXT_PLANNED_COMMAND"

if [[ "$UPDATE_DECISIONS" == "1" ]]; then
  if [[ -z "$LATEST_INGEST_PATH" ]]; then
    LATEST_INGEST_PATH="$(latest_ingest_manifest "$MANIFEST_DIR")"
  fi
  append_decision_entry "$DECISIONS_PATH" "$RUN_ID" "$REPORT_PATH" "$(utc_now)" "$LATEST_INGEST_PATH"
  log "Appended decision log entry to ${DECISIONS_PATH}"
fi

log "Refreshed manifests and run-state artifacts successfully"
if [[ -n "$LATEST_INGEST_PATH" ]]; then
  log "Latest ingest manifest: ${LATEST_INGEST_PATH}"
fi
log "Quality report: ${REPORT_PATH}"
log "Run state: ${RUN_STATE_PATH}"
