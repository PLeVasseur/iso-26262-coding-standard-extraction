#!/usr/bin/env bash
set -euo pipefail

CACHE_ROOT="${CACHE_ROOT:-.cache/iso26262}"
PART="${PART:-6}"
MAX_PAGES="${MAX_PAGES:-60}"
PHASE_ID="${PHASE_ID:-phase-8}"
PHASE_NAME="${PHASE_NAME:-Phase 8 - Deterministic runbook and crash recovery}"
BASE_BRANCH="${BASE_BRANCH:-main}"
UPDATE_DECISIONS="${UPDATE_DECISIONS:-1}"
RUNBOOK_VERSION="${RUNBOOK_VERSION:-1.0}"
EXPECTED_DB_SCHEMA_VERSION="${EXPECTED_DB_SCHEMA_VERSION:-0.3.0}"
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

utc_now() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

current_branch() {
  git rev-parse --abbrev-ref HEAD 2>/dev/null || printf 'main'
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

resolve_engine_version() {
  ENGINE_VERSION="$(cargo metadata --no-deps --format-version 1 | jq -r '[.packages[] | select(.name == "iso26262")][0].version // empty')"
  if [[ -z "$ENGINE_VERSION" ]]; then
    fail "unable to resolve iso26262 version from cargo metadata"
  fi
}

ensure_run_id() {
  if [[ -n "$RUN_ID" ]]; then
    return
  fi

  if [[ -f "$RUN_STATE_PATH" ]]; then
    RUN_ID="$(jq -r '.active_run_id // empty' "$RUN_STATE_PATH")"
  fi

  if [[ -z "$RUN_ID" ]]; then
    RUN_ID="run-$(date -u +%Y%m%dT%H%M%SZ)"
  fi
}

step_rank() {
  case "$1" in
    R00-*) printf '0\n' ;;
    R01-*) printf '1\n' ;;
    R02-*) printf '2\n' ;;
    R03-*) printf '3\n' ;;
    R04-*) printf '4\n' ;;
    R05-*) printf '5\n' ;;
    R06-*) printf '6\n' ;;
    R07-*) printf '7\n' ;;
    R08-*) printf '8\n' ;;
    R09-*) printf '9\n' ;;
    *) printf '4\n' ;;
  esac
}

normalize_step() {
  case "$1" in
    R04-*|R05-*|R06-*|R07-*|R08-*|R09-*) printf '%s\n' "$1" ;;
    *) printf 'R04-TARGET-REFRESH\n' ;;
  esac
}

should_run_step() {
  local step="$1"
  local step_num start_num
  step_num="$(step_rank "$step")"
  start_num="$(step_rank "$START_STEP")"
  [[ "$step_num" -ge "$start_num" ]]
}

write_run_state_internal() {
  local status="$1"
  local current_step="$2"
  local failed_step="$3"
  local failure_reason="$4"
  local next_planned_command="$5"
  local last_successful_command="$6"
  local last_successful_artifact="$7"
  local updated_at="$8"
  local started_at=""
  local active_branch=""
  local last_commit=""

  if [[ -f "$RUN_STATE_PATH" ]]; then
    started_at="$(jq -r '.started_at // empty' "$RUN_STATE_PATH")"
  fi
  if [[ -z "$started_at" ]]; then
    started_at="$updated_at"
  fi

  active_branch="$(current_branch)"
  last_commit="$(git rev-parse --short HEAD 2>/dev/null || printf '')"

  jq -n \
    --arg run_id "$RUN_ID" \
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
    --arg failed_step "$failed_step" \
    --arg failure_reason "$failure_reason" \
    --arg resume_from_step "$RESUME_FROM_STEP" \
    --arg runbook_version "$RUNBOOK_VERSION" \
    --arg engine_version "$ENGINE_VERSION" \
    --arg db_schema_version "$EXPECTED_DB_SCHEMA_VERSION" \
    --arg compatibility_status "$COMPAT_STATUS" \
    --arg compatibility_reason "$COMPAT_REASON" \
    '{
      manifest_version: 2,
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
      last_successful_artifact: $last_successful_artifact,
      failed_step: (if $failed_step == "" then null else $failed_step end),
      failure_reason: (if $failure_reason == "" then null else $failure_reason end),
      resume_from_step: (if $resume_from_step == "" then null else $resume_from_step end),
      restart_policy: {
        hard_block_on_compatibility_mismatch: true,
        rebuild_on_mismatch_enabled: ($ENV.REBUILD_ON_COMPAT_MISMATCH == "1")
      },
      compatibility: {
        runbook_version: $runbook_version,
        engine_version: $engine_version,
        db_schema_version: $db_schema_version,
        status: $compatibility_status,
        reason: (if $compatibility_reason == "" then null else $compatibility_reason end)
      }
    }' > "$RUN_STATE_PATH"
}

write_running_state() {
  local current_step="$1"
  local next_command="$2"
  write_run_state_internal "running" "$current_step" "" "" "$next_command" "$LAST_SUCCESSFUL_COMMAND" "$LAST_SUCCESSFUL_ARTIFACT" "$(utc_now)"
}

write_completed_state() {
  local current_step="$1"
  local next_command="$2"
  write_run_state_internal "completed" "$current_step" "" "" "$next_command" "$LAST_SUCCESSFUL_COMMAND" "$LAST_SUCCESSFUL_ARTIFACT" "$(utc_now)"
}

write_failed_state() {
  local current_step="$1"
  local reason="$2"
  write_run_state_internal "failed" "$current_step" "$current_step" "$reason" "$NEXT_PLANNED_COMMAND" "$LAST_SUCCESSFUL_COMMAND" "$LAST_SUCCESSFUL_ARTIFACT" "$(utc_now)"
}

write_blocked_state() {
  local current_step="$1"
  local reason="$2"
  write_run_state_internal "blocked" "$current_step" "$current_step" "$reason" "$NEXT_PLANNED_COMMAND" "$LAST_SUCCESSFUL_COMMAND" "$LAST_SUCCESSFUL_ARTIFACT" "$(utc_now)"
}

handle_error() {
  local line_no="$1"
  trap - ERR
  ensure_run_id
  local reason="command failed at ${CURRENT_STEP} (line ${line_no})"
  write_failed_state "$CURRENT_STEP" "$reason"
  printf '[refresh][FAIL] %s\n' "$reason" >&2
  exit 1
}

ensure_mainline_branch() {
  local active_branch
  active_branch="$(current_branch)"
  if [[ "$active_branch" != "$BASE_BRANCH" ]]; then
    fail "R00 branch check failed: expected '${BASE_BRANCH}', found '${active_branch}'"
  fi

  if [[ "$BASE_BRANCH" == "main" && "$active_branch" != "main" ]]; then
    fail "R00 mainline mode requires active branch 'main'"
  fi
}

ensure_config_path() {
  if [[ -z "${OPENCODE_CONFIG_DIR:-}" ]]; then
    fail "R01 config check failed: OPENCODE_CONFIG_DIR is not set"
  fi

  if [[ ! -d "${OPENCODE_CONFIG_DIR}" ]]; then
    fail "R01 config check failed: OPENCODE_CONFIG_DIR does not exist: ${OPENCODE_CONFIG_DIR}"
  fi
}

resolve_resume_behavior() {
  if [[ ! -f "$RUN_STATE_PATH" ]]; then
    return
  fi

  local previous_status expected_branch resume_step
  previous_status="$(jq -r '.status // "not_started"' "$RUN_STATE_PATH")"
  expected_branch="$(jq -r '.active_branch // empty' "$RUN_STATE_PATH")"

  if [[ -n "$expected_branch" && "$expected_branch" != "$(current_branch)" ]]; then
    fail "resume blocked: run_state expects branch '${expected_branch}'"
  fi

  case "$previous_status" in
    blocked)
      if [[ "$ALLOW_BLOCKED_RESUME" == "1" ]]; then
        log "Operator override enabled; resuming from blocked state at R04"
        RESUME_FROM_STEP="R04-TARGET-REFRESH"
        START_STEP="R04-TARGET-REFRESH"
      else
        fail "resume blocked: previous run_state status is 'blocked'"
      fi
      ;;
    running)
      resume_step="$(jq -r '.current_step // empty' "$RUN_STATE_PATH")"
      if [[ -n "$resume_step" ]]; then
        RESUME_FROM_STEP="$resume_step"
        START_STEP="$(normalize_step "$resume_step")"
        log "Resuming interrupted run from ${START_STEP}"
      fi
      ;;
    failed)
      resume_step="$(jq -r '.failed_step // .current_step // empty' "$RUN_STATE_PATH")"
      if [[ -n "$resume_step" ]]; then
        RESUME_FROM_STEP="$resume_step"
        START_STEP="$(normalize_step "$resume_step")"
        log "Resuming failed run from ${START_STEP}"
      fi
      ;;
    *)
      ;;
  esac
}

resolve_db_schema_version() {
  if command -v sqlite3 >/dev/null 2>&1 && [[ -f "$DB_PATH" ]]; then
    sqlite3 "$DB_PATH" "SELECT value FROM metadata WHERE key = 'db_schema_version' LIMIT 1;" 2>/dev/null | tr -d '\r\n'
    return
  fi

  if compgen -G "${MANIFEST_DIR}/ingest_run_*.json" >/dev/null; then
    local latest_manifest
    latest_manifest="$(latest_ingest_manifest "$MANIFEST_DIR")"
    jq -r '.db_schema_version // empty' "$latest_manifest"
    return
  fi

  printf '\n'
}

archive_db_for_rebuild() {
  if [[ ! -f "$DB_PATH" ]]; then
    return
  fi

  local archive_path
  archive_path="${CACHE_ROOT}/iso26262_index.$(date -u +%Y%m%dT%H%M%SZ).sqlite"
  mv "$DB_PATH" "$archive_path"
  LAST_SUCCESSFUL_COMMAND="archive_db_for_rebuild"
  LAST_SUCCESSFUL_ARTIFACT="archive:$(basename "$archive_path")"
  log "Archived DB before rebuild: ${archive_path}"
}

evaluate_compatibility() {
  local previous_engine previous_db_schema actual_db_schema reason
  local latest_manifest current_source_hashes last_source_hashes
  reason=""

  if [[ -f "$RUN_STATE_PATH" ]]; then
    previous_engine="$(jq -r '.compatibility.engine_version // empty' "$RUN_STATE_PATH")"
    previous_db_schema="$(jq -r '.compatibility.db_schema_version // empty' "$RUN_STATE_PATH")"

    if [[ -n "$previous_engine" && "$previous_engine" != "$ENGINE_VERSION" ]]; then
      reason="engine_version mismatch: previous=${previous_engine} current=${ENGINE_VERSION}"
    fi

    if [[ -z "$reason" && -n "$previous_db_schema" && "$previous_db_schema" != "$EXPECTED_DB_SCHEMA_VERSION" ]]; then
      reason="db_schema_version mismatch in run_state: previous=${previous_db_schema} expected=${EXPECTED_DB_SCHEMA_VERSION}"
    fi
  fi

  actual_db_schema="$(resolve_db_schema_version)"
  if [[ -z "$reason" && -n "$actual_db_schema" && "$actual_db_schema" != "$EXPECTED_DB_SCHEMA_VERSION" ]]; then
    reason="db schema mismatch in sqlite: found=${actual_db_schema} expected=${EXPECTED_DB_SCHEMA_VERSION}"
  fi

  if [[ -z "$reason" && -f "${MANIFEST_DIR}/pdf_inventory.json" ]] && compgen -G "${MANIFEST_DIR}/ingest_run_*.json" >/dev/null; then
    latest_manifest="$(latest_ingest_manifest "$MANIFEST_DIR")"
    current_source_hashes="$(jq -r '[.pdfs[].sha256] | sort | join(",")' "${MANIFEST_DIR}/pdf_inventory.json" 2>/dev/null || printf '')"
    last_source_hashes="$(jq -r '[.source_hashes[].sha256] | sort | join(",")' "$latest_manifest" 2>/dev/null || printf '')"

    if [[ -n "$current_source_hashes" && -n "$last_source_hashes" && "$current_source_hashes" != "$last_source_hashes" ]]; then
      reason="source hash set changed since latest ingest manifest"
    fi
  fi

  if [[ -n "$reason" ]]; then
    if [[ "$REBUILD_ON_COMPAT_MISMATCH" == "1" ]]; then
      COMPAT_STATUS="rebuild"
      COMPAT_REASON="$reason"
      RUN_ID="run-$(date -u +%Y%m%dT%H%M%SZ)"
      archive_db_for_rebuild
      START_STEP="R04-TARGET-REFRESH"
      RESUME_FROM_STEP=""
      log "Compatibility mismatch detected; proceeding with controlled rebuild"
      return
    fi

    COMPAT_STATUS="blocked"
    COMPAT_REASON="$reason"
    NEXT_PLANNED_COMMAND="Set REBUILD_ON_COMPAT_MISMATCH=1 to archive and rebuild safely"
    write_blocked_state "$CURRENT_STEP" "$reason"
    fail "compatibility mismatch blocked run: ${reason}"
  fi

  COMPAT_STATUS="ok"
  COMPAT_REASON=""
}

refresh_target_sections_artifacts() {
  if [[ ! -f "$TARGET_SECTIONS_JSON" ]]; then
    fail "R04 target refresh failed: missing ${TARGET_SECTIONS_JSON}"
  fi

  local tmp_json tmp_csv
  tmp_json="$(mktemp)"
  tmp_csv="$(mktemp)"

  jq --arg now "$(utc_now)" '.generated_at = $now' "$TARGET_SECTIONS_JSON" > "$tmp_json"
  mv "$tmp_json" "$TARGET_SECTIONS_JSON"

  jq -r '
    ["id","priority","part","year","ref","ref_type","why_it_matters","coding_standard_area","evidence_type","status"],
    (.targets[] | [
      .id,
      .priority,
      .part,
      .year,
      .ref,
      .ref_type,
      .why_it_matters,
      .coding_standard_area,
      .evidence_type,
      .status
    ])
    | @csv
  ' "$TARGET_SECTIONS_JSON" > "$tmp_csv"

  mv "$tmp_csv" "$TARGET_SECTIONS_CSV"

  LAST_SUCCESSFUL_COMMAND="refresh_target_sections_artifacts"
  LAST_SUCCESSFUL_ARTIFACT="manifest:$(basename "$TARGET_SECTIONS_JSON"),csv:$(basename "$TARGET_SECTIONS_CSV")"
}

ensure_traceability_matrix() {
  if [[ ! -f "$TARGET_SECTIONS_JSON" ]]; then
    fail "R07 traceability build failed: missing ${TARGET_SECTIONS_JSON}"
  fi

  local target_count trace_rows
  target_count="$(jq -r '.target_count // (.targets | length)' "$TARGET_SECTIONS_JSON")"

  if [[ ! -f "$TRACEABILITY_PATH" || ! -s "$TRACEABILITY_PATH" ]]; then
    jq -r '
      ["iso_ref","rule_id","verification_method","evidence_artifact","owner","status"],
      (.targets[] | [
        ("ISO 26262-" + (.part | tostring) + ":" + (.year | tostring) + " " + .ref),
        "",
        "",
        "",
        "",
        "planned"
      ])
      | @csv
    ' "$TARGET_SECTIONS_JSON" > "$TRACEABILITY_PATH"
    LAST_SUCCESSFUL_COMMAND="ensure_traceability_matrix"
    LAST_SUCCESSFUL_ARTIFACT="manifest:$(basename "$TRACEABILITY_PATH")"
    return
  fi

  trace_rows=$(( $(wc -l < "$TRACEABILITY_PATH") - 1 ))
  if [[ "$trace_rows" -lt "$target_count" ]]; then
    fail "R07 traceability matrix has fewer rows (${trace_rows}) than target count (${target_count})"
  fi

  LAST_SUCCESSFUL_COMMAND="ensure_traceability_matrix"
  LAST_SUCCESSFUL_ARTIFACT="manifest:$(basename "$TRACEABILITY_PATH")"
}

annotate_ingest_manifest_rebuild_reason() {
  local manifest_path="$1"

  if [[ "$COMPAT_STATUS" != "rebuild" || -z "$COMPAT_REASON" ]]; then
    return
  fi

  local tmp_manifest note_text
  tmp_manifest="$(mktemp)"
  note_text="controlled_rebuild_reason: ${COMPAT_REASON}"

  jq --arg note "$note_text" '.notes = ((.notes // []) + [$note])' "$manifest_path" > "$tmp_manifest"
  mv "$tmp_manifest" "$manifest_path"
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
  NEXT_PLANNED_COMMAND="cargo run -- ingest --cache-root ${CACHE_ROOT} --target-part ${PART} --max-pages-per-doc ${MAX_PAGES}"
  write_running_state "$CURRENT_STEP" "$NEXT_PLANNED_COMMAND"
  cargo check
  cargo run -- ingest --cache-root "$CACHE_ROOT" --target-part "$PART" --max-pages-per-doc "$MAX_PAGES"

  LATEST_INGEST_PATH="$(latest_ingest_manifest "$MANIFEST_DIR")"
  annotate_ingest_manifest_rebuild_reason "$LATEST_INGEST_PATH"
  RUN_ID="$(jq -r '.run_id // empty' "$LATEST_INGEST_PATH")"
  if [[ -z "$RUN_ID" ]]; then
    fail "latest ingest manifest is missing run_id: ${LATEST_INGEST_PATH}"
  fi

  LAST_SUCCESSFUL_COMMAND="cargo run -- ingest --cache-root ${CACHE_ROOT} --target-part ${PART} --max-pages-per-doc ${MAX_PAGES}"
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
  cargo run -- validate --cache-root "$CACHE_ROOT"

  if [[ ! -f "$REPORT_PATH" ]]; then
    fail "quality report not found at ${REPORT_PATH}"
  fi

  if ! jq -e '.status == "passed" and .summary.failed == 0 and .summary.pending == 0' "$REPORT_PATH" >/dev/null; then
    fail "quality report did not pass all checks"
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
