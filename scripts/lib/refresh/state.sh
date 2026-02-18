# shellcheck shell=bash

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
