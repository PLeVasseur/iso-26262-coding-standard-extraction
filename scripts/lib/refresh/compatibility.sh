# shellcheck shell=bash

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
