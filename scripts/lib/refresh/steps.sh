# shellcheck shell=bash

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
