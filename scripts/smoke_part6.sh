#!/usr/bin/env bash
set -euo pipefail

CACHE_ROOT="${CACHE_ROOT:-.cache/iso26262}"
PART="${PART:-6}"
MAX_PAGES="${MAX_PAGES:-60}"
SMOKE_IDEMPOTENCE="${SMOKE_IDEMPOTENCE:-0}"
SMOKE_DETERMINISM="${SMOKE_DETERMINISM:-0}"

log() {
  printf '[smoke] %s\n' "$*"
}

fail() {
  printf '[smoke][FAIL] %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    fail "required command not found: $1"
  fi
}

assert_jq_file() {
  local file_path="$1"
  local jq_expr="$2"
  local description="$3"

  if jq -e "$jq_expr" "$file_path" >/dev/null; then
    log "PASS: ${description}"
  else
    fail "${description}"
  fi
}

assert_jq_json() {
  local json_payload="$1"
  local jq_expr="$2"
  local description="$3"

  if printf '%s' "$json_payload" | jq -e "$jq_expr" >/dev/null; then
    log "PASS: ${description}"
  else
    fail "${description}"
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

run_query_json() {
  local binary_path="$1"
  local query_text="$2"
  shift 2

  "$binary_path" query \
    --cache-root "$CACHE_ROOT" \
    --query "$query_text" \
    --part "$PART" \
    --json \
    --limit 1 \
    "$@"
}

normalize_quality_report() {
  local report_path="$1"

  jq -c '{
    status,
    summary,
    hierarchy_metrics,
    table_quality_scorecard: {
      counters: .table_quality_scorecard.counters,
      table_sparse_row_ratio: .table_quality_scorecard.table_sparse_row_ratio,
      table_overloaded_row_ratio: .table_quality_scorecard.table_overloaded_row_ratio,
      table_marker_sequence_coverage: .table_quality_scorecard.table_marker_sequence_coverage,
      table_description_coverage: .table_quality_scorecard.table_description_coverage
    },
    checks
  }' "$report_path"
}

require_cmd cargo
require_cmd jq

log "Running build and ingest gate commands"
cargo check
cargo build --quiet

BIN_PATH="target/debug/iso26262"
MANIFEST_DIR="${CACHE_ROOT}/manifests"
REPORT_PATH="${MANIFEST_DIR}/extraction_quality_report.json"

"$BIN_PATH" ingest --cache-root "$CACHE_ROOT" --target-part "$PART" --max-pages-per-doc "$MAX_PAGES"
"$BIN_PATH" validate --cache-root "$CACHE_ROOT"

if [[ ! -f "$REPORT_PATH" ]]; then
  fail "quality report not found at ${REPORT_PATH}"
fi

LATEST_INGEST_PATH="$(latest_ingest_manifest "$MANIFEST_DIR")"
log "Using ingest manifest: ${LATEST_INGEST_PATH}"

log "Validating quality report"
assert_jq_file "$REPORT_PATH" '.status == "passed"' 'quality report status is passed'
assert_jq_file "$REPORT_PATH" '.summary.failed == 0 and .summary.pending == 0' 'quality summary has no failed or pending checks'
assert_jq_file "$REPORT_PATH" '(.checks[] | select(.check_id == "Q-010").result) == "pass"' 'Q-010 hierarchy expectations pass'
assert_jq_file "$REPORT_PATH" '(.checks[] | select(.check_id == "Q-011").result) == "pass"' 'Q-011 table sparse-row ratio passes'
assert_jq_file "$REPORT_PATH" '(.checks[] | select(.check_id == "Q-012").result) == "pass"' 'Q-012 table overloaded-row ratio passes'
assert_jq_file "$REPORT_PATH" '(.checks[] | select(.check_id == "Q-013").result) == "pass"' 'Q-013 table marker-sequence coverage passes'
assert_jq_file "$REPORT_PATH" '(.checks[] | select(.check_id == "Q-014").result) == "pass"' 'Q-014 table description coverage passes'
assert_jq_file "$REPORT_PATH" '(.checks[] | select(.check_id == "Q-015").result) == "pass"' 'Q-015 marker extraction coverage passes'
assert_jq_file "$REPORT_PATH" '(.checks[] | select(.check_id == "Q-016").result) == "pass"' 'Q-016 marker citation accuracy passes'
assert_jq_file "$REPORT_PATH" '(.checks[] | select(.check_id == "Q-017").result) == "pass"' 'Q-017 paragraph citation accuracy passes'
assert_jq_file "$REPORT_PATH" '(.checks[] | select(.check_id == "Q-018").result) == "pass"' 'Q-018 structural invariants pass'
assert_jq_file "$REPORT_PATH" '(.checks[] | select(.check_id == "Q-019").result) == "pass"' 'Q-019 ASIL table alignment checks pass'
assert_jq_file "$REPORT_PATH" '.table_quality_scorecard.table_sparse_row_ratio <= 0.20' 'table sparse-row ratio is within threshold'
assert_jq_file "$REPORT_PATH" '.table_quality_scorecard.table_overloaded_row_ratio <= 0.10' 'table overloaded-row ratio is within threshold'
assert_jq_file "$REPORT_PATH" '.table_quality_scorecard.table_marker_sequence_coverage >= 0.90' 'table marker-sequence coverage meets threshold'
assert_jq_file "$REPORT_PATH" '.table_quality_scorecard.table_description_coverage >= 0.90' 'table description coverage meets threshold'

log "Validating representative unit-type samples"
SECTION_JSON="$(run_query_json "$BIN_PATH" "Software unit verification" --node-type section_heading)"
assert_jq_json "$SECTION_JSON" '.returned >= 1 and .results[0].leaf_node_type == "section_heading" and .results[0].reference == "9"' 'section_heading sample query succeeds'

CLAUSE_JSON="$(run_query_json "$BIN_PATH" "9.1")"
assert_jq_json "$CLAUSE_JSON" '.returned >= 1 and .results[0].leaf_node_type == "clause" and .results[0].anchor_type == "clause" and (.results[0].citation | contains("9.1"))' 'clause sample query succeeds'

PARAGRAPH_JSON="$(run_query_json "$BIN_PATH" "9.1 para 3" --node-type paragraph)"
assert_jq_json "$PARAGRAPH_JSON" '.returned >= 1 and .results[0].leaf_node_type == "paragraph" and .results[0].anchor_type == "paragraph" and .results[0].anchor_label_norm == "3" and (.results[0].citation | contains("para 3"))' 'paragraph sample query succeeds'

LIST_ITEM_JSON="$(run_query_json "$BIN_PATH" "9.1 item 2" --node-type list_item)"
assert_jq_json "$LIST_ITEM_JSON" '.returned >= 1 and .results[0].leaf_node_type == "list_item" and .results[0].anchor_type == "marker" and .results[0].anchor_label_norm == "b" and (.results[0].citation | contains("9.1(b)"))' 'list item marker-first citation sample succeeds'

NOTE_ITEM_JSON="$(run_query_json "$BIN_PATH" "5.2 note 2" --node-type note_item)"
assert_jq_json "$NOTE_ITEM_JSON" '.returned >= 1 and .results[0].leaf_node_type == "note_item" and .results[0].parent_ref == "5.2" and .results[0].anchor_type == "marker" and .results[0].anchor_label_norm == "NOTE 1" and (.results[0].citation | contains("NOTE 1"))' 'NOTE marker sample query succeeds'

NOTE_AS_LIST_JSON="$(run_query_json "$BIN_PATH" "5.2 note 2" --node-type list_item)"
assert_jq_json "$NOTE_AS_LIST_JSON" '.returned == 0' 'NOTE query is excluded from list_item results'

TABLE_JSON="$(run_query_json "$BIN_PATH" "Table 3")"
assert_jq_json "$TABLE_JSON" '.returned >= 1 and .results[0].leaf_node_type == "table" and .results[0].anchor_type == "clause"' 'table sample query succeeds'

TABLE_ROW_JSON="$(run_query_json "$BIN_PATH" "Table 3 row 4" --node-type table_row)"
assert_jq_json "$TABLE_ROW_JSON" '.returned >= 1 and .results[0].leaf_node_type == "table_row" and .results[0].anchor_type == "table_row" and (.results[0].snippet | contains("Restricted size of interfaces"))' 'table_row sample query succeeds'

TABLE_CELL_JSON="$(run_query_json "$BIN_PATH" "Table 3 r4c2" --node-type table_cell)"
assert_jq_json "$TABLE_CELL_JSON" '.returned >= 1 and .results[0].leaf_node_type == "table_cell" and .results[0].anchor_type == "table_cell" and .results[0].anchor_label_norm == "r4c2"' 'table_cell sample query succeeds'

REQ_ATOM_JSON="$(run_query_json "$BIN_PATH" "software unit design" --node-type requirement_atom)"
assert_jq_json "$REQ_ATOM_JSON" '.returned >= 1 and .results[0].leaf_node_type == "requirement_atom" and (.results[0].reference | contains("req"))' 'requirement_atom sample query succeeds'

if [[ "$SMOKE_DETERMINISM" == "1" ]]; then
  log "Running optional validate/query determinism check"

  baseline_report="$(normalize_quality_report "$REPORT_PATH")"
  baseline_marker_query="$(run_query_json "$BIN_PATH" "8.4.5 item 1" --node-type list_item | jq -c '.results[0] | {
    reference,
    citation,
    anchor_type,
    anchor_label_norm,
    page_pdf_start,
    page_pdf_end,
    source_hash,
    citation_anchor_id
  }')"
  baseline_paragraph_query="$(run_query_json "$BIN_PATH" "9.1 para 3" --node-type paragraph | jq -c '.results[0] | {
    reference,
    citation,
    anchor_type,
    anchor_label_norm,
    page_pdf_start,
    page_pdf_end,
    source_hash,
    citation_anchor_id
  }')"

  "$BIN_PATH" validate --cache-root "$CACHE_ROOT"

  current_report="$(normalize_quality_report "$REPORT_PATH")"
  current_marker_query="$(run_query_json "$BIN_PATH" "8.4.5 item 1" --node-type list_item | jq -c '.results[0] | {
    reference,
    citation,
    anchor_type,
    anchor_label_norm,
    page_pdf_start,
    page_pdf_end,
    source_hash,
    citation_anchor_id
  }')"
  current_paragraph_query="$(run_query_json "$BIN_PATH" "9.1 para 3" --node-type paragraph | jq -c '.results[0] | {
    reference,
    citation,
    anchor_type,
    anchor_label_norm,
    page_pdf_start,
    page_pdf_end,
    source_hash,
    citation_anchor_id
  }')"

  if [[ "$baseline_report" != "$current_report" ]]; then
    fail "quality report core fields changed between consecutive validate runs"
  fi
  if [[ "$baseline_marker_query" != "$current_marker_query" ]]; then
    fail "marker query output changed between consecutive validate runs"
  fi
  if [[ "$baseline_paragraph_query" != "$current_paragraph_query" ]]; then
    fail "paragraph query output changed between consecutive validate runs"
  fi

  log "PASS: validate/query outputs are deterministic across consecutive validate runs"
fi

if [[ "$SMOKE_IDEMPOTENCE" == "1" ]]; then
  log "Running optional idempotence check"
  baseline_counts="$(jq -c '.counts | {
    table_row_nodes_inserted,
    table_cell_nodes_inserted,
    list_item_nodes_inserted,
    note_nodes_inserted,
    note_item_nodes_inserted,
    paragraph_nodes_inserted,
    table_sparse_rows_count,
    table_overloaded_rows_count,
    table_rows_with_descriptions_count,
    table_marker_observed_count
  }' "$LATEST_INGEST_PATH")"

  "$BIN_PATH" ingest --cache-root "$CACHE_ROOT" --target-part "$PART" --max-pages-per-doc "$MAX_PAGES"
  second_ingest_path="$(latest_ingest_manifest "$MANIFEST_DIR")"
  current_counts="$(jq -c '.counts | {
    table_row_nodes_inserted,
    table_cell_nodes_inserted,
    list_item_nodes_inserted,
    note_nodes_inserted,
    note_item_nodes_inserted,
    paragraph_nodes_inserted,
    table_sparse_rows_count,
    table_overloaded_rows_count,
    table_rows_with_descriptions_count,
    table_marker_observed_count
  }' "$second_ingest_path")"

  if [[ "$baseline_counts" != "$current_counts" ]]; then
    fail "idempotence counts changed between ingest runs"
  fi

  log "PASS: idempotence counts are stable"
fi

log "Smoke checks completed successfully"
