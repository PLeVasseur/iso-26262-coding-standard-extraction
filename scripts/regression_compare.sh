#!/usr/bin/env bash
set -euo pipefail

RUN_ID=""
MODE="full"
OUTPUT_ROOT=".cache/iso26262/regression"
THRESHOLDS_PATH="scripts/lib/regression/thresholds.json"
EXPECT_STATUS=""

usage() {
  cat <<'EOF'
Usage: scripts/regression_compare.sh --run-id <id> [options]

Required:
  --run-id <id>

Optional:
  --mode <lite|full>                Compare mode (default: full)
  --output-root <path>              Regression output root (default: .cache/iso26262/regression)
  --thresholds <path>               Threshold config (default: scripts/lib/regression/thresholds.json)
  --expect-status <PASS|WARN|FAIL>  Assert expected status (useful for one-time calibration)
  -h, --help                        Show this help message
EOF
}

log() {
  printf '[reg-compare] %s\n' "$*"
}

fail() {
  printf '[reg-compare][FAIL] %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    fail "required command not found: $1"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-id)
      RUN_ID="$2"
      shift 2
      ;;
    --mode)
      MODE="$2"
      shift 2
      ;;
    --output-root)
      OUTPUT_ROOT="$2"
      shift 2
      ;;
    --thresholds)
      THRESHOLDS_PATH="$2"
      shift 2
      ;;
    --expect-status)
      EXPECT_STATUS="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      fail "unknown argument: $1"
      ;;
  esac
done

if [[ -z "$RUN_ID" ]]; then
  fail "--run-id is required"
fi

case "$MODE" in
  lite|full) ;;
  *) fail "--mode must be one of: lite, full" ;;
esac

if [[ -n "$EXPECT_STATUS" ]]; then
  case "$EXPECT_STATUS" in
    PASS|WARN|FAIL) ;;
    *) fail "--expect-status must be PASS, WARN, or FAIL" ;;
  esac
fi

for command_name in jq sha256sum mktemp mkdir date; do
  require_cmd "$command_name"
done

if [[ ! -f "$THRESHOLDS_PATH" ]]; then
  fail "threshold file not found: $THRESHOLDS_PATH"
fi

COMPARE_FILTER_PATH="scripts/lib/regression/compare.jq"
if [[ ! -f "$COMPARE_FILTER_PATH" ]]; then
  fail "compare jq filter not found: $COMPARE_FILTER_PATH"
fi

RUN_DIR="$OUTPUT_ROOT/$RUN_ID"
BEFORE_DIR="$RUN_DIR/before"
AFTER_DIR="$RUN_DIR/after"
COMPARE_DIR="$RUN_DIR/compare"

if [[ ! -d "$BEFORE_DIR" ]]; then
  fail "before snapshot directory not found: $BEFORE_DIR"
fi

if [[ ! -d "$AFTER_DIR" ]]; then
  fail "after snapshot directory not found: $AFTER_DIR"
fi

mkdir -p "$COMPARE_DIR"

QUICK_BEFORE="$BEFORE_DIR/quality_report_quick_stageb.json"
QUICK_AFTER="$AFTER_DIR/quality_report_quick_stageb.json"
FULL_BEFORE="$BEFORE_DIR/quality_report_full_stageb.json"
FULL_AFTER="$AFTER_DIR/quality_report_full_stageb.json"
BENCH_BEFORE="$BEFORE_DIR/benchmark_quick.json"
BENCH_AFTER="$AFTER_DIR/benchmark_quick.json"
LEXICAL_BEFORE_JSONL="$BEFORE_DIR/search_snapshot_lexical.jsonl"
LEXICAL_AFTER_JSONL="$AFTER_DIR/search_snapshot_lexical.jsonl"
SEMANTIC_BEFORE_JSONL="$BEFORE_DIR/search_snapshot_semantic.jsonl"
SEMANTIC_AFTER_JSONL="$AFTER_DIR/search_snapshot_semantic.jsonl"

for path in "$QUICK_BEFORE" "$QUICK_AFTER" "$BENCH_BEFORE" "$BENCH_AFTER" "$LEXICAL_BEFORE_JSONL" "$LEXICAL_AFTER_JSONL" "$SEMANTIC_BEFORE_JSONL" "$SEMANTIC_AFTER_JSONL"; do
  if [[ ! -f "$path" ]]; then
    fail "required artifact missing: $path"
  fi
done

if [[ "$MODE" == "full" ]]; then
  for path in "$FULL_BEFORE" "$FULL_AFTER"; do
    if [[ ! -f "$path" ]]; then
      fail "full-mode artifact missing: $path"
    fi
  done
fi

LEXICAL_BEFORE_JSON="$(mktemp)"
LEXICAL_AFTER_JSON="$(mktemp)"
SEMANTIC_BEFORE_JSON="$(mktemp)"
SEMANTIC_AFTER_JSON="$(mktemp)"
FULL_BEFORE_JSON="$(mktemp)"
FULL_AFTER_JSON="$(mktemp)"
trap 'rm -f "$LEXICAL_BEFORE_JSON" "$LEXICAL_AFTER_JSON" "$SEMANTIC_BEFORE_JSON" "$SEMANTIC_AFTER_JSON" "$FULL_BEFORE_JSON" "$FULL_AFTER_JSON"' EXIT

jq -s '.' "$LEXICAL_BEFORE_JSONL" >"$LEXICAL_BEFORE_JSON"
jq -s '.' "$LEXICAL_AFTER_JSONL" >"$LEXICAL_AFTER_JSON"
jq -s '.' "$SEMANTIC_BEFORE_JSONL" >"$SEMANTIC_BEFORE_JSON"
jq -s '.' "$SEMANTIC_AFTER_JSONL" >"$SEMANTIC_AFTER_JSON"

if [[ "$MODE" == "full" ]]; then
  cp "$FULL_BEFORE" "$FULL_BEFORE_JSON"
  cp "$FULL_AFTER" "$FULL_AFTER_JSON"
else
  printf 'null\n' >"$FULL_BEFORE_JSON"
  printf 'null\n' >"$FULL_AFTER_JSON"
fi

THRESHOLD_HASH="$(sha256sum "$THRESHOLDS_PATH" | awk '{print $1}')"
DRIFT_REPORT_JSON_PATH="$COMPARE_DIR/drift_report.json"
DRIFT_REPORT_MD_PATH="$COMPARE_DIR/drift_report.md"
GATE_STATUS_PATH="$COMPARE_DIR/gate_status.txt"

jq -n \
  --arg run_id "$RUN_ID" \
  --arg mode "$MODE" \
  --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg threshold_hash "$THRESHOLD_HASH" \
  --slurpfile thresholds "$THRESHOLDS_PATH" \
  --slurpfile quick_before "$QUICK_BEFORE" \
  --slurpfile quick_after "$QUICK_AFTER" \
  --slurpfile full_before "$FULL_BEFORE_JSON" \
  --slurpfile full_after "$FULL_AFTER_JSON" \
  --slurpfile benchmark_before "$BENCH_BEFORE" \
  --slurpfile benchmark_after "$BENCH_AFTER" \
  --slurpfile lexical_before "$LEXICAL_BEFORE_JSON" \
  --slurpfile lexical_after "$LEXICAL_AFTER_JSON" \
  --slurpfile semantic_before "$SEMANTIC_BEFORE_JSON" \
  --slurpfile semantic_after "$SEMANTIC_AFTER_JSON" \
  -f "$COMPARE_FILTER_PATH" >"$DRIFT_REPORT_JSON_PATH"

GATE_STATUS="$(jq -r '.gate_status' "$DRIFT_REPORT_JSON_PATH")"
printf '%s\n' "$GATE_STATUS" >"$GATE_STATUS_PATH"

jq -r '
  def pct($value):
    if $value == null then "n/a"
    else ((($value * 10000) | round) / 100 | tostring) + "%"
    end;

  def n($value):
    if $value == null then "n/a"
    elif ($value | type) == "number" then (((($value * 1000000) | round) / 1000000) | tostring)
    else ($value | tostring)
    end;

  [
    "# Regression Drift Report",
    "",
    "- Run ID: `\(.run_id)`",
    "- Mode: `\(.mode)`",
    "- Gate status: `\(.gate_status)`",
    "- Threshold schema version: `\(.threshold_schema_version)`",
    "- Threshold hash: `\(.threshold_file_hash)`",
    "",
    "## Quick Stage B",
    "- Before: status=`\(.quick_stage_b.before.status)` checks=`\(.quick_stage_b.before.summary.passed // "n/a")/\(.quick_stage_b.before.summary.total_checks // "n/a")`",
    "- After: status=`\(.quick_stage_b.after.status)` checks=`\(.quick_stage_b.after.summary.passed // "n/a")/\(.quick_stage_b.after.summary.total_checks // "n/a")`",
    "- New failed checks: `\((.quick_stage_b.new_failed_checks | join(", ")) // "")`",
    "",
    "## Snapshot Drift",
    "- Lexical top1 expected hit rate: `\(pct(.search_snapshots.lexical.before.top1_expected_hit_rate)) -> \(pct(.search_snapshots.lexical.after.top1_expected_hit_rate))`",
    "- Lexical avg Jaccard@10: `\(n(.search_snapshots.lexical.overlap.avg_jaccard_at_10))`",
    "- Lexical no-result rate: `\(pct(.search_snapshots.lexical.before.no_result_rate)) -> \(pct(.search_snapshots.lexical.after.no_result_rate))`",
    "- Semantic top1 expected hit rate: `\(pct(.search_snapshots.semantic.before.top1_expected_hit_rate)) -> \(pct(.search_snapshots.semantic.after.top1_expected_hit_rate))`",
    "- Semantic avg Jaccard@10: `\(n(.search_snapshots.semantic.overlap.avg_jaccard_at_10))`",
    "- Semantic no-result rate: `\(pct(.search_snapshots.semantic.before.no_result_rate)) -> \(pct(.search_snapshots.semantic.after.no_result_rate))`",
    "",
    "## Benchmark Drift",
    "- Lexical p95 delta (ms): `\(n(.benchmark_quick.mode_deltas.lexical.latency_ms_delta.p95))`",
    "- Semantic p95 delta (ms): `\(n(.benchmark_quick.mode_deltas.semantic.latency_ms_delta.p95))`",
    "- Hybrid p95 delta (ms): `\(n(.benchmark_quick.mode_deltas.hybrid.latency_ms_delta.p95))`",
    ""
  ]
  + (if .mode == "full" then [
      "## Full Stage B",
      "- Before: status=`\(.full_stage_b.before.status)` checks=`\(.full_stage_b.before.summary.passed // "n/a")/\(.full_stage_b.before.summary.total_checks // "n/a")`",
      "- After: status=`\(.full_stage_b.after.status)` checks=`\(.full_stage_b.after.summary.passed // "n/a")/\(.full_stage_b.after.summary.total_checks // "n/a")`",
      "- New failed checks: `\((.full_stage_b.new_failed_checks | join(", ")) // "")`",
      ""
    ] else [] end)
  + [
      "## Hard Fail Rules Triggered",
      (if (.rule_results.hard_failures | length) == 0 then "- none"
       else (.rule_results.hard_failures[] | "- `\(.id)`: \(.message) (observed=\(n(.observed)), threshold=\(n(.threshold)))")
       end),
      "",
      "## Soft Fail Rules Triggered",
      (if (.rule_results.soft_failures | length) == 0 then "- none"
       else (.rule_results.soft_failures[] | "- `\(.id)`: \(.message) (observed=\(n(.observed)), threshold=\(n(.threshold)))")
       end),
      ""
    ]
  | .[]
' "$DRIFT_REPORT_JSON_PATH" >"$DRIFT_REPORT_MD_PATH"

log "Wrote drift report: $DRIFT_REPORT_JSON_PATH"
log "Wrote markdown summary: $DRIFT_REPORT_MD_PATH"
log "Gate status: $GATE_STATUS"

if [[ -n "$EXPECT_STATUS" && "$EXPECT_STATUS" != "$GATE_STATUS" ]]; then
  fail "expected status ${EXPECT_STATUS}, observed ${GATE_STATUS}"
fi

case "$GATE_STATUS" in
  PASS)
    exit 0
    ;;
  WARN)
    exit 10
    ;;
  FAIL)
    exit 20
    ;;
  *)
    fail "unexpected gate status: $GATE_STATUS"
    ;;
esac
