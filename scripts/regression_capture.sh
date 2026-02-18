#!/usr/bin/env bash
set -euo pipefail

MODE="full"
RUN_ID="reg-$(date -u +%Y%m%dT%H%M%SZ)"
PHASE=""
SOURCE_CACHE_ROOT=".cache/iso26262"
OUTPUT_ROOT=".cache/iso26262/regression"
SEMANTIC_MODEL_ID="miniLM-L6-v2-local-v1"
PART="6"
MAX_PAGES="60"
TARGET_PARTS="2 6 8 9"
LEXICAL_K="96"
SEMANTIC_K="96"
RRF_K="60"
TIMEOUT_MS="2000"
SNAPSHOT_LIMIT="10"
BENCH_PROFILE="quick"
BENCH_REPEATS_LITE="1"
BENCH_REPEATS_FULL="2"
FORCE="0"

usage() {
  cat <<'EOF'
Usage: scripts/regression_capture.sh [options]

Required:
  --phase <before|after>

Optional:
  --run-id <id>                     Run identifier (default: reg-<utc timestamp>)
  --mode <lite|full>                Capture mode (default: full)
  --source-cache-root <path>        Source cache root to seed snapshots (default: .cache/iso26262)
  --output-root <path>              Regression output root (default: .cache/iso26262/regression)
  --semantic-model-id <id>          Semantic model id (default: miniLM-L6-v2-local-v1)
  --part <n>                        Part used for quick-mode refresh/smoke checks (default: 6)
  --max-pages <n>                   Max pages for quick-mode ingest (default: 60)
  --target-parts "<parts>"          Full-mode target parts (default: "2 6 8 9")
  --lexical-k <n>                   Query lexical candidate limit (default: 96)
  --semantic-k <n>                  Query semantic candidate limit (default: 96)
  --rrf-k <n>                       Query RRF k (default: 60)
  --timeout-ms <n>                  Query timeout in ms (default: 2000)
  --snapshot-limit <n>              Per-query result limit for snapshots (default: 10)
  --bench-profile <quick|standard|full>
                                     Benchmark profile (default: quick)
  --bench-repeats-lite <n>          Benchmark repeats in lite mode (default: 1)
  --bench-repeats-full <n>          Benchmark repeats in full mode (default: 2)
  --force                           Overwrite existing phase folder for this run
  -h, --help                        Show this help message
EOF
}

log() {
  printf '[reg-capture] %s\n' "$*"
}

fail() {
  printf '[reg-capture][FAIL] %s\n' "$*" >&2
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
    --phase)
      PHASE="$2"
      shift 2
      ;;
    --mode)
      MODE="$2"
      shift 2
      ;;
    --source-cache-root)
      SOURCE_CACHE_ROOT="$2"
      shift 2
      ;;
    --output-root)
      OUTPUT_ROOT="$2"
      shift 2
      ;;
    --semantic-model-id)
      SEMANTIC_MODEL_ID="$2"
      shift 2
      ;;
    --part)
      PART="$2"
      shift 2
      ;;
    --max-pages)
      MAX_PAGES="$2"
      shift 2
      ;;
    --target-parts)
      TARGET_PARTS="$2"
      shift 2
      ;;
    --lexical-k)
      LEXICAL_K="$2"
      shift 2
      ;;
    --semantic-k)
      SEMANTIC_K="$2"
      shift 2
      ;;
    --rrf-k)
      RRF_K="$2"
      shift 2
      ;;
    --timeout-ms)
      TIMEOUT_MS="$2"
      shift 2
      ;;
    --snapshot-limit)
      SNAPSHOT_LIMIT="$2"
      shift 2
      ;;
    --bench-profile)
      BENCH_PROFILE="$2"
      shift 2
      ;;
    --bench-repeats-lite)
      BENCH_REPEATS_LITE="$2"
      shift 2
      ;;
    --bench-repeats-full)
      BENCH_REPEATS_FULL="$2"
      shift 2
      ;;
    --force)
      FORCE="1"
      shift 1
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

case "$PHASE" in
  before|after) ;;
  *) fail "--phase must be one of: before, after" ;;
esac

case "$MODE" in
  lite|full) ;;
  *) fail "--mode must be one of: lite, full" ;;
esac

for value in "$PART" "$MAX_PAGES" "$LEXICAL_K" "$SEMANTIC_K" "$RRF_K" "$TIMEOUT_MS" "$SNAPSHOT_LIMIT" "$BENCH_REPEATS_LITE" "$BENCH_REPEATS_FULL"; do
  if ! [[ "$value" =~ ^[0-9]+$ ]]; then
    fail "numeric option has invalid value: $value"
  fi
done

for command_name in cargo jq git cp rm mkdir mktemp sha256sum date awk sort sed; do
  require_cmd "$command_name"
done

if [[ ! -d "$SOURCE_CACHE_ROOT" ]]; then
  fail "source cache root does not exist: $SOURCE_CACHE_ROOT"
fi

if [[ -z "${OPENCODE_CONFIG_DIR:-}" ]]; then
  fail "OPENCODE_CONFIG_DIR is required for refresh runbook"
fi

RUN_DIR="$OUTPUT_ROOT/$RUN_ID"
PHASE_DIR="$RUN_DIR/$PHASE"
PHASE_CACHE_ROOT="$PHASE_DIR/cache"
PHASE_MANIFEST_DIR="$PHASE_CACHE_ROOT/manifests"
CAPTURE_MANIFEST_PATH="$PHASE_DIR/capture_manifest.json"
LEXICAL_SNAPSHOT_PATH="$PHASE_DIR/search_snapshot_lexical.jsonl"
SEMANTIC_SNAPSHOT_PATH="$PHASE_DIR/search_snapshot_semantic.jsonl"
QUICK_STAGE_B_REPORT_PATH="$PHASE_DIR/quality_report_quick_stageb.json"
FULL_STAGE_B_REPORT_PATH="$PHASE_DIR/quality_report_full_stageb.json"
BENCHMARK_CANONICAL_PATH="$PHASE_DIR/benchmark_quick.json"
LOCKFILE_PATH="$PHASE_DIR/semantic_model_config.lock.json"

mkdir -p "$RUN_DIR"
if [[ -d "$PHASE_DIR" ]]; then
  if [[ "$FORCE" != "1" ]]; then
    fail "phase directory already exists: $PHASE_DIR (use --force to overwrite)"
  fi
  rm -rf "$PHASE_DIR"
fi
mkdir -p "$PHASE_DIR"

ACTIVE_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
HEAD_COMMIT="$(git rev-parse HEAD)"
HEAD_SHORT="$(git rev-parse --short HEAD)"
GIT_DIRTY="false"
if [[ -n "$(git status --short)" ]]; then
  GIT_DIRTY="true"
fi

seed_phase_cache() {
  log "Seeding phase cache from ${SOURCE_CACHE_ROOT}"
  mkdir -p "$PHASE_CACHE_ROOT"

  if compgen -G "$SOURCE_CACHE_ROOT/*.pdf" >/dev/null; then
    cp -a "$SOURCE_CACHE_ROOT"/*.pdf "$PHASE_CACHE_ROOT/"
  fi

  if [[ -d "$SOURCE_CACHE_ROOT/pdfs" ]]; then
    cp -a "$SOURCE_CACHE_ROOT/pdfs" "$PHASE_CACHE_ROOT/pdfs"
  fi

  if [[ -f "$SOURCE_CACHE_ROOT/iso26262_index.sqlite" ]]; then
    cp -a "$SOURCE_CACHE_ROOT/iso26262_index.sqlite" "$PHASE_CACHE_ROOT/iso26262_index.sqlite"
  fi

  if [[ -f "$SOURCE_CACHE_ROOT/iso26262_index.sqlite-shm" ]]; then
    cp -a "$SOURCE_CACHE_ROOT/iso26262_index.sqlite-shm" "$PHASE_CACHE_ROOT/iso26262_index.sqlite-shm"
  fi

  if [[ -f "$SOURCE_CACHE_ROOT/iso26262_index.sqlite-wal" ]]; then
    cp -a "$SOURCE_CACHE_ROOT/iso26262_index.sqlite-wal" "$PHASE_CACHE_ROOT/iso26262_index.sqlite-wal"
  fi

  if [[ -d "$SOURCE_CACHE_ROOT/manifests" ]]; then
    cp -a "$SOURCE_CACHE_ROOT/manifests" "$PHASE_MANIFEST_DIR"
  else
    mkdir -p "$PHASE_MANIFEST_DIR"
  fi

  if [[ ! -d "$PHASE_MANIFEST_DIR" ]]; then
    fail "failed to seed manifests directory under $PHASE_CACHE_ROOT"
  fi

  shopt -s nullglob
  local seeded_pdf_paths=("$PHASE_CACHE_ROOT"/*.pdf)
  shopt -u nullglob
  if [[ "${#seeded_pdf_paths[@]}" -eq 0 ]]; then
    fail "no root-level PDFs were seeded into phase cache from $SOURCE_CACHE_ROOT"
  fi

  rm -f "$PHASE_MANIFEST_DIR/run_state.json"
  rm -f "$PHASE_MANIFEST_DIR/decisions_log.jsonl"
  rm -f "$PHASE_MANIFEST_DIR/extraction_quality_report.json"
  rm -f "$PHASE_MANIFEST_DIR"/ingest_run_*.json
  rm -f "$PHASE_MANIFEST_DIR"/embedding_run_*.json
  rm -f "$PHASE_MANIFEST_DIR"/semantic_benchmark_*.json
  rm -f "$PHASE_MANIFEST_DIR"/semantic_benchmark_queries_*.tsv
}

copy_stage_b_report() {
  local destination="$1"
  local source_report="$PHASE_MANIFEST_DIR/extraction_quality_report.json"
  if [[ ! -f "$source_report" ]]; then
    fail "expected quality report missing: $source_report"
  fi
  cp "$source_report" "$destination"
}

run_refresh_quick_stage() {
  log "Running quick refresh (Stage A)"
  CACHE_ROOT="$PHASE_CACHE_ROOT" \
  PART="$PART" \
  MAX_PAGES="$MAX_PAGES" \
  WP2_GATE_STAGE="A" \
  BASE_BRANCH="$ACTIVE_BRANCH" \
  UPDATE_DECISIONS="0" \
  SEMANTIC_MODEL_ID="$SEMANTIC_MODEL_ID" \
  EMBED_LOCKFILE_PATH="$LOCKFILE_PATH" \
  scripts/refresh_quality_artifacts.sh

  log "Running quick Stage B validate"
  WP2_GATE_STAGE="B" cargo run -- validate --cache-root "$PHASE_CACHE_ROOT"
  copy_stage_b_report "$QUICK_STAGE_B_REPORT_PATH"
}

run_refresh_full_stage() {
  log "Running full-target refresh (Stage A)"
  CACHE_ROOT="$PHASE_CACHE_ROOT" \
  FULL_TARGET_SET="1" \
  TARGET_PARTS="$TARGET_PARTS" \
  WP2_GATE_STAGE="A" \
  BASE_BRANCH="$ACTIVE_BRANCH" \
  UPDATE_DECISIONS="0" \
  SEMANTIC_MODEL_ID="$SEMANTIC_MODEL_ID" \
  EMBED_LOCKFILE_PATH="$LOCKFILE_PATH" \
  scripts/refresh_quality_artifacts.sh

  log "Running full-target Stage B validate"
  WP2_GATE_STAGE="B" cargo run -- validate --cache-root "$PHASE_CACHE_ROOT"
  copy_stage_b_report "$FULL_STAGE_B_REPORT_PATH"
}

run_smoke_checks() {
  log "Running smoke checks"
  CACHE_ROOT="$PHASE_CACHE_ROOT" \
  PART="$PART" \
  MAX_PAGES="$MAX_PAGES" \
  SEMANTIC_MODEL_ID="$SEMANTIC_MODEL_ID" \
  EMBED_LOCKFILE_PATH="$LOCKFILE_PATH" \
  scripts/smoke_part6.sh

  log "Running deterministic smoke checks"
  CACHE_ROOT="$PHASE_CACHE_ROOT" \
  PART="$PART" \
  MAX_PAGES="$MAX_PAGES" \
  SEMANTIC_MODEL_ID="$SEMANTIC_MODEL_ID" \
  EMBED_LOCKFILE_PATH="$LOCKFILE_PATH" \
  SMOKE_DETERMINISM="1" \
  scripts/smoke_part6.sh
}

BENCH_OUTPUT_PATHS=()

run_benchmark_capture() {
  local repeats="$1"
  if [[ "$repeats" -le 0 ]]; then
    fail "benchmark repeat count must be > 0"
  fi

  log "Running benchmark captures (${repeats} run(s))"
  local run_index
  for run_index in $(seq 1 "$repeats"); do
    local output_path="$PHASE_DIR/benchmark_quick_run${run_index}.json"
    CACHE_ROOT="$PHASE_CACHE_ROOT" \
    BENCH_PROFILE="$BENCH_PROFILE" \
    SEMANTIC_MODEL_ID="$SEMANTIC_MODEL_ID" \
    OUTPUT_DIR="$PHASE_DIR" \
    OUTPUT_PATH="$output_path" \
    RUN_ID="${RUN_ID}-${PHASE}-bench-${run_index}" \
    LEXICAL_K="$LEXICAL_K" \
    SEMANTIC_K="$SEMANTIC_K" \
    RRF_K="$RRF_K" \
    TIMEOUT_MS="$TIMEOUT_MS" \
    scripts/benchmark_query_modes.sh
    BENCH_OUTPUT_PATHS+=("$output_path")
  done

  local selected_path="${BENCH_OUTPUT_PATHS[0]}"
  if [[ "$repeats" -gt 1 ]]; then
    local tmp_scores
    tmp_scores="$(mktemp)"
    local score_path
    for score_path in "${BENCH_OUTPUT_PATHS[@]}"; do
      local hybrid_p95
      hybrid_p95="$(jq -r '.mode_summaries[] | select(.mode == "hybrid") | .latency_ms.p95 // 0' "$score_path")"
      printf '%s\t%s\n' "$hybrid_p95" "$score_path" >>"$tmp_scores"
    done
    sort -n "$tmp_scores" >"${tmp_scores}.sorted"
    local median_index=$(( (repeats + 1) / 2 ))
    selected_path="$(awk -F'\t' -v target_line="$median_index" 'NR == target_line { print $2 }' "${tmp_scores}.sorted")"
    rm -f "$tmp_scores" "${tmp_scores}.sorted"
  fi

  cp "$selected_path" "$BENCHMARK_CANONICAL_PATH"
  log "Selected canonical benchmark report: $selected_path"
}

capture_query_snapshot() {
  local mode="$1"
  local output_path="$2"
  local query_manifest_path="$PHASE_MANIFEST_DIR/semantic_eval_queries.json"
  local query_counter=0

  if [[ ! -f "$query_manifest_path" ]]; then
    fail "semantic eval query manifest missing: $query_manifest_path"
  fi

  : >"$output_path"

  while IFS= read -r query_json; do
    query_counter=$((query_counter + 1))
    local query_text query_id part_filter chunk_type_filter
    query_text="$(jq -r '.query_text' <<<"$query_json")"
    query_id="$(jq -r '.query_id' <<<"$query_json")"
    part_filter="$(jq -r '.part_filter // empty' <<<"$query_json")"
    chunk_type_filter="$(jq -r '.chunk_type_filter // empty' <<<"$query_json")"

    local args
    args=(
      "$BIN_PATH" query
      --cache-root "$PHASE_CACHE_ROOT"
      --query "$query_text"
      --retrieval-mode "$mode"
      --lexical-k "$LEXICAL_K"
      --semantic-k "$SEMANTIC_K"
      --rrf-k "$RRF_K"
      --timeout-ms "$TIMEOUT_MS"
      --json
      --limit "$SNAPSHOT_LIMIT"
    )

    if [[ -n "$part_filter" ]]; then
      args+=(--part "$part_filter")
    fi

    if [[ -n "$chunk_type_filter" ]]; then
      args+=(--type "$chunk_type_filter")
    fi

    if [[ "$mode" != "lexical" ]]; then
      args+=(--semantic-model-id "$SEMANTIC_MODEL_ID")
    fi

    local stderr_path response
    stderr_path="$(mktemp)"
    if response="$("${args[@]}" 2>"$stderr_path")"; then
      jq -cn \
        --arg mode "$mode" \
        --argjson query "$query_json" \
        --argjson response "$response" \
        --argjson timeout_ms "$TIMEOUT_MS" \
        '{
          query_id: $query.query_id,
          query_text: $query.query_text,
          intent: ($query.intent // null),
          mode: $mode,
          part_filter: ($query.part_filter // null),
          chunk_type_filter: ($query.chunk_type_filter // null),
          must_hit_top1: ($query.must_hit_top1 // false),
          expected_chunk_ids: ($query.expected_chunk_ids // []),
          status: "ok",
          returned: ($response.returned // 0),
          fallback_used: ($response.retrieval.fallback_used // false),
          query_duration_ms: ($response.retrieval.query_duration_ms // null),
          timeout_ms: $timeout_ms,
          timed_out: (
            if ($response.retrieval.query_duration_ms // null) == null or $timeout_ms <= 0 then false
            else (($response.retrieval.query_duration_ms // 0) >= $timeout_ms)
            end
          ),
          candidate_counts: {
            lexical: ($response.retrieval.lexical_candidate_count // null),
            semantic: ($response.retrieval.semantic_candidate_count // null),
            fused: ($response.retrieval.fused_candidate_count // null)
          },
          top_chunk_ids: (($response.results // [])[:10] | map(.chunk_id)),
          top_refs: (($response.results // [])[:10] | map(.reference)),
          top1_chunk_id: ($response.results[0].chunk_id // null),
          top1_ref: ($response.results[0].reference // null),
          top1_citation: ($response.results[0].citation // null),
          error: null
        }' >>"$output_path"
    else
      local exit_code error_message
      exit_code="$?"
      error_message="$(tr '\n' ' ' <"$stderr_path" | sed -E 's/[[:space:]]+/ /g; s/^ //; s/ $//')"
      jq -cn \
        --arg mode "$mode" \
        --argjson query "$query_json" \
        --arg error_message "$error_message" \
        --argjson exit_code "$exit_code" \
        '{
          query_id: $query.query_id,
          query_text: $query.query_text,
          intent: ($query.intent // null),
          mode: $mode,
          part_filter: ($query.part_filter // null),
          chunk_type_filter: ($query.chunk_type_filter // null),
          must_hit_top1: ($query.must_hit_top1 // false),
          expected_chunk_ids: ($query.expected_chunk_ids // []),
          status: "error",
          returned: 0,
          fallback_used: false,
          query_duration_ms: null,
          timeout_ms: null,
          timed_out: false,
          candidate_counts: {
            lexical: null,
            semantic: null,
            fused: null
          },
          top_chunk_ids: [],
          top_refs: [],
          top1_chunk_id: null,
          top1_ref: null,
          top1_citation: null,
          error: {
            exit_code: $exit_code,
            message: $error_message
          }
        }' >>"$output_path"
    fi
    rm -f "$stderr_path"

    if [[ "$query_counter" -eq 1 || $((query_counter % 10)) -eq 0 ]]; then
      log "Snapshot progress mode=${mode} queries=${query_counter} last_query_id=${query_id}"
    fi
  done < <(jq -c '.queries | sort_by(.query_id)[]' "$query_manifest_path")

  log "Snapshot complete mode=${mode} queries=${query_counter}"
}

capture_snapshots() {
  log "Capturing lexical and semantic query snapshots"
  capture_query_snapshot "lexical" "$LEXICAL_SNAPSHOT_PATH"
  capture_query_snapshot "semantic" "$SEMANTIC_SNAPSHOT_PATH"
}

write_capture_manifest() {
  local benchmark_runs_json full_report_json benchmark_selected_json
  benchmark_runs_json="$(printf '%s\n' "${BENCH_OUTPUT_PATHS[@]}" | jq -R . | jq -s .)"
  benchmark_selected_json="$(jq -Rn --arg value "$BENCHMARK_CANONICAL_PATH" '$value')"
  if [[ "$MODE" == "full" ]]; then
    full_report_json="$(jq -Rn --arg value "$FULL_STAGE_B_REPORT_PATH" '$value')"
  else
    full_report_json="null"
  fi

  jq -n \
    --arg run_id "$RUN_ID" \
    --arg phase "$PHASE" \
    --arg mode "$MODE" \
    --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg source_cache_root "$SOURCE_CACHE_ROOT" \
    --arg phase_cache_root "$PHASE_CACHE_ROOT" \
    --arg opencode_config_dir "${OPENCODE_CONFIG_DIR}" \
    --arg active_branch "$ACTIVE_BRANCH" \
    --arg head_commit "$HEAD_COMMIT" \
    --arg head_short "$HEAD_SHORT" \
    --arg git_dirty "$GIT_DIRTY" \
    --arg semantic_model_id "$SEMANTIC_MODEL_ID" \
    --arg target_parts "$TARGET_PARTS" \
    --argjson part "$PART" \
    --argjson max_pages "$MAX_PAGES" \
    --argjson lexical_k "$LEXICAL_K" \
    --argjson semantic_k "$SEMANTIC_K" \
    --argjson rrf_k "$RRF_K" \
    --argjson timeout_ms "$TIMEOUT_MS" \
    --argjson snapshot_limit "$SNAPSHOT_LIMIT" \
    --arg bench_profile "$BENCH_PROFILE" \
    --argjson bench_repeats_lite "$BENCH_REPEATS_LITE" \
    --argjson bench_repeats_full "$BENCH_REPEATS_FULL" \
    --arg quick_report_path "$QUICK_STAGE_B_REPORT_PATH" \
    --arg lexical_snapshot_path "$LEXICAL_SNAPSHOT_PATH" \
    --arg semantic_snapshot_path "$SEMANTIC_SNAPSHOT_PATH" \
    --arg lockfile_path "$LOCKFILE_PATH" \
    --argjson full_report_path "$full_report_json" \
    --argjson benchmark_runs "$benchmark_runs_json" \
    --argjson benchmark_selected "$benchmark_selected_json" \
    '{
      manifest_version: 1,
      run_id: $run_id,
      phase: $phase,
      mode: $mode,
      generated_at: $generated_at,
      source_cache_root: $source_cache_root,
      phase_cache_root: $phase_cache_root,
      opencode_config_dir: $opencode_config_dir,
      git: {
        branch: $active_branch,
        head_commit: $head_commit,
        head_short: $head_short,
        dirty: ($git_dirty == "true")
      },
      controls: {
        semantic_model_id: $semantic_model_id,
        part: $part,
        max_pages: $max_pages,
        target_parts: ($target_parts | split(" ") | map(select(length > 0))),
        lexical_k: $lexical_k,
        semantic_k: $semantic_k,
        rrf_k: $rrf_k,
        timeout_ms: $timeout_ms,
        snapshot_limit: $snapshot_limit,
        bench_profile: $bench_profile,
        bench_repeats_lite: $bench_repeats_lite,
        bench_repeats_full: $bench_repeats_full
      },
      artifacts: {
        quick_stage_b_report: $quick_report_path,
        full_stage_b_report: $full_report_path,
        lexical_snapshot: $lexical_snapshot_path,
        semantic_snapshot: $semantic_snapshot_path,
        benchmark_runs: $benchmark_runs,
        benchmark_selected: $benchmark_selected,
        semantic_model_lockfile: $lockfile_path
      }
    }' >"$CAPTURE_MANIFEST_PATH"
}

seed_phase_cache

log "Running compile and tests"
cargo check
cargo test
cargo build --quiet
BIN_PATH="target/debug/iso26262"

run_smoke_checks
run_refresh_quick_stage

if [[ "$MODE" == "full" ]]; then
  run_refresh_full_stage
fi

if [[ "$MODE" == "full" ]]; then
  run_benchmark_capture "$BENCH_REPEATS_FULL"
else
  run_benchmark_capture "$BENCH_REPEATS_LITE"
fi

capture_snapshots
write_capture_manifest

log "Capture complete"
log "Run ID: $RUN_ID"
log "Phase: $PHASE"
log "Mode: $MODE"
log "Capture manifest: $CAPTURE_MANIFEST_PATH"
