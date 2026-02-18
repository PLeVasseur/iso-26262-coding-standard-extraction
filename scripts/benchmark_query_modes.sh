#!/usr/bin/env bash
set -euo pipefail
CACHE_ROOT="${CACHE_ROOT:-.cache/iso26262}"
QUERY_MANIFEST_PATH="${QUERY_MANIFEST_PATH:-${CACHE_ROOT}/manifests/semantic_eval_queries.json}"
SEMANTIC_MODEL_ID="${SEMANTIC_MODEL_ID:-miniLM-L6-v2-local-v1}"
BENCH_PROFILE="${BENCH_PROFILE:-quick}"
LEXICAL_K="${LEXICAL_K:-96}"
SEMANTIC_K="${SEMANTIC_K:-96}"
RRF_K="${RRF_K:-60}"
TIMEOUT_MS="${TIMEOUT_MS:-2000}"
WARMUP_PASSES="${WARMUP_PASSES:-}"
TIMED_PASSES="${TIMED_PASSES:-}"
BENCH_QUERY_LIMIT="${BENCH_QUERY_LIMIT:-}"
MODES="${MODES:-lexical semantic hybrid}"
OUTPUT_DIR="${OUTPUT_DIR:-${CACHE_ROOT}/manifests}"
BENCH_PROGRESS="${BENCH_PROGRESS:-0}"
BENCH_PROGRESS_EVERY="${BENCH_PROGRESS_EVERY:-25}"
BENCH_REPORT_FILTER_PATH="${BENCH_REPORT_FILTER_PATH:-scripts/lib/benchmark/report.jq}"
RUN_TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${RUN_ID:-bench-${RUN_TIMESTAMP}}"
OUTPUT_PATH="${OUTPUT_PATH:-${OUTPUT_DIR}/semantic_benchmark_${RUN_TIMESTAMP}.json}"
QUERY_SNAPSHOT_PATH="${OUTPUT_DIR}/semantic_benchmark_queries_${RUN_TIMESTAMP}.tsv"
for command_name in cargo jq sha256sum sort awk wc mktemp tr date; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "[bench] missing required command: ${command_name}" >&2
    exit 1
  fi
done
if [[ ! -f "${QUERY_MANIFEST_PATH}" ]]; then
  echo "[bench] missing query manifest: ${QUERY_MANIFEST_PATH}" >&2
  echo "[bench] run validate first to generate semantic eval queries" >&2
  exit 1
fi

if [[ ! -f "${BENCH_REPORT_FILTER_PATH}" ]]; then
  echo "[bench] missing benchmark jq filter: ${BENCH_REPORT_FILTER_PATH}" >&2
  exit 1
fi
mkdir -p "${OUTPUT_DIR}"
for mode in ${MODES}; do
  case "${mode}" in
    lexical|semantic|hybrid) ;;
    *)
      echo "[bench] unsupported mode in MODES: ${mode}" >&2
      exit 1
      ;;
  esac
done
is_truthy() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}
format_duration_hms() {
  local total_seconds="$1"
  if [[ -z "${total_seconds}" || "${total_seconds}" -lt 0 ]]; then
    total_seconds=0
  fi
  local hours
  local minutes
  local seconds
  hours=$((total_seconds / 3600))
  minutes=$(((total_seconds % 3600) / 60))
  seconds=$((total_seconds % 60))
  printf "%02d:%02d:%02d" "${hours}" "${minutes}" "${seconds}"
}

apply_bench_profile_defaults() {
  local profile
  profile="$(tr '[:upper:]' '[:lower:]' <<<"${BENCH_PROFILE}")"

  local default_limit
  local default_warmup
  local default_timed
  case "${profile}" in
    quick)
      default_limit=20
      default_warmup=1
      default_timed=1
      ;;
    standard)
      default_limit=30
      default_warmup=1
      default_timed=2
      ;;
    full)
      default_limit=0
      default_warmup=2
      default_timed=5
      ;;
    *)
      echo "[bench] unsupported BENCH_PROFILE: ${BENCH_PROFILE} (allowed: quick, standard, full)" >&2
      exit 1
      ;;
  esac

  BENCH_PROFILE="${profile}"
  BENCH_QUERY_LIMIT="${BENCH_QUERY_LIMIT:-${default_limit}}"
  WARMUP_PASSES="${WARMUP_PASSES:-${default_warmup}}"
  TIMED_PASSES="${TIMED_PASSES:-${default_timed}}"
}

apply_bench_profile_defaults

if ! [[ "${WARMUP_PASSES}" =~ ^[0-9]+$ ]] || ! [[ "${TIMED_PASSES}" =~ ^[0-9]+$ ]]; then
  echo "[bench] WARMUP_PASSES and TIMED_PASSES must be integers >= 0" >&2
  exit 1
fi

if ! [[ "${BENCH_QUERY_LIMIT}" =~ ^[0-9]+$ ]]; then
  echo "[bench] BENCH_QUERY_LIMIT must be an integer >= 0" >&2
  exit 1
fi

echo "[bench] profile=${BENCH_PROFILE} limit=${BENCH_QUERY_LIMIT} warmup=${WARMUP_PASSES} timed=${TIMED_PASSES} modes=${MODES}"

echo "[bench] building binary"
cargo build >/dev/null
BIN_PATH="target/debug/iso26262"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT
base_queries_tsv="${tmp_dir}/queries_base.tsv"
jq -r '
  .queries
  | sort_by(.query_id)
  | .[]
  | [
      .query_id,
      .query_text,
      (.part_filter // ""),
      (.chunk_type_filter // "")
    ]
  | @tsv
' "${QUERY_MANIFEST_PATH}" >"${base_queries_tsv}"

if [[ "${BENCH_QUERY_LIMIT}" =~ ^[0-9]+$ ]] && [[ "${BENCH_QUERY_LIMIT}" -gt 0 ]]; then
  awk -F'\t' -v limit="${BENCH_QUERY_LIMIT}" 'NR <= limit { print }' "${base_queries_tsv}" >"${base_queries_tsv}.limited"
  mv "${base_queries_tsv}.limited" "${base_queries_tsv}"
fi

query_count="$(wc -l < "${base_queries_tsv}" | tr -d ' ')"
if [[ "${query_count}" -eq 0 ]]; then
  echo "[bench] query manifest produced zero benchmark queries" >&2
  exit 1
fi

if ! [[ "${BENCH_PROGRESS_EVERY}" =~ ^[0-9]+$ ]]; then
  echo "[bench] BENCH_PROGRESS_EVERY must be an integer >= 0" >&2
  exit 1
fi

mode_count="$(wc -w <<<"${MODES}" | tr -d ' ')"
total_passes=$((WARMUP_PASSES + TIMED_PASSES))
total_runs=$((query_count * mode_count * total_passes))
progress_completed_runs=0
progress_started_epoch="$(date +%s)"

maybe_log_progress() {
  local mode="$1"
  local phase="$2"
  local pass_index="$3"
  local query_index="$4"
  local query_id="$5"
  progress_completed_runs=$((progress_completed_runs + 1))
  if ! is_truthy "${BENCH_PROGRESS}"; then
    return 0
  fi
  local should_log=0
  if [[ "${progress_completed_runs}" -eq 1 || "${progress_completed_runs}" -eq "${total_runs}" ]]; then
    should_log=1
  elif [[ "${BENCH_PROGRESS_EVERY}" -gt 0 ]] && ((progress_completed_runs % BENCH_PROGRESS_EVERY == 0)); then
    should_log=1
  fi
  if [[ "${should_log}" -ne 1 ]]; then
    return 0
  fi
  local now_epoch
  local elapsed_seconds
  local remaining_runs
  local eta_seconds
  local progress_pct
  now_epoch="$(date +%s)"
  elapsed_seconds=$((now_epoch - progress_started_epoch))
  remaining_runs=$((total_runs - progress_completed_runs))
  if [[ "${progress_completed_runs}" -gt 0 && "${elapsed_seconds}" -gt 0 ]]; then
    eta_seconds=$((elapsed_seconds * remaining_runs / progress_completed_runs))
  else
    eta_seconds=0
  fi
  progress_pct="$(awk -v done="${progress_completed_runs}" -v total="${total_runs}" 'BEGIN { if (total <= 0) printf "0.00"; else printf "%.2f", done * 100.0 / total }')"
  echo "[bench][progress] ${progress_completed_runs}/${total_runs} (${progress_pct}%) mode=${mode} phase=${phase} pass=${pass_index} query=${query_index}/${query_count} id=${query_id} elapsed=$(format_duration_hms "${elapsed_seconds}") eta=$(format_duration_hms "${eta_seconds}")" >&2
}
cp "${base_queries_tsv}" "${QUERY_SNAPSHOT_PATH}"
echo "[bench] benchmark query snapshot: ${QUERY_SNAPSHOT_PATH} (${query_count} queries)"

timed_records_jsonl="${tmp_dir}/timed_records.jsonl"
failures_jsonl="${tmp_dir}/failures.jsonl"
modes_json="${tmp_dir}/modes.json"
: >"${timed_records_jsonl}"
: >"${failures_jsonl}"
printf '%s\n' ${MODES} | jq -R . | jq -s . >"${modes_json}"

build_pass_order() {
  local pass_key="$1"
  local output_file="$2"

  : >"${output_file}"
  while IFS=$'\t' read -r query_id query_text part_filter chunk_type_filter; do
    local order_hash
    order_hash="$(printf '%s|%s' "${pass_key}" "${query_id}" | sha256sum | awk '{print $1}')"
    printf '%s\t%s\t%s\t%s\t%s\n' \
      "${order_hash}" \
      "${query_id}" \
      "${query_text}" \
      "${part_filter}" \
      "${chunk_type_filter}" >>"${output_file}"
  done <"${base_queries_tsv}"

  sort -t $'\t' -k1,1 "${output_file}" | awk -F'\t' '{print $2"\t"$3"\t"$4"\t"$5}' >"${output_file}.sorted"
  mv "${output_file}.sorted" "${output_file}"
}

run_query_case() {
  local mode="$1"
  local phase="$2"
  local pass_index="$3"
  local query_id="$4"
  local query_text="$5"
  local part_filter="$6"
  local chunk_type_filter="$7"
  local query_index="$8"

  local args
  args=(
    "${BIN_PATH}" query
    --cache-root "${CACHE_ROOT}"
    --query "${query_text}"
    --retrieval-mode "${mode}"
    --lexical-k "${LEXICAL_K}"
    --semantic-k "${SEMANTIC_K}"
    --rrf-k "${RRF_K}"
    --timeout-ms "${TIMEOUT_MS}"
    --json
  )

  if [[ -n "${part_filter}" ]]; then
    args+=(--part "${part_filter}")
  fi

  if [[ -n "${chunk_type_filter}" ]]; then
    args+=(--type "${chunk_type_filter}")
  fi

  if [[ "${mode}" != "lexical" ]]; then
    args+=(--semantic-model-id "${SEMANTIC_MODEL_ID}")
  fi

  local stderr_file
  stderr_file="${tmp_dir}/stderr_${mode}_${phase}_${pass_index}_${query_id}.log"

  local wall_started_ns
  local wall_finished_ns
  wall_started_ns="$(date +%s%N)"
  local response
  if ! response="$("${args[@]}" 2>"${stderr_file}")"; then
    wall_finished_ns="$(date +%s%N)"
    local wall_ms
    wall_ms="$(awk -v start="${wall_started_ns}" -v end="${wall_finished_ns}" 'BEGIN { printf "%.3f", (end-start)/1000000 }')"
    local failure_reason
    failure_reason="$(tr '\n' ' ' <"${stderr_file}" | awk '{gsub(/[[:space:]]+/, " "); print}')"
    jq -cn \
      --arg mode "${mode}" \
      --arg phase "${phase}" \
      --arg query_id "${query_id}" \
      --arg query_text "${query_text}" \
      --arg reason "${failure_reason}" \
      --argjson pass_index "${pass_index}" \
      --argjson wall_ms "${wall_ms}" \
      '{
        mode: $mode,
        phase: $phase,
        pass_index: $pass_index,
        query_id: $query_id,
        query_text: $query_text,
        wall_ms: $wall_ms,
        reason: $reason
      }' >>"${failures_jsonl}"
    maybe_log_progress "${mode}" "${phase}" "${pass_index}" "${query_index}" "${query_id}"
    return 1
  fi
  wall_finished_ns="$(date +%s%N)"

  local wall_ms
  wall_ms="$(awk -v start="${wall_started_ns}" -v end="${wall_finished_ns}" 'BEGIN { printf "%.3f", (end-start)/1000000 }')"

  local query_duration_ms
  query_duration_ms="$(jq -r '.retrieval.query_duration_ms // empty' <<<"${response}" 2>/dev/null || true)"
  if [[ -z "${query_duration_ms}" || "${query_duration_ms}" == "null" ]]; then
    query_duration_ms="${wall_ms}"
  fi

  local lexical_candidate_count
  local semantic_candidate_count
  local fused_candidate_count
  local returned
  local fallback_used

  lexical_candidate_count="$(jq -r '.retrieval.lexical_candidate_count // 0' <<<"${response}" 2>/dev/null || echo 0)"
  semantic_candidate_count="$(jq -r '.retrieval.semantic_candidate_count // 0' <<<"${response}" 2>/dev/null || echo 0)"
  fused_candidate_count="$(jq -r '.retrieval.fused_candidate_count // 0' <<<"${response}" 2>/dev/null || echo 0)"
  returned="$(jq -r '.returned // 0' <<<"${response}" 2>/dev/null || echo 0)"
  fallback_used="$(jq -r '.retrieval.fallback_used // false' <<<"${response}" 2>/dev/null || echo false)"

  if [[ "${phase}" == "timed" ]]; then
    jq -cn \
      --arg mode "${mode}" \
      --arg phase "${phase}" \
      --arg query_id "${query_id}" \
      --arg query_text "${query_text}" \
      --argjson pass_index "${pass_index}" \
      --argjson latency_ms "${query_duration_ms}" \
      --argjson wall_ms "${wall_ms}" \
      --argjson lexical_candidate_count "${lexical_candidate_count}" \
      --argjson semantic_candidate_count "${semantic_candidate_count}" \
      --argjson fused_candidate_count "${fused_candidate_count}" \
      --argjson returned "${returned}" \
      --argjson fallback_used "${fallback_used}" \
      '{
        mode: $mode,
        phase: $phase,
        pass_index: $pass_index,
        query_id: $query_id,
        query_text: $query_text,
        latency_ms: $latency_ms,
        wall_ms: $wall_ms,
        lexical_candidate_count: $lexical_candidate_count,
        semantic_candidate_count: $semantic_candidate_count,
        fused_candidate_count: $fused_candidate_count,
        returned: $returned,
        fallback_used: $fallback_used
      }' >>"${timed_records_jsonl}"
  fi

  maybe_log_progress "${mode}" "${phase}" "${pass_index}" "${query_index}" "${query_id}"
}

for mode in ${MODES}; do
  for pass_index in $(seq 1 "${WARMUP_PASSES}"); do
    if is_truthy "${BENCH_PROGRESS}"; then
      echo "[bench][progress] starting mode=${mode} phase=warmup pass=${pass_index}/${WARMUP_PASSES}"
    fi
    pass_queries_tsv="${tmp_dir}/queries_warmup_${mode}_${pass_index}.tsv"
    build_pass_order "warmup-${pass_index}" "${pass_queries_tsv}"
    query_index=0
    while IFS=$'\t' read -r query_id query_text part_filter chunk_type_filter; do
      query_index=$((query_index + 1))
      run_query_case "${mode}" "warmup" "${pass_index}" "${query_id}" "${query_text}" "${part_filter}" "${chunk_type_filter}" "${query_index}" >/dev/null || true
    done <"${pass_queries_tsv}"
  done
done

for mode in ${MODES}; do
  for pass_index in $(seq 1 "${TIMED_PASSES}"); do
    if is_truthy "${BENCH_PROGRESS}"; then
      echo "[bench][progress] starting mode=${mode} phase=timed pass=${pass_index}/${TIMED_PASSES}"
    fi
    pass_queries_tsv="${tmp_dir}/queries_timed_${mode}_${pass_index}.tsv"
    build_pass_order "timed-${pass_index}" "${pass_queries_tsv}"
    query_index=0
    while IFS=$'\t' read -r query_id query_text part_filter chunk_type_filter; do
      query_index=$((query_index + 1))
      run_query_case "${mode}" "timed" "${pass_index}" "${query_id}" "${query_text}" "${part_filter}" "${chunk_type_filter}" "${query_index}" >/dev/null || true
    done <"${pass_queries_tsv}"
  done
done

cpu_model="$(awk -F: '/model name/ {gsub(/^ +/, "", $2); print $2; exit}' /proc/cpuinfo 2>/dev/null || true)"
cpu_cores="$(awk -F: '/cpu cores/ {gsub(/^ +/, "", $2); print $2; exit}' /proc/cpuinfo 2>/dev/null || true)"
cpu_threads="$(nproc --all 2>/dev/null || nproc 2>/dev/null || echo "")"
memory_total_kb="$(awk '/MemTotal/ {print $2; exit}' /proc/meminfo 2>/dev/null || true)"
kernel="$(uname -srmo 2>/dev/null || uname -a)"
rustc_version="$(rustc --version 2>/dev/null || true)"
cargo_version="$(cargo --version 2>/dev/null || true)"

filesystem_row="$(df -T "${CACHE_ROOT}" 2>/dev/null | awk 'NR==2 {print $1"\t"$2"\t"$7}')"
filesystem_device="$(awk -F'\t' '{print $1}' <<<"${filesystem_row}" 2>/dev/null || true)"
filesystem_type="$(awk -F'\t' '{print $2}' <<<"${filesystem_row}" 2>/dev/null || true)"
filesystem_mount="$(awk -F'\t' '{print $3}' <<<"${filesystem_row}" 2>/dev/null || true)"

jq -n \
  --arg run_id "${RUN_ID}" \
  --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg bench_profile "${BENCH_PROFILE}" \
  --arg cache_root "${CACHE_ROOT}" \
  --arg query_manifest_path "${QUERY_MANIFEST_PATH}" \
  --arg query_snapshot_path "${QUERY_SNAPSHOT_PATH}" \
  --arg semantic_model_id "${SEMANTIC_MODEL_ID}" \
  --arg cpu_model "${cpu_model}" \
  --arg cpu_cores "${cpu_cores}" \
  --arg cpu_threads "${cpu_threads}" \
  --arg memory_total_kb "${memory_total_kb}" \
  --arg kernel "${kernel}" \
  --arg rustc_version "${rustc_version}" \
  --arg cargo_version "${cargo_version}" \
  --arg filesystem_device "${filesystem_device}" \
  --arg filesystem_type "${filesystem_type}" \
  --arg filesystem_mount "${filesystem_mount}" \
  --argjson query_count "${query_count}" \
  --argjson lexical_k "${LEXICAL_K}" \
  --argjson semantic_k "${SEMANTIC_K}" \
  --argjson rrf_k "${RRF_K}" \
  --argjson timeout_ms "${TIMEOUT_MS}" \
  --argjson warmup_passes "${WARMUP_PASSES}" \
  --argjson timed_passes "${TIMED_PASSES}" \
  --slurpfile timed_records "${timed_records_jsonl}" \
  --slurpfile failures "${failures_jsonl}" \
  --slurpfile modes "${modes_json}" \
  -f "${BENCH_REPORT_FILTER_PATH}" >"${OUTPUT_PATH}"

echo "[bench] wrote benchmark report: ${OUTPUT_PATH}"
jq -r '
  .mode_summaries[]
  | "[bench] mode=\(.mode) queries=\(.completed_timed_queries)/\(.expected_timed_queries) failures=\(.timed_failure_count) p50=\(.latency_ms.p50 // "n/a")ms p95=\(.latency_ms.p95 // "n/a")ms"
' "${OUTPUT_PATH}"

if is_truthy "${BENCH_PROGRESS}"; then
  bench_elapsed_seconds=$(( $(date +%s) - progress_started_epoch ))
  echo "[bench][progress] completed runs=${progress_completed_runs}/${total_runs} elapsed=$(format_duration_hms "${bench_elapsed_seconds}")"
fi

jq -e '.overall.valid == true' "${OUTPUT_PATH}" >/dev/null || {
  echo "[bench] benchmark invalid: timed failure rate exceeds 1% for at least one mode" >&2
  exit 1
}
