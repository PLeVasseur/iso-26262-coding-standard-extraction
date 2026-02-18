def mean:
  if length == 0 then null
  else (add / length)
  end;

def percentile(p):
  if length == 0 then null
  else
    (sort) as $sorted
    | ($sorted | length) as $n
    | (($n - 1) * p) as $index
    | ($index | floor) as $low
    | ($index | ceil) as $high
    | if $low == $high then
        $sorted[$low]
      else
        ($sorted[$low] + ($sorted[$high] - $sorted[$low]) * ($index - $low))
      end
  end;

def numeric_stats(values):
  {
    min: (values | min?),
    max: (values | max?),
    mean: (values | mean),
    p50: (values | percentile(0.50)),
    p95: (values | percentile(0.95)),
    p99: (values | percentile(0.99))
  };

def mode_summary(mode_name):
  ($timed_records | map(select(.mode == mode_name and .phase == "timed"))) as $rows
  | ($failures | map(select(.mode == mode_name and .phase == "timed"))) as $mode_failures
  | ($timed_passes * $query_count) as $expected
  | {
      mode: mode_name,
      expected_timed_queries: $expected,
      completed_timed_queries: ($rows | length),
      timed_failure_count: ($mode_failures | length),
      failure_rate: (
        if $expected == 0 then 1.0
        else (($mode_failures | length) / $expected)
        end
      ),
      latency_ms: numeric_stats($rows | map(.latency_ms)),
      wall_ms: numeric_stats($rows | map(.wall_ms)),
      returned_results: numeric_stats($rows | map(.returned)),
      fallback_used_rate: (
        if ($rows | length) == 0 then null
        else (($rows | map(select(.fallback_used == true)) | length) / ($rows | length))
        end
      ),
      candidate_counts: {
        lexical: numeric_stats($rows | map(.lexical_candidate_count)),
        semantic: numeric_stats($rows | map(.semantic_candidate_count)),
        fused: numeric_stats($rows | map(.fused_candidate_count))
      },
      sample_query_ids: ($rows | map(.query_id) | unique | .[:8])
    };

($modes[0]) as $mode_list
| {
    manifest_version: 1,
    run_id: $run_id,
    generated_at: $generated_at,
    benchmark_scope: {
      profile: $bench_profile,
      cache_root: $cache_root,
      query_manifest_path: $query_manifest_path,
      query_snapshot_path: $query_snapshot_path,
      query_count: $query_count,
      warmup_passes: $warmup_passes,
      timed_passes: $timed_passes,
      modes: $mode_list,
      timeout_ms: $timeout_ms,
      lexical_k: $lexical_k,
      semantic_k: $semantic_k,
      rrf_k: $rrf_k,
      semantic_model_id: $semantic_model_id
    },
    environment: {
      cpu_model: $cpu_model,
      cpu_cores: ($cpu_cores | if . == "" then null else . end),
      cpu_threads: ($cpu_threads | if . == "" then null else . end),
      memory_total_kb: ($memory_total_kb | if . == "" then null else . end),
      kernel: $kernel,
      rustc: $rustc_version,
      cargo: $cargo_version,
      filesystem_device: ($filesystem_device | if . == "" then null else . end),
      filesystem_type: ($filesystem_type | if . == "" then null else . end),
      filesystem_mount: ($filesystem_mount | if . == "" then null else . end)
    },
    mode_summaries: [
      $mode_list[] as $mode_name
      | mode_summary($mode_name)
    ],
    failures: $failures,
    overall: {
      timed_query_count: ($timed_records | map(select(.phase == "timed")) | length),
      timed_failure_count: ($failures | map(select(.phase == "timed")) | length),
      valid: ([
        $mode_list[] as $mode_name
        | mode_summary($mode_name).failure_rate
      ] | all(. <= 0.01))
    }
  }
