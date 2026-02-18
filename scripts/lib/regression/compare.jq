def mean:
  if length == 0 then null
  else (add / length)
  end;

def safe_rate($num; $den):
  if $den == 0 then null
  else ($num / $den)
  end;

def num_delta($before; $after):
  if $before == null or $after == null then null
  else ($after - $before)
  end;

def rel_increase($before; $after):
  if $before == null or $after == null then null
  elif $before == 0 then
    if $after == 0 then 0
    else null
    end
  else (($after - $before) / $before)
  end;

def non_passing($report):
  ($report.checks // [])
  | map(select((.result // "") != "pass") | .check_id)
  | unique
  | sort;

def summary_semantic_quality($report):
  {
    lexical_ndcg_at_10: ($report.semantic_quality.lexical_ndcg_at_10 // null),
    lexical_recall_at_50: ($report.semantic_quality.lexical_recall_at_50 // null),
    semantic_ndcg_at_10: ($report.semantic_quality.semantic_ndcg_at_10 // null),
    hybrid_ndcg_at_10: ($report.semantic_quality.hybrid_ndcg_at_10 // null),
    hybrid_recall_at_50: ($report.semantic_quality.hybrid_recall_at_50 // null),
    hybrid_mrr_at_10_first_hit: ($report.semantic_quality.hybrid_mrr_at_10_first_hit // null),
    citation_parity_top1: ($report.semantic_quality.citation_parity_top1 // null),
    retrieval_determinism_topk_overlap: ($report.semantic_quality.retrieval_determinism_topk_overlap // null),
    pinpoint_determinism_top1: ($report.semantic_quality.pinpoint_determinism_top1 // null),
    pinpoint_fallback_ratio: ($report.semantic_quality.pinpoint_fallback_ratio // null)
  };

def summary_citation_parity($report):
  {
    top1_parity: ($report.citation_parity.top1_parity // null),
    top3_containment: ($report.citation_parity.top3_containment // null),
    page_range_parity: ($report.citation_parity.page_range_parity // null)
  };

def snapshot_summary($rows):
  ($rows // []) as $records
  | ($records | length) as $total
  | ($records | map(select((.status // "ok") != "ok")) | length) as $errors
  | ($records | map(select((.returned // 0) == 0)) | length) as $no_results
  | ($records | map(select((.timed_out // false) == true)) | length) as $timeouts
  | ($records | map(select((.fallback_used // false) == true)) | length) as $fallbacks
  | ($records | map(select((.status // "ok") == "ok" and (.must_hit_top1 // false) == true))) as $protected
  | ($protected
     | map(select((.top1_chunk_id // null) as $top1 | $top1 != null and ((.expected_chunk_ids // []) | index($top1) != null)))
     | length) as $protected_hits
  | ($records | map(select((.status // "ok") == "ok" and (.candidate_counts.lexical // null) != null) | .candidate_counts.lexical)) as $lexical_counts
  | ($records | map(select((.status // "ok") == "ok" and (.candidate_counts.semantic // null) != null) | .candidate_counts.semantic)) as $semantic_counts
  | ($records | map(select((.status // "ok") == "ok" and (.candidate_counts.fused // null) != null) | .candidate_counts.fused)) as $fused_counts
  | {
      total_queries: $total,
      error_count: $errors,
      no_result_rate: safe_rate($no_results; $total),
      timeout_rate: safe_rate($timeouts; $total),
      fallback_used_rate: safe_rate($fallbacks; $total),
      protected_query_count: ($protected | length),
      top1_expected_hit_rate: safe_rate($protected_hits; ($protected | length)),
      avg_lexical_candidate_count: ($lexical_counts | mean),
      avg_semantic_candidate_count: ($semantic_counts | mean),
      avg_fused_candidate_count: ($fused_counts | mean)
    };

def unique_values($values):
  ($values // []) | unique;

def jaccard($left; $right):
  (unique_values($left)) as $a
  | (unique_values($right)) as $b
  | (($a + $b) | unique) as $union
  | if ($union | length) == 0 then 1.0
    else (([$a[] | select($b | index(.) != null)] | unique | length) / ($union | length))
    end;

def overlap_summary($before_rows; $after_rows):
  ($before_rows // []) as $before
  | ($after_rows // []) as $after
  | ($before
     | map(select((.status // "ok") == "ok")
           | {key: .query_id, value: {top_chunk_ids: (.top_chunk_ids // []), top1_chunk_id: (.top1_chunk_id // null)}})
     | from_entries) as $before_map
  | ($after
     | map(select((.status // "ok") == "ok")
           | {key: .query_id, value: {top_chunk_ids: (.top_chunk_ids // []), top1_chunk_id: (.top1_chunk_id // null)}})
     | from_entries) as $after_map
  | ($before_map | keys_unsorted) as $before_keys
  | ($after_map | keys_unsorted) as $after_keys
  | ([$before_keys[] | select($after_keys | index(.) != null)] | sort) as $common_keys
  | ($common_keys | map(jaccard($before_map[.].top_chunk_ids; $after_map[.].top_chunk_ids))) as $jaccards
  | ($common_keys | map(select($before_map[.].top1_chunk_id == $after_map[.].top1_chunk_id)) | length) as $top1_same
  | {
      query_pair_count: ($common_keys | length),
      avg_jaccard_at_10: ($jaccards | mean),
      top1_identity_rate: safe_rate($top1_same; ($common_keys | length))
    };

def benchmark_mode($bench; $mode_name):
  (($bench.mode_summaries // []) | map(select(.mode == $mode_name)) | first // null);

def benchmark_mode_delta($before_bench; $after_bench; $mode_name):
  (benchmark_mode($before_bench; $mode_name)) as $before_mode
  | (benchmark_mode($after_bench; $mode_name)) as $after_mode
  | {
      mode: $mode_name,
      before: $before_mode,
      after: $after_mode,
      latency_ms_delta: {
        p50: num_delta($before_mode.latency_ms.p50; $after_mode.latency_ms.p50),
        p95: num_delta($before_mode.latency_ms.p95; $after_mode.latency_ms.p95),
        p99: num_delta($before_mode.latency_ms.p99; $after_mode.latency_ms.p99),
        mean: num_delta($before_mode.latency_ms.mean; $after_mode.latency_ms.mean)
      },
      wall_ms_delta: {
        p50: num_delta($before_mode.wall_ms.p50; $after_mode.wall_ms.p50),
        p95: num_delta($before_mode.wall_ms.p95; $after_mode.wall_ms.p95),
        p99: num_delta($before_mode.wall_ms.p99; $after_mode.wall_ms.p99),
        mean: num_delta($before_mode.wall_ms.mean; $after_mode.wall_ms.mean)
      }
    };

def p95_soft_limit($before_p95; $soft):
  if $before_p95 == null then null
  else [
    ($soft.benchmark_p95_ms_max_increase // 5),
    ($before_p95 * ($soft.benchmark_p95_rel_max_increase // 0.1))
  ] | max
  end;

($thresholds[0] // {}) as $threshold
| ($threshold.hard // {}) as $hard
| ($threshold.soft // {}) as $soft
| ($quick_before[0] // {}) as $quick_before_report
| ($quick_after[0] // {}) as $quick_after_report
| ($full_before[0] // null) as $full_before_report
| ($full_after[0] // null) as $full_after_report
| ($benchmark_before[0] // {}) as $benchmark_before_report
| ($benchmark_after[0] // {}) as $benchmark_after_report
| ($lexical_before[0] // []) as $lexical_before_rows
| ($lexical_after[0] // []) as $lexical_after_rows
| ($semantic_before[0] // []) as $semantic_before_rows
| ($semantic_after[0] // []) as $semantic_after_rows
| (non_passing($quick_before_report)) as $quick_before_non_passing
| (non_passing($quick_after_report)) as $quick_after_non_passing
| ([$quick_after_non_passing[] | select($quick_before_non_passing | index(.) | not)] | sort) as $quick_new_failed_checks
| (summary_semantic_quality($quick_before_report)) as $quick_semantic_before
| (summary_semantic_quality($quick_after_report)) as $quick_semantic_after
| (summary_citation_parity($quick_before_report)) as $quick_citation_before
| (summary_citation_parity($quick_after_report)) as $quick_citation_after
| (if ($mode == "full") then non_passing($full_before_report) else [] end) as $full_before_non_passing
| (if ($mode == "full") then non_passing($full_after_report) else [] end) as $full_after_non_passing
| ([$full_after_non_passing[] | select($full_before_non_passing | index(.) | not)] | sort) as $full_new_failed_checks
| {
    lexical: {
      before: snapshot_summary($lexical_before_rows),
      after: snapshot_summary($lexical_after_rows),
      overlap: overlap_summary($lexical_before_rows; $lexical_after_rows)
    },
    semantic: {
      before: snapshot_summary($semantic_before_rows),
      after: snapshot_summary($semantic_after_rows),
      overlap: overlap_summary($semantic_before_rows; $semantic_after_rows)
    }
  } as $snapshots
| {
    lexical: benchmark_mode_delta($benchmark_before_report; $benchmark_after_report; "lexical"),
    semantic: benchmark_mode_delta($benchmark_before_report; $benchmark_after_report; "semantic"),
    hybrid: benchmark_mode_delta($benchmark_before_report; $benchmark_after_report; "hybrid")
  } as $benchmark_mode_deltas
| [
    if (($hard.new_failed_checks_block // true) and ($quick_new_failed_checks | length) > 0) then
      {
        id: "H-NEW-FAILED-CHECKS-QUICK",
        message: "new failed checks appeared in quick Stage B report",
        observed: $quick_new_failed_checks,
        threshold: "none"
      }
    else empty end,

    if ($mode == "full" and ($full_new_failed_checks | length) > 0) then
      {
        id: "H-NEW-FAILED-CHECKS-FULL",
        message: "new failed checks appeared in full Stage B report",
        observed: $full_new_failed_checks,
        threshold: "none"
      }
    else empty end,

    if ($quick_semantic_after.retrieval_determinism_topk_overlap != null and
        $quick_semantic_after.retrieval_determinism_topk_overlap < ($hard.determinism_min // 1.0)) then
      {
        id: "H-DETERMINISM-RETRIEVAL",
        message: "retrieval determinism is below hard minimum",
        observed: $quick_semantic_after.retrieval_determinism_topk_overlap,
        threshold: ($hard.determinism_min // 1.0)
      }
    else empty end,

    if ($quick_semantic_after.pinpoint_determinism_top1 != null and
        $quick_semantic_after.pinpoint_determinism_top1 < ($hard.determinism_min // 1.0)) then
      {
        id: "H-DETERMINISM-PINPOINT",
        message: "pinpoint determinism is below hard minimum",
        observed: $quick_semantic_after.pinpoint_determinism_top1,
        threshold: ($hard.determinism_min // 1.0)
      }
    else empty end,

    if (num_delta($quick_citation_before.top1_parity; $quick_citation_after.top1_parity) != null and
        num_delta($quick_citation_before.top1_parity; $quick_citation_after.top1_parity) < ($hard.citation_top1_min_delta // 0.0)) then
      {
        id: "H-CITATION-TOP1",
        message: "citation top-1 parity regressed",
        observed: num_delta($quick_citation_before.top1_parity; $quick_citation_after.top1_parity),
        threshold: ($hard.citation_top1_min_delta // 0.0)
      }
    else empty end,

    if ($benchmark_after_report.overall.valid != true) then
      {
        id: "H-BENCHMARK-INVALID",
        message: "benchmark overall validity is false",
        observed: $benchmark_after_report.overall.valid,
        threshold: true
      }
    else empty end,

    if ($benchmark_after_report.overall.timed_failure_count // 0) > ($benchmark_before_report.overall.timed_failure_count // 0) then
      {
        id: "H-BENCHMARK-FAILURES-INCREASED",
        message: "timed benchmark failures increased",
        observed: {
          before: ($benchmark_before_report.overall.timed_failure_count // 0),
          after: ($benchmark_after_report.overall.timed_failure_count // 0)
        },
        threshold: "after <= before"
      }
    else empty end,

    if ($snapshots.lexical.before.top1_expected_hit_rate != null and
        $snapshots.lexical.after.top1_expected_hit_rate != null and
        $snapshots.lexical.before.top1_expected_hit_rate >= ($hard.top1_expected_hit_floor // 0.95) and
        $snapshots.lexical.after.top1_expected_hit_rate < ($hard.top1_expected_hit_floor // 0.95)) then
      {
        id: "H-LEXICAL-TOP1-HIT-FLOOR",
        message: "lexical top-1 expected hit rate crossed below hard floor",
        observed: $snapshots.lexical.after.top1_expected_hit_rate,
        threshold: ($hard.top1_expected_hit_floor // 0.95)
      }
    else empty end,

    if ($snapshots.semantic.before.top1_expected_hit_rate != null and
        $snapshots.semantic.after.top1_expected_hit_rate != null and
        $snapshots.semantic.before.top1_expected_hit_rate >= ($hard.top1_expected_hit_floor // 0.95) and
        $snapshots.semantic.after.top1_expected_hit_rate < ($hard.top1_expected_hit_floor // 0.95)) then
      {
        id: "H-SEMANTIC-TOP1-HIT-FLOOR",
        message: "semantic top-1 expected hit rate crossed below hard floor",
        observed: $snapshots.semantic.after.top1_expected_hit_rate,
        threshold: ($hard.top1_expected_hit_floor // 0.95)
      }
    else empty end,

    if (num_delta($snapshots.lexical.before.no_result_rate; $snapshots.lexical.after.no_result_rate) != null and
        num_delta($snapshots.lexical.before.no_result_rate; $snapshots.lexical.after.no_result_rate) > ($hard.no_result_rate_max_increase // 0.05)) then
      {
        id: "H-LEXICAL-NO-RESULT-RATE",
        message: "lexical no-result rate increase exceeded hard threshold",
        observed: num_delta($snapshots.lexical.before.no_result_rate; $snapshots.lexical.after.no_result_rate),
        threshold: ($hard.no_result_rate_max_increase // 0.05)
      }
    else empty end,

    if (num_delta($snapshots.semantic.before.no_result_rate; $snapshots.semantic.after.no_result_rate) != null and
        num_delta($snapshots.semantic.before.no_result_rate; $snapshots.semantic.after.no_result_rate) > ($hard.no_result_rate_max_increase // 0.05)) then
      {
        id: "H-SEMANTIC-NO-RESULT-RATE",
        message: "semantic no-result rate increase exceeded hard threshold",
        observed: num_delta($snapshots.semantic.before.no_result_rate; $snapshots.semantic.after.no_result_rate),
        threshold: ($hard.no_result_rate_max_increase // 0.05)
      }
    else empty end,

    if (num_delta($snapshots.lexical.before.timeout_rate; $snapshots.lexical.after.timeout_rate) != null and
        num_delta($snapshots.lexical.before.timeout_rate; $snapshots.lexical.after.timeout_rate) > ($hard.timeout_rate_max_increase // 0.02)) then
      {
        id: "H-LEXICAL-TIMEOUT-RATE",
        message: "lexical timeout rate increase exceeded hard threshold",
        observed: num_delta($snapshots.lexical.before.timeout_rate; $snapshots.lexical.after.timeout_rate),
        threshold: ($hard.timeout_rate_max_increase // 0.02)
      }
    else empty end,

    if (num_delta($snapshots.semantic.before.timeout_rate; $snapshots.semantic.after.timeout_rate) != null and
        num_delta($snapshots.semantic.before.timeout_rate; $snapshots.semantic.after.timeout_rate) > ($hard.timeout_rate_max_increase // 0.02)) then
      {
        id: "H-SEMANTIC-TIMEOUT-RATE",
        message: "semantic timeout rate increase exceeded hard threshold",
        observed: num_delta($snapshots.semantic.before.timeout_rate; $snapshots.semantic.after.timeout_rate),
        threshold: ($hard.timeout_rate_max_increase // 0.02)
      }
    else empty end
  ] as $hard_failures
| [
    if (num_delta($quick_semantic_before.lexical_ndcg_at_10; $quick_semantic_after.lexical_ndcg_at_10) != null and
        num_delta($quick_semantic_before.lexical_ndcg_at_10; $quick_semantic_after.lexical_ndcg_at_10) < -($soft.metric_abs_drop_max // 0.005)) then
      {
        id: "S-LEXICAL-NDCG",
        message: "lexical nDCG@10 dropped beyond soft threshold",
        observed: num_delta($quick_semantic_before.lexical_ndcg_at_10; $quick_semantic_after.lexical_ndcg_at_10),
        threshold: -($soft.metric_abs_drop_max // 0.005)
      }
    else empty end,

    if (num_delta($quick_semantic_before.lexical_recall_at_50; $quick_semantic_after.lexical_recall_at_50) != null and
        num_delta($quick_semantic_before.lexical_recall_at_50; $quick_semantic_after.lexical_recall_at_50) < -($soft.metric_abs_drop_max // 0.005)) then
      {
        id: "S-LEXICAL-RECALL",
        message: "lexical recall@50 dropped beyond soft threshold",
        observed: num_delta($quick_semantic_before.lexical_recall_at_50; $quick_semantic_after.lexical_recall_at_50),
        threshold: -($soft.metric_abs_drop_max // 0.005)
      }
    else empty end,

    if (num_delta($quick_semantic_before.semantic_ndcg_at_10; $quick_semantic_after.semantic_ndcg_at_10) != null and
        num_delta($quick_semantic_before.semantic_ndcg_at_10; $quick_semantic_after.semantic_ndcg_at_10) < -($soft.metric_abs_drop_max // 0.005)) then
      {
        id: "S-SEMANTIC-NDCG",
        message: "semantic nDCG@10 dropped beyond soft threshold",
        observed: num_delta($quick_semantic_before.semantic_ndcg_at_10; $quick_semantic_after.semantic_ndcg_at_10),
        threshold: -($soft.metric_abs_drop_max // 0.005)
      }
    else empty end,

    if (num_delta($quick_semantic_before.hybrid_ndcg_at_10; $quick_semantic_after.hybrid_ndcg_at_10) != null and
        num_delta($quick_semantic_before.hybrid_ndcg_at_10; $quick_semantic_after.hybrid_ndcg_at_10) < -($soft.metric_abs_drop_max // 0.005)) then
      {
        id: "S-HYBRID-NDCG",
        message: "hybrid nDCG@10 dropped beyond soft threshold",
        observed: num_delta($quick_semantic_before.hybrid_ndcg_at_10; $quick_semantic_after.hybrid_ndcg_at_10),
        threshold: -($soft.metric_abs_drop_max // 0.005)
      }
    else empty end,

    if (num_delta($quick_semantic_before.hybrid_recall_at_50; $quick_semantic_after.hybrid_recall_at_50) != null and
        num_delta($quick_semantic_before.hybrid_recall_at_50; $quick_semantic_after.hybrid_recall_at_50) < -($soft.metric_abs_drop_max // 0.005)) then
      {
        id: "S-HYBRID-RECALL",
        message: "hybrid recall@50 dropped beyond soft threshold",
        observed: num_delta($quick_semantic_before.hybrid_recall_at_50; $quick_semantic_after.hybrid_recall_at_50),
        threshold: -($soft.metric_abs_drop_max // 0.005)
      }
    else empty end,

    if (num_delta($quick_semantic_before.hybrid_mrr_at_10_first_hit; $quick_semantic_after.hybrid_mrr_at_10_first_hit) != null and
        num_delta($quick_semantic_before.hybrid_mrr_at_10_first_hit; $quick_semantic_after.hybrid_mrr_at_10_first_hit) < -($soft.metric_abs_drop_max // 0.005)) then
      {
        id: "S-HYBRID-MRR",
        message: "hybrid MRR@10 first-hit dropped beyond soft threshold",
        observed: num_delta($quick_semantic_before.hybrid_mrr_at_10_first_hit; $quick_semantic_after.hybrid_mrr_at_10_first_hit),
        threshold: -($soft.metric_abs_drop_max // 0.005)
      }
    else empty end,

    if (num_delta($snapshots.semantic.before.fallback_used_rate; $snapshots.semantic.after.fallback_used_rate) != null and
        num_delta($snapshots.semantic.before.fallback_used_rate; $snapshots.semantic.after.fallback_used_rate) > ($soft.fallback_ratio_max_increase // 0.02)) then
      {
        id: "S-FALLBACK-RATE",
        message: "semantic fallback-used rate increased beyond soft threshold",
        observed: num_delta($snapshots.semantic.before.fallback_used_rate; $snapshots.semantic.after.fallback_used_rate),
        threshold: ($soft.fallback_ratio_max_increase // 0.02)
      }
    else empty end,

    if ($snapshots.lexical.overlap.avg_jaccard_at_10 != null and
        $snapshots.lexical.overlap.avg_jaccard_at_10 < ($soft.jaccard_at_10_min // 0.9)) then
      {
        id: "S-LEXICAL-JACCARD",
        message: "lexical avg Jaccard@10 dropped below soft floor",
        observed: $snapshots.lexical.overlap.avg_jaccard_at_10,
        threshold: ($soft.jaccard_at_10_min // 0.9)
      }
    else empty end,

    if ($snapshots.semantic.overlap.avg_jaccard_at_10 != null and
        $snapshots.semantic.overlap.avg_jaccard_at_10 < ($soft.jaccard_at_10_min // 0.9)) then
      {
        id: "S-SEMANTIC-JACCARD",
        message: "semantic avg Jaccard@10 dropped below soft floor",
        observed: $snapshots.semantic.overlap.avg_jaccard_at_10,
        threshold: ($soft.jaccard_at_10_min // 0.9)
      }
    else empty end,

    if (rel_increase($snapshots.semantic.before.avg_fused_candidate_count; $snapshots.semantic.after.avg_fused_candidate_count) != null and
        rel_increase($snapshots.semantic.before.avg_fused_candidate_count; $snapshots.semantic.after.avg_fused_candidate_count) > ($soft.candidate_count_rel_increase_max // 0.2)) then
      {
        id: "S-CANDIDATE-COUNT-DRIFT",
        message: "semantic fused candidate count increased beyond soft threshold",
        observed: rel_increase($snapshots.semantic.before.avg_fused_candidate_count; $snapshots.semantic.after.avg_fused_candidate_count),
        threshold: ($soft.candidate_count_rel_increase_max // 0.2)
      }
    else empty end,

    if ($benchmark_mode_deltas.lexical.latency_ms_delta.p95 != null and
        $benchmark_mode_deltas.lexical.latency_ms_delta.p95 > p95_soft_limit($benchmark_mode_deltas.lexical.before.latency_ms.p95; $soft)) then
      {
        id: "S-BENCH-P95-LEXICAL",
        message: "lexical benchmark p95 drift exceeded soft threshold",
        observed: $benchmark_mode_deltas.lexical.latency_ms_delta.p95,
        threshold: p95_soft_limit($benchmark_mode_deltas.lexical.before.latency_ms.p95; $soft)
      }
    else empty end,

    if ($benchmark_mode_deltas.semantic.latency_ms_delta.p95 != null and
        $benchmark_mode_deltas.semantic.latency_ms_delta.p95 > p95_soft_limit($benchmark_mode_deltas.semantic.before.latency_ms.p95; $soft)) then
      {
        id: "S-BENCH-P95-SEMANTIC",
        message: "semantic benchmark p95 drift exceeded soft threshold",
        observed: $benchmark_mode_deltas.semantic.latency_ms_delta.p95,
        threshold: p95_soft_limit($benchmark_mode_deltas.semantic.before.latency_ms.p95; $soft)
      }
    else empty end,

    if ($benchmark_mode_deltas.hybrid.latency_ms_delta.p95 != null and
        $benchmark_mode_deltas.hybrid.latency_ms_delta.p95 > p95_soft_limit($benchmark_mode_deltas.hybrid.before.latency_ms.p95; $soft)) then
      {
        id: "S-BENCH-P95-HYBRID",
        message: "hybrid benchmark p95 drift exceeded soft threshold",
        observed: $benchmark_mode_deltas.hybrid.latency_ms_delta.p95,
        threshold: p95_soft_limit($benchmark_mode_deltas.hybrid.before.latency_ms.p95; $soft)
      }
    else empty end
  ] as $soft_failures
| {
    manifest_version: 1,
    generated_at: $generated_at,
    run_id: $run_id,
    mode: $mode,
    threshold_schema_version: ($threshold.schema_version // null),
    threshold_file_hash: $threshold_hash,
    quick_stage_b: {
      before: {
        status: ($quick_before_report.status // null),
        summary: ($quick_before_report.summary // null),
        non_passing_checks: $quick_before_non_passing,
        semantic_quality: $quick_semantic_before,
        citation_parity: $quick_citation_before
      },
      after: {
        status: ($quick_after_report.status // null),
        summary: ($quick_after_report.summary // null),
        non_passing_checks: $quick_after_non_passing,
        semantic_quality: $quick_semantic_after,
        citation_parity: $quick_citation_after
      },
      new_failed_checks: $quick_new_failed_checks
    },
    full_stage_b: (
      if $mode == "full" then
        {
          before: {
            status: ($full_before_report.status // null),
            summary: ($full_before_report.summary // null),
            non_passing_checks: $full_before_non_passing
          },
          after: {
            status: ($full_after_report.status // null),
            summary: ($full_after_report.summary // null),
            non_passing_checks: $full_after_non_passing
          },
          new_failed_checks: $full_new_failed_checks
        }
      else null
      end
    ),
    search_snapshots: $snapshots,
    benchmark_quick: {
      before: {
        overall: ($benchmark_before_report.overall // null),
        mode_summaries: ($benchmark_before_report.mode_summaries // [])
      },
      after: {
        overall: ($benchmark_after_report.overall // null),
        mode_summaries: ($benchmark_after_report.mode_summaries // [])
      },
      mode_deltas: $benchmark_mode_deltas
    },
    rule_results: {
      hard_failures: $hard_failures,
      soft_failures: $soft_failures
    },
    gate_status: (
      if ($hard_failures | length) > 0 then "FAIL"
      elif ($soft_failures | length) > 0 then "WARN"
      else "PASS"
      end
    )
  }
