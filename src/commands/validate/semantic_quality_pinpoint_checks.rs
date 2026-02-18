use super::*;

pub fn append_pinpoint_checks(
    stage: Wp2GateStage,
    summary: &PinpointQualitySummary,
    checks: &mut Vec<QualityCheck>,
    recommendations: &mut Vec<String>,
) {
    let q039_stage_a_warn = summary
        .pinpoint_at_1_relevance
        .map(|value| value < WP3_PINPOINT_AT1_STAGE_A_MIN)
        .unwrap_or(true);
    let q039_stage_b_fail = summary
        .pinpoint_at_1_relevance
        .map(|value| value < WP3_PINPOINT_AT1_STAGE_B_MIN)
        .unwrap_or(true);
    checks.push(stage_metric_check(
        "Q-039",
        "Pinpoint@1 relevance",
        stage,
        summary.pinpoint_at_1_relevance,
        false,
        q039_stage_b_fail,
    ));

    let q040_stage_a_warn = summary
        .table_row_accuracy_at_1
        .map(|value| value < WP3_PINPOINT_TABLE_STAGE_A_MIN)
        .unwrap_or(true);
    let q040_stage_b_fail = summary
        .table_row_accuracy_at_1
        .map(|value| value < WP3_PINPOINT_TABLE_STAGE_B_MIN)
        .unwrap_or(true);
    checks.push(stage_metric_check(
        "Q-040",
        "Table pinpoint row accuracy@1",
        stage,
        summary.table_row_accuracy_at_1,
        false,
        q040_stage_b_fail,
    ));

    let q041_stage_a_warn = summary.citation_anchor_mismatch_count > 0;
    let q041_stage_b_fail = summary.citation_anchor_mismatch_count > 0;
    checks.push(stage_metric_check(
        "Q-041",
        "Pinpoint citation-anchor consistency",
        stage,
        Some(summary.citation_anchor_mismatch_count as f64),
        false,
        q041_stage_b_fail,
    ));

    let q042_stage_a_warn = summary
        .fallback_ratio
        .map(|value| value > WP3_PINPOINT_FALLBACK_STAGE_A_MAX)
        .unwrap_or(true);
    let q042_stage_b_fail = summary
        .fallback_ratio
        .map(|value| value > WP3_PINPOINT_FALLBACK_STAGE_B_MAX)
        .unwrap_or(true);
    checks.push(stage_metric_check(
        "Q-042",
        "Pinpoint fallback ratio",
        stage,
        summary.fallback_ratio,
        false,
        q042_stage_b_fail,
    ));

    let q043_stage_a_warn = summary
        .determinism_top1
        .map(|value| value < WP3_PINPOINT_DETERMINISM_STAGE_A_MIN)
        .unwrap_or(true);
    let q043_stage_b_fail = summary
        .determinism_top1
        .map(|value| value < WP3_PINPOINT_DETERMINISM_STAGE_B_MIN)
        .unwrap_or(true);
    checks.push(stage_metric_check(
        "Q-043",
        "Pinpoint determinism",
        stage,
        summary.determinism_top1,
        false,
        q043_stage_b_fail,
    ));

    let q044_stage_a_warn = summary
        .latency_overhead_p95_ms
        .map(|value| value > WP3_PINPOINT_OVERHEAD_STAGE_A_MAX_MS)
        .unwrap_or(true);
    let q044_stage_b_fail = summary
        .latency_overhead_p95_ms
        .map(|value| value > WP3_PINPOINT_OVERHEAD_STAGE_B_MAX_MS)
        .unwrap_or(true);
    checks.push(stage_metric_check(
        "Q-044",
        "Pinpoint latency overhead",
        stage,
        summary.latency_overhead_p95_ms,
        false,
        q044_stage_b_fail,
    ));

    if stage == Wp2GateStage::A {
        if q039_stage_a_warn {
            recommendations.push(
                "Q-039 Stage A warning: pinpoint top-1 relevance is below target; improve pinpoint unit extraction and token matching.".to_string(),
            );
        }
        if q040_stage_a_warn {
            recommendations.push(
                "Q-040 Stage A warning: table pinpoint row accuracy is below target; improve table row mapping and row-key labels.".to_string(),
            );
        }
        if q041_stage_a_warn {
            recommendations.push(
                "Q-041 Stage A warning: pinpoint units include anchor-incompatible selections; verify row/cell to chunk citation compatibility.".to_string(),
            );
        }
        if q042_stage_a_warn {
            recommendations.push(
                "Q-042 Stage A warning: pinpoint fallback ratio is high on high-confidence queries; improve unit candidate extraction coverage.".to_string(),
            );
        }
        if q043_stage_a_warn {
            recommendations.push(
                "Q-043 Stage A warning: pinpoint top-unit determinism is below target; enforce deterministic unit ordering and scoring ties.".to_string(),
            );
        }
        if q044_stage_a_warn {
            recommendations.push(
                "Q-044 Stage A warning: pinpoint latency overhead exceeds budget; cap unit candidates or optimize scoring path.".to_string(),
            );
        }
    }

    if stage == Wp2GateStage::B {
        if q039_stage_b_fail {
            recommendations.push(
                "Raise pinpoint@1 relevance (Q-039) before promoting pinpoint precision to Stage B hard gate.".to_string(),
            );
        }
        if q040_stage_b_fail {
            recommendations.push(
                "Improve table pinpoint row accuracy (Q-040) before Stage B pinpoint promotion."
                    .to_string(),
            );
        }
        if q041_stage_b_fail {
            recommendations.push(
                "Resolve pinpoint anchor-consistency mismatches (Q-041) before Stage B promotion."
                    .to_string(),
            );
        }
        if q042_stage_b_fail {
            recommendations.push(
                "Reduce pinpoint fallback ratio (Q-042) for high-confidence queries before Stage B promotion.".to_string(),
            );
        }
        if q043_stage_b_fail {
            recommendations.push(
                "Stabilize pinpoint top-unit determinism (Q-043) before Stage B promotion."
                    .to_string(),
            );
        }
        if q044_stage_b_fail {
            recommendations.push(
                "Reduce pinpoint latency overhead (Q-044) below Stage B threshold before promotion.".to_string(),
            );
        }
    }
}
