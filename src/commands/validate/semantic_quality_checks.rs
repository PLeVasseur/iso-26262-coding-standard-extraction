const SEMANTIC_EVAL_MANIFEST_FILENAME: &str = "semantic_eval_queries.json";
const SEMANTIC_QUALITY_REPORT_FILENAME: &str = "semantic_quality_report.json";
const SEMANTIC_EVAL_MANIFEST_SOURCE: &str = "validate-bootstrap-v1";
const SEMANTIC_TOP_K: usize = 10;
const SEMANTIC_RETRIEVAL_LIMIT: usize = 50;
const SEMANTIC_RRF_K: f64 = 60.0;

const WP3_EMBEDDING_COVERAGE_STAGE_A_MIN: f64 = 0.98;
const WP3_EMBEDDING_COVERAGE_STAGE_B_MIN: f64 = 0.995;
const WP3_STALE_EMBEDDING_STAGE_A_MAX: f64 = 0.02;
const WP3_STALE_EMBEDDING_STAGE_B_MAX: f64 = 0.0;
const WP3_SEMANTIC_NDCG_STAGE_A_MIN: f64 = 0.60;
const WP3_SEMANTIC_NDCG_STAGE_B_MIN: f64 = 0.72;
const WP3_HYBRID_NDCG_UPLIFT_STAGE_A_MIN: f64 = 0.03;
const WP3_HYBRID_NDCG_UPLIFT_STAGE_B_MIN: f64 = 0.06;
const WP3_EXACT_TOP1_MIN: f64 = 1.0;
const WP3_CITATION_PARITY_MIN: f64 = 1.0;
const WP3_LATENCY_RATIO_MAX: f64 = 2.5;
const WP3_HYBRID_P95_MAX_MS: f64 = 500.0;
const WP3_DETERMINISM_STAGE_A_MIN: f64 = 0.95;
const WP3_DETERMINISM_STAGE_B_MIN: f64 = 0.98;
const WP3_HYBRID_MRR_STAGE_A_MIN: f64 = 0.80;
const WP3_HYBRID_MRR_STAGE_B_MIN: f64 = 0.90;
const WP3_RECALL_DROP_STAGE_A_MAX: f64 = 0.02;
const WP3_RECALL_DROP_STAGE_B_MAX: f64 = 0.0;
const WP3_JUDGED_STAGE_A_MIN: f64 = 0.70;
const WP3_JUDGED_STAGE_B_MIN: f64 = 0.85;
const WP3_NDCG_UPLIFT_P_STAGE_A_MAX: f64 = 0.10;
const WP3_NDCG_UPLIFT_P_STAGE_B_MAX: f64 = 0.05;

#[derive(Debug, Default)]
struct SemanticQualityAssessment {
    checks: Vec<QualityCheck>,
    summary: SemanticQualitySummaryReport,
    recommendations: Vec<String>,
}

#[derive(Debug, Clone)]
struct SemanticRetrievedHit {
    chunk_id: String,
    reference: String,
    page_pdf_start: Option<i64>,
    page_pdf_end: Option<i64>,
    citation_anchor_id: Option<String>,
    score: f64,
}

#[derive(Debug)]
struct SemanticEvalComputation {
    summary: SemanticQualitySummaryReport,
}

#[derive(Debug, Clone)]
struct QueryEvalRecord {
    lexical_hits: Vec<SemanticRetrievedHit>,
    semantic_hits: Vec<SemanticRetrievedHit>,
    hybrid_hits: Vec<SemanticRetrievedHit>,
    lexical_ndcg: Option<f64>,
    semantic_ndcg: Option<f64>,
    hybrid_ndcg: Option<f64>,
    hybrid_rr_at_10: Option<f64>,
    lexical_recall_at_50: Option<f64>,
    hybrid_recall_at_50: Option<f64>,
    judged_at_10: Option<f64>,
    lexical_latency_ms: f64,
    semantic_latency_ms: f64,
    hybrid_latency_ms: f64,
    exact_top1_hit_hybrid: Option<bool>,
    citation_top1_match_lexical_vs_hybrid: Option<bool>,
    determinism_top10_overlap: Option<f64>,
}

fn build_semantic_quality_assessment(
    connection: &Connection,
    manifest_dir: &Path,
    run_id: &str,
    refs: &[GoldReference],
    stage: Wp2GateStage,
    semantic_metrics: &SemanticEmbeddingMetrics,
    semantic_embeddings: &mut SemanticEmbeddingReport,
) -> Result<SemanticQualityAssessment> {
    let mut assessment = SemanticQualityAssessment::default();

    append_semantic_embedding_checks(
        stage,
        semantic_metrics,
        semantic_embeddings,
        &mut assessment.checks,
        &mut assessment.recommendations,
    );

    let eval_manifest = load_or_bootstrap_semantic_eval_manifest(connection, manifest_dir, refs)?;
    let quality = compute_semantic_eval_quality(
        connection,
        manifest_dir,
        run_id,
        stage,
        refs,
        semantic_embeddings,
        &eval_manifest,
    )?;
    append_semantic_retrieval_checks(
        stage,
        &quality.summary,
        &mut assessment.checks,
        &mut assessment.recommendations,
    );

    assessment.summary = quality.summary;
    Ok(assessment)
}

fn append_semantic_embedding_checks(
    stage: Wp2GateStage,
    semantic_metrics: &SemanticEmbeddingMetrics,
    semantic_embeddings: &mut SemanticEmbeddingReport,
    checks: &mut Vec<QualityCheck>,
    recommendations: &mut Vec<String>,
) {
    let q031_hard_fail = semantic_metrics.eligible_chunks > 0
        && semantic_embeddings
            .chunk_embedding_coverage_ratio
            .map(|ratio| ratio < WP3_EMBEDDING_COVERAGE_STAGE_A_MIN)
            .unwrap_or(true);
    let q031_stage_b_fail = semantic_metrics.eligible_chunks > 0
        && semantic_embeddings
            .chunk_embedding_coverage_ratio
            .map(|ratio| ratio < WP3_EMBEDDING_COVERAGE_STAGE_B_MIN)
            .unwrap_or(true);
    checks.push(QualityCheck {
        check_id: "Q-031".to_string(),
        name: "Chunk embedding coverage ratio".to_string(),
        result: if semantic_metrics.eligible_chunks == 0 {
            "pending".to_string()
        } else {
            wp2_result(stage, q031_hard_fail, q031_stage_b_fail).to_string()
        },
    });

    let q032_hard_fail = semantic_metrics.eligible_chunks > 0
        && semantic_embeddings
            .stale_embedding_ratio
            .map(|ratio| ratio > WP3_STALE_EMBEDDING_STAGE_A_MAX)
            .unwrap_or(true);
    let q032_stage_b_fail = semantic_metrics.eligible_chunks > 0
        && semantic_embeddings
            .stale_embedding_ratio
            .map(|ratio| ratio > WP3_STALE_EMBEDDING_STAGE_B_MAX)
            .unwrap_or(true);
    checks.push(QualityCheck {
        check_id: "Q-032".to_string(),
        name: "Stale embedding ratio".to_string(),
        result: if semantic_metrics.eligible_chunks == 0 {
            "pending".to_string()
        } else {
            wp2_result(stage, q032_hard_fail, q032_stage_b_fail).to_string()
        },
    });

    if stage == Wp2GateStage::A {
        if semantic_metrics.eligible_chunks == 0 {
            semantic_embeddings
                .warnings
                .push("Q-031/Q-032 Stage A warning: no eligible chunks found for semantic embedding checks.".to_string());
        } else {
            if q031_stage_b_fail {
                semantic_embeddings.warnings.push(
                    "Q-031 Stage A warning: chunk embedding coverage is below Stage B targets."
                        .to_string(),
                );
            }
            if q032_stage_b_fail {
                semantic_embeddings.warnings.push(
                    "Q-032 Stage A warning: stale embedding ratio is above Stage B targets."
                        .to_string(),
                );
            }
        }
    }

    if semantic_metrics.eligible_chunks > 0 {
        if q031_hard_fail || (stage == Wp2GateStage::B && q031_stage_b_fail) {
            recommendations.push(
                "Run `embed --refresh-mode missing-or-stale` for the active semantic model and verify chunk eligibility filters include clause/annex/table types.".to_string(),
            );
        }
        if q032_hard_fail || (stage == Wp2GateStage::B && q032_stage_b_fail) {
            recommendations.push(
                "Refresh stale embeddings for the active semantic model and ensure embedding payload hashing/dimensions are aligned with current model config.".to_string(),
            );
        }
    }
}

fn append_semantic_retrieval_checks(
    stage: Wp2GateStage,
    summary: &SemanticQualitySummaryReport,
    checks: &mut Vec<QualityCheck>,
    recommendations: &mut Vec<String>,
) {
    let q033_stage_a_warn = summary
        .semantic_ndcg_at_10
        .map(|value| value < WP3_SEMANTIC_NDCG_STAGE_A_MIN)
        .unwrap_or(true);
    let q033_stage_b_fail = summary
        .semantic_ndcg_at_10
        .map(|value| value < WP3_SEMANTIC_NDCG_STAGE_B_MIN)
        .unwrap_or(true);
    checks.push(stage_metric_check(
        "Q-033",
        "Semantic nDCG@10 on non-exact intent set",
        stage,
        summary.semantic_ndcg_at_10,
        false,
        q033_stage_b_fail,
    ));
    let q034_stage_a_warn = summary
        .hybrid_ndcg_uplift_vs_lexical
        .map(|value| value < WP3_HYBRID_NDCG_UPLIFT_STAGE_A_MIN)
        .unwrap_or(true);
    let q034_stage_b_fail = summary
        .hybrid_ndcg_uplift_vs_lexical
        .map(|value| value < WP3_HYBRID_NDCG_UPLIFT_STAGE_B_MIN)
        .unwrap_or(true);
    checks.push(stage_metric_check(
        "Q-034",
        "Hybrid nDCG@10 uplift vs lexical baseline",
        stage,
        summary.hybrid_ndcg_uplift_vs_lexical,
        false,
        q034_stage_b_fail,
    ));
    let q035_stage_a_warn = summary
        .exact_ref_top1_hit_rate
        .map(|value| value < WP3_EXACT_TOP1_MIN)
        .unwrap_or(true);
    let q035_stage_b_fail = summary
        .exact_ref_top1_hit_rate
        .map(|value| value < WP3_EXACT_TOP1_MIN)
        .unwrap_or(true);
    checks.push(stage_metric_check(
        "Q-035",
        "Exact reference Top-1 hit rate in hybrid",
        stage,
        summary.exact_ref_top1_hit_rate,
        false,
        q035_stage_b_fail,
    ));
    let q036_stage_a_warn = summary
        .citation_parity_top1
        .map(|value| value < WP3_CITATION_PARITY_MIN)
        .unwrap_or(true);
    let q036_stage_b_fail = summary
        .citation_parity_top1
        .map(|value| value < WP3_CITATION_PARITY_MIN)
        .unwrap_or(true);
    checks.push(stage_metric_check(
        "Q-036",
        "Citation parity for hybrid top-1 results",
        stage,
        summary.citation_parity_top1,
        false,
        q036_stage_b_fail,
    ));
    let q037_stage_a_warn = summary
        .latency_ratio_vs_lexical
        .map(|value| value > WP3_LATENCY_RATIO_MAX)
        .unwrap_or(true);
    let q037_stage_b_fail = summary
        .latency_ratio_vs_lexical
        .map(|value| value > WP3_LATENCY_RATIO_MAX)
        .unwrap_or(true)
        || summary
            .hybrid_p95_latency_ms
            .map(|value| value > WP3_HYBRID_P95_MAX_MS)
            .unwrap_or(true);
    checks.push(stage_metric_check(
        "Q-037",
        "Hybrid p95 latency budget",
        stage,
        summary.hybrid_p95_latency_ms,
        false,
        q037_stage_b_fail,
    ));
    let q038_stage_a_warn = summary
        .retrieval_determinism_topk_overlap
        .map(|value| value < WP3_DETERMINISM_STAGE_A_MIN)
        .unwrap_or(true);
    let q038_stage_b_fail = summary
        .retrieval_determinism_topk_overlap
        .map(|value| value < WP3_DETERMINISM_STAGE_B_MIN)
        .unwrap_or(true);
    checks.push(stage_metric_check(
        "Q-038",
        "Retrieval determinism (top-k overlap)",
        stage,
        summary.retrieval_determinism_topk_overlap,
        false,
        q038_stage_b_fail,
    ));
    let q045_stage_a_warn = summary
        .hybrid_mrr_at_10_first_hit
        .map(|value| value < WP3_HYBRID_MRR_STAGE_A_MIN)
        .unwrap_or(true);
    let q045_stage_b_fail = summary
        .hybrid_mrr_at_10_first_hit
        .map(|value| value < WP3_HYBRID_MRR_STAGE_B_MIN)
        .unwrap_or(true);
    checks.push(stage_metric_check(
        "Q-045",
        "Hybrid MRR@10 on first-hit intents",
        stage,
        summary.hybrid_mrr_at_10_first_hit,
        false,
        q045_stage_b_fail,
    ));
    let q046_stage_a_warn = summary
        .hybrid_recall_at_50_delta_vs_lexical
        .map(|value| value < -WP3_RECALL_DROP_STAGE_A_MAX)
        .unwrap_or(true);
    let q046_stage_b_fail = summary
        .hybrid_recall_at_50_delta_vs_lexical
        .map(|value| value < -WP3_RECALL_DROP_STAGE_B_MAX)
        .unwrap_or(true);
    checks.push(stage_metric_check(
        "Q-046",
        "Hybrid Recall@50 non-regression vs lexical",
        stage,
        summary.hybrid_recall_at_50_delta_vs_lexical,
        false,
        q046_stage_b_fail,
    ));
    let q047_stage_a_warn = summary
        .judged_at_10_label_completeness
        .map(|value| value < WP3_JUDGED_STAGE_A_MIN)
        .unwrap_or(true);
    let q047_stage_b_fail = summary
        .judged_at_10_label_completeness
        .map(|value| value < WP3_JUDGED_STAGE_B_MIN)
        .unwrap_or(true);
    checks.push(stage_metric_check(
        "Q-047",
        "Judged@10 label completeness",
        stage,
        summary.judged_at_10_label_completeness,
        false,
        q047_stage_b_fail,
    ));
    let ci_includes_zero = match (
        summary.ndcg_uplift_bootstrap_ci_low,
        summary.ndcg_uplift_bootstrap_ci_high,
    ) {
        (Some(low), Some(high)) => low <= 0.0 && high >= 0.0,
        _ => true,
    };
    let q048_stage_a_warn = summary
        .ndcg_uplift_p_value
        .map(|p_value| p_value >= WP3_NDCG_UPLIFT_P_STAGE_A_MAX)
        .unwrap_or(true)
        || ci_includes_zero;
    let q048_stage_b_fail = summary
        .ndcg_uplift_p_value
        .map(|p_value| p_value >= WP3_NDCG_UPLIFT_P_STAGE_B_MAX)
        .unwrap_or(true)
        || ci_includes_zero;
    checks.push(stage_metric_check(
        "Q-048",
        "nDCG uplift significance + effect-size confidence",
        stage,
        summary.ndcg_uplift_p_value,
        false,
        q048_stage_b_fail,
    ));
    if stage == Wp2GateStage::A {
        if q033_stage_a_warn {
            recommendations.push(
                "Q-033 Stage A warning: semantic nDCG is below the Stage A target; improve semantic query coverage and/or embedding quality.".to_string(),
            );
        }
        if q034_stage_a_warn {
            recommendations.push(
                "Q-034 Stage A warning: hybrid uplift over lexical is below the Stage A target; tune lexical/semantic candidate pools and fusion.".to_string(),
            );
        }
        if q035_stage_a_warn {
            recommendations.push(
                "Q-035 Stage A warning: exact-intent top-1 is below target; ensure exact-intent routing keeps lexical priority in hybrid mode.".to_string(),
            );
        }
        if q036_stage_a_warn {
            recommendations.push(
                "Q-036 Stage A warning: citation parity dropped for hybrid top-1 results; verify reference/anchor tie-break behavior.".to_string(),
            );
        }
        if q037_stage_a_warn {
            recommendations.push(
                "Q-037 Stage A warning: hybrid latency ratio exceeds Stage A budget; reduce candidate sizes or optimize semantic scoring path.".to_string(),
            );
        }
        if q038_stage_a_warn {
            recommendations.push(
                "Q-038 Stage A warning: repeated hybrid runs are not stable enough; review ranking tie-breakers and deterministic ordering.".to_string(),
            );
        }
        if q045_stage_a_warn {
            recommendations.push(
                "Q-045 Stage A warning: first-hit intent MRR is below target; prioritize exact/keyword/table-intent ranking in hybrid fusion.".to_string(),
            );
        }
        if q046_stage_a_warn {
            recommendations.push(
                "Q-046 Stage A warning: hybrid recall@50 regressed vs lexical baseline; widen semantic candidate breadth or adjust fusion weights.".to_string(),
            );
        }
        if q047_stage_a_warn {
            recommendations.push(
                "Q-047 Stage A warning: judged coverage is insufficient for trustworthy metrics; expand judged_chunk_ids labels in semantic_eval_queries.json.".to_string(),
            );
        }
        if q048_stage_a_warn {
            recommendations.push(
                "Q-048 Stage A warning: nDCG uplift confidence is weak (p-value/CI); increase labeled query depth before promotion decisions.".to_string(),
            );
        }
    }
    if stage == Wp2GateStage::B {
        if q033_stage_b_fail {
            recommendations.push(
                "Improve semantic relevance quality until Q-033 meets Stage B threshold before promoting semantic defaults.".to_string(),
            );
        }
        if q034_stage_b_fail {
            recommendations.push(
                "Tune hybrid fusion/candidate parameters so hybrid nDCG uplift (Q-034) clears Stage B threshold.".to_string(),
            );
        }
        if q035_stage_b_fail {
            recommendations.push(
                "Restore exact-reference top-1 perfection in hybrid mode (Q-035) before Stage B promotion.".to_string(),
            );
        }
        if q036_stage_b_fail {
            recommendations.push(
                "Resolve citation parity regressions in hybrid mode (Q-036) and re-run baseline verification.".to_string(),
            );
        }
        if q037_stage_b_fail {
            recommendations.push(
                "Reduce hybrid latency (Q-037) below both relative and absolute Stage B budgets."
                    .to_string(),
            );
        }
        if q038_stage_b_fail {
            recommendations.push(
                "Stabilize hybrid retrieval determinism (Q-038) by enforcing deterministic candidate and tie-break ordering.".to_string(),
            );
        }
        if q045_stage_b_fail {
            recommendations.push(
                "Raise first-hit intent quality so Q-045 MRR@10 clears Stage B threshold before semantic promotion.".to_string(),
            );
        }
        if q046_stage_b_fail {
            recommendations.push(
                "Eliminate hybrid recall regressions (Q-046) relative to lexical baseline before Stage B promotion.".to_string(),
            );
        }
        if q047_stage_b_fail {
            recommendations.push(
                "Increase judged label completeness (Q-047) to Stage B minimum by expanding query-level judged_chunk_ids.".to_string(),
            );
        }
        if q048_stage_b_fail {
            recommendations.push(
                "Improve statistical confidence for nDCG uplift (Q-048) so p-value and CI satisfy Stage B thresholds.".to_string(),
            );
        }
    }
}

fn stage_metric_check(
    check_id: &str,
    name: &str,
    stage: Wp2GateStage,
    metric: Option<f64>,
    hard_fail: bool,
    stage_b_fail: bool,
) -> QualityCheck {
    let result = if metric.is_none() {
        "pending".to_string()
    } else {
        wp2_result(stage, hard_fail, stage_b_fail).to_string()
    };

    QualityCheck {
        check_id: check_id.to_string(),
        name: name.to_string(),
        result,
    }
}
