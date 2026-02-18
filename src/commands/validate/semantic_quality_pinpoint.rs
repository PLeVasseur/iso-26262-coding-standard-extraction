const PINPOINT_EVAL_MANIFEST_FILENAME: &str = "pinpoint_eval_queries.json";
const PINPOINT_QUALITY_REPORT_FILENAME: &str = "pinpoint_quality_report.json";
const PINPOINT_EVAL_MANIFEST_SOURCE: &str = "validate-pinpoint-bootstrap-v1";
const PINPOINT_UNIT_LIMIT: usize = 5;
const PINPOINT_TABLE_ROW_LIMIT: usize = 64;

const PINPOINT_TOKEN_STOPWORDS: &[&str] = &[
    "a",
    "an",
    "and",
    "as",
    "at",
    "by",
    "concept",
    "concerning",
    "for",
    "from",
    "guidance",
    "in",
    "into",
    "of",
    "on",
    "or",
    "related",
    "requirement",
    "requirements",
    "table",
    "that",
    "the",
    "to",
    "with",
];

const WP3_PINPOINT_AT1_STAGE_A_MIN: f64 = 0.70;
const WP3_PINPOINT_AT1_STAGE_B_MIN: f64 = 0.82;
const WP3_PINPOINT_TABLE_STAGE_A_MIN: f64 = 0.70;
const WP3_PINPOINT_TABLE_STAGE_B_MIN: f64 = 0.85;
const WP3_PINPOINT_FALLBACK_STAGE_A_MAX: f64 = 0.35;
const WP3_PINPOINT_FALLBACK_STAGE_B_MAX: f64 = 0.20;
const WP3_PINPOINT_DETERMINISM_STAGE_A_MIN: f64 = 0.95;
const WP3_PINPOINT_DETERMINISM_STAGE_B_MIN: f64 = 0.98;
const WP3_PINPOINT_OVERHEAD_STAGE_A_MAX_MS: f64 = 60.0;
const WP3_PINPOINT_OVERHEAD_STAGE_B_MAX_MS: f64 = 40.0;

#[derive(Debug, Clone)]
struct PinpointUnitEval {
    unit_id: String,
    unit_type: String,
    score: f64,
    text_preview: String,
    row_key: Option<String>,
    token_signature: String,
    citation_anchor_compatible: bool,
}

#[derive(Debug, Clone)]
struct PinpointQueryEval {
    top_unit: Option<PinpointUnitEval>,
    fallback_used: bool,
}

fn append_pinpoint_quality_assessment(
    connection: &Connection,
    manifest_dir: &Path,
    run_id: &str,
    stage: Wp2GateStage,
    semantic_embeddings: &SemanticEmbeddingReport,
    semantic_eval_manifest: &SemanticEvalManifest,
    summary: &mut SemanticQualitySummaryReport,
    checks: &mut Vec<QualityCheck>,
    recommendations: &mut Vec<String>,
) -> Result<()> {
    let pinpoint_manifest =
        load_or_bootstrap_pinpoint_eval_manifest(connection, manifest_dir, semantic_eval_manifest)?;
    let quality = compute_pinpoint_quality(
        connection,
        manifest_dir,
        run_id,
        stage,
        semantic_embeddings,
        &pinpoint_manifest,
    )?;

    summary.pinpoint_eval_manifest = quality.summary.source_eval_manifest.clone();
    summary.pinpoint_quality_report_path = quality.summary.quality_report_path.clone();
    summary.pinpoint_total_queries = quality.summary.total_queries;
    summary.pinpoint_table_queries = quality.summary.table_queries;
    summary.pinpoint_high_confidence_queries = quality.summary.high_confidence_queries;
    summary.pinpoint_at_1_relevance = quality.summary.pinpoint_at_1_relevance;
    summary.pinpoint_table_row_accuracy_at_1 = quality.summary.table_row_accuracy_at_1;
    summary.pinpoint_citation_anchor_mismatch_count =
        Some(quality.summary.citation_anchor_mismatch_count as f64);
    summary.pinpoint_fallback_ratio = quality.summary.fallback_ratio;
    summary.pinpoint_determinism_top1 = quality.summary.determinism_top1;
    summary.pinpoint_latency_overhead_p95_ms = quality.summary.latency_overhead_p95_ms;
    summary.warnings.extend(quality.summary.warnings.clone());

    append_pinpoint_checks(stage, &quality.summary, checks, recommendations);
    Ok(())
}

include!("semantic_quality_pinpoint_manifest.rs");
include!("semantic_quality_pinpoint_eval.rs");
include!("semantic_quality_pinpoint_checks.rs");
