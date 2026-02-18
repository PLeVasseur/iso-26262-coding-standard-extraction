pub const PINPOINT_EVAL_MANIFEST_FILENAME: &str = "pinpoint_eval_queries.json";
pub const PINPOINT_QUALITY_REPORT_FILENAME: &str = "pinpoint_quality_report.json";
pub const PINPOINT_EVAL_MANIFEST_SOURCE: &str = "validate-pinpoint-bootstrap-v1";
pub const PINPOINT_UNIT_LIMIT: usize = 5;
pub const PINPOINT_TABLE_ROW_LIMIT: usize = 64;

pub const PINPOINT_TOKEN_STOPWORDS: &[&str] = &[
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

pub const WP3_PINPOINT_AT1_STAGE_A_MIN: f64 = 0.70;
pub const WP3_PINPOINT_AT1_STAGE_B_MIN: f64 = 0.82;
pub const WP3_PINPOINT_TABLE_STAGE_A_MIN: f64 = 0.70;
pub const WP3_PINPOINT_TABLE_STAGE_B_MIN: f64 = 0.85;
pub const WP3_PINPOINT_FALLBACK_STAGE_A_MAX: f64 = 0.35;
pub const WP3_PINPOINT_FALLBACK_STAGE_B_MAX: f64 = 0.20;
pub const WP3_PINPOINT_DETERMINISM_STAGE_A_MIN: f64 = 0.95;
pub const WP3_PINPOINT_DETERMINISM_STAGE_B_MIN: f64 = 0.98;
pub const WP3_PINPOINT_OVERHEAD_STAGE_A_MAX_MS: f64 = 60.0;
pub const WP3_PINPOINT_OVERHEAD_STAGE_B_MAX_MS: f64 = 40.0;

#[derive(Debug, Clone)]
pub struct PinpointUnitEval {
    pub unit_id: String,
    pub unit_type: String,
    pub score: f64,
    pub text_preview: String,
    pub row_key: Option<String>,
    pub token_signature: String,
    pub citation_anchor_compatible: bool,
}

#[derive(Debug, Clone)]
pub struct PinpointQueryEval {
    pub top_unit: Option<PinpointUnitEval>,
    pub fallback_used: bool,
}

pub fn append_pinpoint_quality_assessment(
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

#[path = "semantic_quality_pinpoint_checks.rs"]
mod semantic_quality_pinpoint_checks;
#[path = "semantic_quality_pinpoint_eval.rs"]
mod semantic_quality_pinpoint_eval;
#[path = "semantic_quality_pinpoint_manifest.rs"]
mod semantic_quality_pinpoint_manifest;

pub use self::semantic_quality_pinpoint_checks::*;
pub use self::semantic_quality_pinpoint_eval::*;
pub use self::semantic_quality_pinpoint_manifest::*;
use super::*;
