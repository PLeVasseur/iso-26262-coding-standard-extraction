use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CitationParityBaseline {
    pub manifest_version: u32,
    pub run_id: String,
    pub generated_at: String,
    pub db_schema_version: Option<String>,
    #[serde(default)]
    pub decision_id: Option<String>,
    #[serde(default)]
    pub change_reason: Option<String>,
    pub target_linked_count: usize,
    pub query_options: String,
    pub checksum: String,
    pub entries: Vec<CitationParityEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CitationParityArtifact {
    pub manifest_version: u32,
    pub run_id: String,
    pub generated_at: String,
    pub baseline_path: String,
    pub baseline_mode: String,
    pub baseline_checksum: Option<String>,
    pub baseline_missing: bool,
    pub target_linked_count: usize,
    pub comparable_count: usize,
    pub top1_parity: Option<f64>,
    pub top3_containment: Option<f64>,
    pub page_range_parity: Option<f64>,
    pub baseline_created: bool,
    pub entries: Vec<CitationParityComparisonEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CitationParityComparisonEntry {
    pub target_id: String,
    pub top1_match: bool,
    pub top3_contains_baseline: bool,
    pub page_range_match: bool,
}

#[derive(Debug)]
pub struct Wp2Assessment {
    pub checks: Vec<QualityCheck>,
    pub extraction_fidelity: ExtractionFidelityReport,
    pub hierarchy_semantics: HierarchySemanticsReport,
    pub table_semantics: TableSemanticsReport,
    pub citation_parity: CitationParitySummaryReport,
    pub semantic_embeddings: SemanticEmbeddingReport,
    pub semantic_quality: SemanticQualitySummaryReport,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct SemanticEmbeddingReport {
    pub active_model_id: String,
    pub embedding_dim: Option<usize>,
    pub eligible_chunks: usize,
    pub embedded_chunks: usize,
    pub stale_rows: usize,
    pub embedding_rows_for_active_model: usize,
    pub chunk_embedding_coverage_ratio: Option<f64>,
    pub stale_embedding_ratio: Option<f64>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct SemanticQualitySummaryReport {
    pub source_eval_manifest: Option<String>,
    pub quality_report_path: Option<String>,
    pub active_model_id: Option<String>,
    pub total_queries: usize,
    pub non_exact_queries: usize,
    pub exact_queries: usize,
    pub semantic_ndcg_at_10: Option<f64>,
    pub lexical_ndcg_at_10: Option<f64>,
    pub hybrid_ndcg_at_10: Option<f64>,
    pub hybrid_ndcg_uplift_vs_lexical: Option<f64>,
    pub exact_ref_top1_hit_rate: Option<f64>,
    pub citation_parity_top1: Option<f64>,
    pub lexical_p95_latency_ms: Option<f64>,
    pub hybrid_p95_latency_ms: Option<f64>,
    pub latency_ratio_vs_lexical: Option<f64>,
    pub retrieval_determinism_topk_overlap: Option<f64>,
    pub hybrid_mrr_at_10_first_hit: Option<f64>,
    pub lexical_recall_at_50: Option<f64>,
    pub hybrid_recall_at_50: Option<f64>,
    pub hybrid_recall_at_50_delta_vs_lexical: Option<f64>,
    pub judged_at_10_label_completeness: Option<f64>,
    pub ndcg_uplift_p_value: Option<f64>,
    pub ndcg_uplift_bootstrap_ci_low: Option<f64>,
    pub ndcg_uplift_bootstrap_ci_high: Option<f64>,
    pub pinpoint_eval_manifest: Option<String>,
    pub pinpoint_quality_report_path: Option<String>,
    pub pinpoint_total_queries: usize,
    pub pinpoint_table_queries: usize,
    pub pinpoint_high_confidence_queries: usize,
    pub pinpoint_at_1_relevance: Option<f64>,
    pub pinpoint_table_row_accuracy_at_1: Option<f64>,
    pub pinpoint_citation_anchor_mismatch_count: Option<f64>,
    pub pinpoint_fallback_ratio: Option<f64>,
    pub pinpoint_determinism_top1: Option<f64>,
    pub pinpoint_latency_overhead_p95_ms: Option<f64>,
    pub baseline_path: String,
    pub baseline_mode: String,
    pub baseline_run_id: Option<String>,
    pub baseline_checksum: Option<String>,
    pub baseline_created: bool,
    pub baseline_missing: bool,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SemanticBaselineMode {
    Verify,
    Bootstrap,
}

impl SemanticBaselineMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Verify => "verify",
            Self::Bootstrap => "bootstrap",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticEvalManifest {
    pub manifest_version: u32,
    pub generated_at: String,
    pub source: String,
    pub queries: Vec<SemanticEvalQuery>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticEvalQuery {
    pub query_id: String,
    pub query_text: String,
    pub intent: String,
    pub expected_chunk_ids: Vec<String>,
    #[serde(default)]
    pub judged_chunk_ids: Vec<String>,
    #[serde(default)]
    pub expected_refs: Vec<String>,
    pub must_hit_top1: bool,
    #[serde(default)]
    pub part_filter: Option<u32>,
    #[serde(default)]
    pub chunk_type_filter: Option<String>,
    #[serde(default)]
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SemanticQualityArtifact {
    pub manifest_version: u32,
    pub run_id: String,
    pub generated_at: String,
    pub source_eval_manifest: String,
    pub active_model_id: Option<String>,
    pub summary: SemanticQualitySummaryReport,
    pub query_results: Vec<SemanticQualityQueryResult>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SemanticQualityQueryResult {
    pub query_id: String,
    pub intent: String,
    pub query_text: String,
    pub expected_chunk_ids: Vec<String>,
    pub lexical_top_chunk_ids: Vec<String>,
    pub semantic_top_chunk_ids: Vec<String>,
    pub hybrid_top_chunk_ids: Vec<String>,
    pub lexical_ndcg_at_10: Option<f64>,
    pub semantic_ndcg_at_10: Option<f64>,
    pub hybrid_ndcg_at_10: Option<f64>,
    pub hybrid_rr_at_10: Option<f64>,
    pub lexical_recall_at_50: Option<f64>,
    pub hybrid_recall_at_50: Option<f64>,
    pub judged_at_10: Option<f64>,
    pub lexical_latency_ms: f64,
    pub semantic_latency_ms: f64,
    pub hybrid_latency_ms: f64,
    pub exact_top1_hit_hybrid: Option<bool>,
    pub citation_top1_match_lexical_vs_hybrid: Option<bool>,
    pub determinism_top10_overlap: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PinpointEvalManifest {
    pub manifest_version: u32,
    pub generated_at: String,
    pub source: String,
    pub queries: Vec<PinpointEvalQuery>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PinpointEvalQuery {
    pub query_id: String,
    pub query_text: String,
    pub parent_expected_chunk_ids: Vec<String>,
    #[serde(default)]
    pub expected_unit_ids: Vec<String>,
    #[serde(default)]
    pub expected_token_sets: Vec<Vec<String>>,
    #[serde(default)]
    pub expected_row_keys: Vec<String>,
    pub high_confidence: bool,
    pub intent: String,
    #[serde(default)]
    pub part_filter: Option<u32>,
    #[serde(default)]
    pub chunk_type_filter: Option<String>,
    #[serde(default)]
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PinpointQualityArtifact {
    pub manifest_version: u32,
    pub run_id: String,
    pub generated_at: String,
    pub source_eval_manifest: String,
    pub summary: PinpointQualitySummary,
    pub query_results: Vec<PinpointQualityQueryResult>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct PinpointQualitySummary {
    pub source_eval_manifest: Option<String>,
    pub quality_report_path: Option<String>,
    pub total_queries: usize,
    pub table_queries: usize,
    pub high_confidence_queries: usize,
    pub pinpoint_at_1_relevance: Option<f64>,
    pub table_row_accuracy_at_1: Option<f64>,
    pub citation_anchor_mismatch_count: usize,
    pub fallback_ratio: Option<f64>,
    pub determinism_top1: Option<f64>,
    pub latency_overhead_p95_ms: Option<f64>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PinpointQualityQueryResult {
    pub query_id: String,
    pub intent: String,
    pub query_text: String,
    pub parent_chunk_id: Option<String>,
    pub top_unit_id: Option<String>,
    pub top_unit_type: Option<String>,
    pub top_unit_text: Option<String>,
    pub top_row_key: Option<String>,
    pub top_unit_score: Option<f64>,
    pub relevance_hit_at_1: Option<bool>,
    pub row_accuracy_hit_at_1: Option<bool>,
    pub citation_anchor_compatible: Option<bool>,
    pub fallback_used: bool,
    pub determinism_top1_match: Option<bool>,
    pub latency_without_pinpoint_ms: f64,
    pub latency_with_pinpoint_ms: f64,
    pub latency_overhead_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticRetrievalBaseline {
    pub manifest_version: u32,
    pub run_id: String,
    pub generated_at: String,
    pub db_schema_version: Option<String>,
    #[serde(default)]
    pub decision_id: Option<String>,
    #[serde(default)]
    pub change_reason: Option<String>,
    pub check_ids: Vec<String>,
    pub query_ids: Vec<String>,
    pub thresholds: SemanticRetrievalBaselineThresholds,
    pub summary_metrics: SemanticRetrievalBaselineMetrics,
    pub checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SemanticRetrievalBaselineThresholds {
    pub q031_stage_a_min: f64,
    pub q031_stage_b_min: f64,
    pub q032_stage_a_max: f64,
    pub q032_stage_b_max: f64,
    pub q033_stage_a_min: f64,
    pub q033_stage_b_min: f64,
    pub q034_stage_a_min: f64,
    pub q034_stage_b_min: f64,
    pub q035_stage_a_min: f64,
    pub q035_stage_b_min: f64,
    pub q036_stage_a_min: f64,
    pub q036_stage_b_min: f64,
    pub q037_latency_ratio_stage_a_max: f64,
    pub q037_latency_ratio_stage_b_max: f64,
    pub q037_hybrid_p95_stage_b_max_ms: f64,
    pub q038_stage_a_min: f64,
    pub q038_stage_b_min: f64,
    pub q045_stage_a_min: f64,
    pub q045_stage_b_min: f64,
    pub q046_stage_a_max_drop: f64,
    pub q046_stage_b_max_drop: f64,
    pub q047_stage_a_min: f64,
    pub q047_stage_b_min: f64,
    pub q048_stage_a_p_max: f64,
    pub q048_stage_b_p_max: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticRetrievalBaselineMetrics {
    pub q031_chunk_embedding_coverage_ratio: Option<f64>,
    pub q032_stale_embedding_ratio: Option<f64>,
    pub q033_semantic_ndcg_at_10: Option<f64>,
    pub q034_hybrid_ndcg_uplift_vs_lexical: Option<f64>,
    pub q035_exact_ref_top1_hit_rate: Option<f64>,
    pub q036_citation_parity_top1: Option<f64>,
    pub q037_hybrid_p95_latency_ms: Option<f64>,
    pub q037_latency_ratio_vs_lexical: Option<f64>,
    pub q038_retrieval_determinism_topk_overlap: Option<f64>,
    pub q045_hybrid_mrr_at_10_first_hit: Option<f64>,
    pub q046_hybrid_recall_at_50_delta_vs_lexical: Option<f64>,
    pub q047_judged_at_10_label_completeness: Option<f64>,
    pub q048_ndcg_uplift_p_value: Option<f64>,
    pub q048_ndcg_uplift_bootstrap_ci_low: Option<f64>,
    pub q048_ndcg_uplift_bootstrap_ci_high: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct NamedIngestRunSnapshot {
    pub manifest_name: String,
    pub snapshot: IngestRunSnapshot,
}

#[derive(Debug, Deserialize)]
pub struct TargetSectionsManifest {
    #[serde(default)]
    pub target_count: Option<usize>,
    pub targets: Vec<TargetSectionReference>,
}

#[derive(Debug, Deserialize)]
pub struct TargetSectionReference {
    pub id: String,
    pub part: u32,
}
