#[derive(Debug, Clone, Serialize, Deserialize)]
struct CitationParityBaseline {
    manifest_version: u32,
    run_id: String,
    generated_at: String,
    db_schema_version: Option<String>,
    #[serde(default)]
    decision_id: Option<String>,
    #[serde(default)]
    change_reason: Option<String>,
    target_linked_count: usize,
    query_options: String,
    checksum: String,
    entries: Vec<CitationParityEntry>,
}

#[derive(Debug, Clone, Serialize)]
struct CitationParityArtifact {
    manifest_version: u32,
    run_id: String,
    generated_at: String,
    baseline_path: String,
    baseline_mode: String,
    baseline_checksum: Option<String>,
    baseline_missing: bool,
    target_linked_count: usize,
    comparable_count: usize,
    top1_parity: Option<f64>,
    top3_containment: Option<f64>,
    page_range_parity: Option<f64>,
    baseline_created: bool,
    entries: Vec<CitationParityComparisonEntry>,
}

#[derive(Debug, Clone, Serialize)]
struct CitationParityComparisonEntry {
    target_id: String,
    top1_match: bool,
    top3_contains_baseline: bool,
    page_range_match: bool,
}

#[derive(Debug)]
struct Wp2Assessment {
    checks: Vec<QualityCheck>,
    extraction_fidelity: ExtractionFidelityReport,
    hierarchy_semantics: HierarchySemanticsReport,
    table_semantics: TableSemanticsReport,
    citation_parity: CitationParitySummaryReport,
    semantic_embeddings: SemanticEmbeddingReport,
    semantic_quality: SemanticQualitySummaryReport,
    recommendations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
struct SemanticEmbeddingReport {
    active_model_id: String,
    embedding_dim: Option<usize>,
    eligible_chunks: usize,
    embedded_chunks: usize,
    stale_rows: usize,
    embedding_rows_for_active_model: usize,
    chunk_embedding_coverage_ratio: Option<f64>,
    stale_embedding_ratio: Option<f64>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
struct SemanticQualitySummaryReport {
    source_eval_manifest: Option<String>,
    quality_report_path: Option<String>,
    active_model_id: Option<String>,
    total_queries: usize,
    non_exact_queries: usize,
    exact_queries: usize,
    semantic_ndcg_at_10: Option<f64>,
    lexical_ndcg_at_10: Option<f64>,
    hybrid_ndcg_at_10: Option<f64>,
    hybrid_ndcg_uplift_vs_lexical: Option<f64>,
    exact_ref_top1_hit_rate: Option<f64>,
    citation_parity_top1: Option<f64>,
    lexical_p95_latency_ms: Option<f64>,
    hybrid_p95_latency_ms: Option<f64>,
    latency_ratio_vs_lexical: Option<f64>,
    retrieval_determinism_topk_overlap: Option<f64>,
    hybrid_mrr_at_10_first_hit: Option<f64>,
    lexical_recall_at_50: Option<f64>,
    hybrid_recall_at_50: Option<f64>,
    hybrid_recall_at_50_delta_vs_lexical: Option<f64>,
    judged_at_10_label_completeness: Option<f64>,
    ndcg_uplift_p_value: Option<f64>,
    ndcg_uplift_bootstrap_ci_low: Option<f64>,
    ndcg_uplift_bootstrap_ci_high: Option<f64>,
    baseline_path: String,
    baseline_mode: String,
    baseline_run_id: Option<String>,
    baseline_checksum: Option<String>,
    baseline_created: bool,
    baseline_missing: bool,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SemanticBaselineMode {
    Verify,
    Bootstrap,
}

impl SemanticBaselineMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Verify => "verify",
            Self::Bootstrap => "bootstrap",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SemanticEvalManifest {
    manifest_version: u32,
    generated_at: String,
    source: String,
    queries: Vec<SemanticEvalQuery>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SemanticEvalQuery {
    query_id: String,
    query_text: String,
    intent: String,
    expected_chunk_ids: Vec<String>,
    #[serde(default)]
    judged_chunk_ids: Vec<String>,
    #[serde(default)]
    expected_refs: Vec<String>,
    must_hit_top1: bool,
    #[serde(default)]
    part_filter: Option<u32>,
    #[serde(default)]
    chunk_type_filter: Option<String>,
    #[serde(default)]
    notes: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SemanticQualityArtifact {
    manifest_version: u32,
    run_id: String,
    generated_at: String,
    source_eval_manifest: String,
    active_model_id: Option<String>,
    summary: SemanticQualitySummaryReport,
    query_results: Vec<SemanticQualityQueryResult>,
}

#[derive(Debug, Clone, Serialize)]
struct SemanticQualityQueryResult {
    query_id: String,
    intent: String,
    query_text: String,
    expected_chunk_ids: Vec<String>,
    lexical_top_chunk_ids: Vec<String>,
    semantic_top_chunk_ids: Vec<String>,
    hybrid_top_chunk_ids: Vec<String>,
    lexical_ndcg_at_10: Option<f64>,
    semantic_ndcg_at_10: Option<f64>,
    hybrid_ndcg_at_10: Option<f64>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SemanticRetrievalBaseline {
    manifest_version: u32,
    run_id: String,
    generated_at: String,
    db_schema_version: Option<String>,
    #[serde(default)]
    decision_id: Option<String>,
    #[serde(default)]
    change_reason: Option<String>,
    check_ids: Vec<String>,
    query_ids: Vec<String>,
    thresholds: SemanticRetrievalBaselineThresholds,
    summary_metrics: SemanticRetrievalBaselineMetrics,
    checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SemanticRetrievalBaselineThresholds {
    q031_stage_a_min: f64,
    q031_stage_b_min: f64,
    q032_stage_a_max: f64,
    q032_stage_b_max: f64,
    q033_stage_a_min: f64,
    q033_stage_b_min: f64,
    q034_stage_a_min: f64,
    q034_stage_b_min: f64,
    q035_stage_a_min: f64,
    q035_stage_b_min: f64,
    q036_stage_a_min: f64,
    q036_stage_b_min: f64,
    q037_latency_ratio_stage_a_max: f64,
    q037_latency_ratio_stage_b_max: f64,
    q037_hybrid_p95_stage_b_max_ms: f64,
    q038_stage_a_min: f64,
    q038_stage_b_min: f64,
    q045_stage_a_min: f64,
    q045_stage_b_min: f64,
    q046_stage_a_max_drop: f64,
    q046_stage_b_max_drop: f64,
    q047_stage_a_min: f64,
    q047_stage_b_min: f64,
    q048_stage_a_p_max: f64,
    q048_stage_b_p_max: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SemanticRetrievalBaselineMetrics {
    q031_chunk_embedding_coverage_ratio: Option<f64>,
    q032_stale_embedding_ratio: Option<f64>,
    q033_semantic_ndcg_at_10: Option<f64>,
    q034_hybrid_ndcg_uplift_vs_lexical: Option<f64>,
    q035_exact_ref_top1_hit_rate: Option<f64>,
    q036_citation_parity_top1: Option<f64>,
    q037_hybrid_p95_latency_ms: Option<f64>,
    q037_latency_ratio_vs_lexical: Option<f64>,
    q038_retrieval_determinism_topk_overlap: Option<f64>,
    q045_hybrid_mrr_at_10_first_hit: Option<f64>,
    q046_hybrid_recall_at_50_delta_vs_lexical: Option<f64>,
    q047_judged_at_10_label_completeness: Option<f64>,
    q048_ndcg_uplift_p_value: Option<f64>,
    q048_ndcg_uplift_bootstrap_ci_low: Option<f64>,
    q048_ndcg_uplift_bootstrap_ci_high: Option<f64>,
}

#[derive(Debug, Clone)]
struct NamedIngestRunSnapshot {
    manifest_name: String,
    snapshot: IngestRunSnapshot,
}

#[derive(Debug, Deserialize)]
struct TargetSectionsManifest {
    #[serde(default)]
    target_count: Option<usize>,
    targets: Vec<TargetSectionReference>,
}

#[derive(Debug, Deserialize)]
struct TargetSectionReference {
    id: String,
    part: u32,
}
