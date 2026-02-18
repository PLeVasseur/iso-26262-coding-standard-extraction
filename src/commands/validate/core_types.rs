use super::*;

pub const DB_SCHEMA_VERSION: &str = "0.4.0";
pub const TABLE_SPARSE_ROW_RATIO_MAX: f64 = 0.20;
pub const TABLE_OVERLOADED_ROW_RATIO_MAX: f64 = 0.10;
pub const TABLE_MARKER_SEQUENCE_COVERAGE_MIN: f64 = 0.90;
pub const TABLE_DESCRIPTION_COVERAGE_MIN: f64 = 0.90;
pub const MARKER_EXTRACTION_COVERAGE_MIN: f64 = 0.95;
pub const MARKER_CITATION_ACCURACY_MIN: f64 = 0.90;
pub const PARAGRAPH_CITATION_ACCURACY_MIN: f64 = 0.90;
pub const ASIL_ALIGNMENT_MIN_RATING_COVERAGE: f64 = 0.60;
pub const ASIL_ALIGNMENT_MAX_MALFORMED_RATIO: f64 = 0.10;
pub const ASIL_ALIGNMENT_MAX_OUTLIER_RATIO: f64 = 0.15;
pub const WP2_EXTRACTION_PROVENANCE_COVERAGE_MIN: f64 = 1.0;
pub const WP2_TEXT_LAYER_REPLAY_STABILITY_MIN: f64 = 0.999;
pub const WP2_OCR_REPLAY_STABILITY_MIN: f64 = 0.98;
pub const WP2_PRINTED_MAPPING_DETECTABLE_MIN: f64 = 0.98;
pub const WP2_PRINTED_DETECTABILITY_DROP_MAX: f64 = 0.05;
pub const WP2_CLAUSE_MAX_WORDS: usize = 900;
pub const WP2_OVERLAP_MIN_WORDS: usize = 50;
pub const WP2_OVERLAP_MAX_WORDS: usize = 100;
pub const WP2_OVERLAP_COMPLIANCE_MIN: f64 = 0.95;
pub const WP2_LIST_FALLBACK_RATIO_MAX: f64 = 0.05;
pub const WP2_ASIL_STRICT_MIN_RATING_COVERAGE: f64 = 0.85;
pub const WP2_ASIL_STRICT_MAX_MALFORMED_RATIO: f64 = 0.05;
pub const WP2_ASIL_STRICT_MAX_OUTLIER_RATIO: f64 = 0.08;
pub const WP2_ASIL_STRICT_MAX_ONE_CELL_RATIO: f64 = 0.25;
pub const WP2_NOISE_LEAKAGE_GLOBAL_MAX: f64 = 0.001;
pub const WP2_CITATION_TOP1_MIN: f64 = 0.99;
pub const WP2_CITATION_TOP3_MIN: f64 = 1.0;
pub const WP2_CITATION_PAGE_RANGE_MIN: f64 = 0.99;
pub const WP2_CITATION_BASELINE_MODE_ENV: &str = "WP2_CITATION_BASELINE_MODE";
pub const WP2_CITATION_BASELINE_PATH_ENV: &str = "WP2_CITATION_BASELINE_PATH";
pub const WP2_CITATION_BASELINE_DECISION_ENV: &str = "WP2_CITATION_BASELINE_DECISION_ID";
pub const WP2_CITATION_BASELINE_REASON_ENV: &str = "WP2_CITATION_BASELINE_REASON";

#[derive(Debug, Deserialize, Serialize)]
pub struct GoldSetManifest {
    pub manifest_version: u32,
    pub generated_at: String,
    pub run_id: String,
    pub gold_references: Vec<GoldReference>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GoldReference {
    pub id: String,
    pub doc_id: String,
    #[serde(rename = "ref")]
    pub reference: String,
    #[serde(default)]
    pub target_id: Option<String>,
    #[serde(default)]
    pub target_ref_raw: Option<String>,
    #[serde(default)]
    pub canonical_ref: Option<String>,
    #[serde(default)]
    pub ref_resolution_mode: Option<String>,
    pub expected_page_pattern: String,
    pub must_match_terms: Vec<String>,
    #[serde(default)]
    pub expected_node_type: Option<String>,
    #[serde(default)]
    pub expected_parent_ref: Option<String>,
    #[serde(default)]
    pub expected_min_rows: Option<usize>,
    #[serde(default)]
    pub expected_min_cols: Option<usize>,
    #[serde(default)]
    pub expected_min_list_items: Option<usize>,
    #[serde(default)]
    pub expected_anchor_type: Option<String>,
    #[serde(default)]
    pub expected_marker_label: Option<String>,
    #[serde(default)]
    pub expected_paragraph_index: Option<usize>,
    pub status: String,
}

#[derive(Debug)]
pub struct ReferenceEvaluation {
    pub skipped: bool,
    pub found: bool,
    pub chunk_type: Option<String>,
    pub page_start: Option<i64>,
    pub page_end: Option<i64>,
    pub source_hash: Option<String>,
    pub has_all_terms: bool,
    pub has_any_term: bool,
    pub table_row_count: usize,
    pub table_cell_count: usize,
    pub list_item_count: usize,
    pub lineage_complete: bool,
    pub hierarchy_ok: bool,
    pub page_pattern_match: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct HierarchyMetrics {
    pub references_with_lineage: usize,
    pub table_references_with_rows: usize,
    pub table_references_with_cells: usize,
    pub references_with_list_items: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct TargetCoverageReport {
    pub source_manifest: Option<String>,
    pub target_total: usize,
    pub target_linked_gold_total: usize,
    pub covered_target_total: usize,
    pub missing_target_ids: Vec<String>,
    pub duplicate_target_ids: Vec<String>,
    pub unexpected_target_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FreshnessReport {
    pub source_manifest_dir: String,
    pub required_parts: Vec<u32>,
    pub latest_manifest: Option<String>,
    pub latest_run_id: Option<String>,
    pub latest_started_at: Option<String>,
    pub latest_run_parts: Vec<u32>,
    pub latest_run_by_part: Vec<PartFreshness>,
    pub full_target_cycle_run_id: Option<String>,
    pub stale_parts: Vec<u32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PartFreshness {
    pub part: u32,
    pub manifest: Option<String>,
    pub run_id: Option<String>,
    pub started_at: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct QualityReport {
    pub manifest_version: u32,
    pub run_id: String,
    pub generated_at: String,
    pub status: String,
    pub summary: QualitySummary,
    pub wp2_stage_policy: Wp2StagePolicy,
    pub target_coverage: TargetCoverageReport,
    pub freshness: FreshnessReport,
    pub hierarchy_metrics: HierarchyMetrics,
    pub table_quality_scorecard: TableQualityScorecard,
    pub extraction_fidelity: ExtractionFidelityReport,
    pub hierarchy_semantics: HierarchySemanticsReport,
    pub table_semantics: TableSemanticsReport,
    pub citation_parity: CitationParitySummaryReport,
    pub semantic_embeddings: SemanticEmbeddingReport,
    pub semantic_quality: SemanticQualitySummaryReport,
    pub checks: Vec<QualityCheck>,
    pub issues: Vec<String>,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Wp2StagePolicy {
    pub requested_stage: String,
    pub effective_stage: String,
    pub enforcement_mode: String,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ExtractionFidelityReport {
    pub source_manifest: Option<String>,
    pub processed_pages: usize,
    pub provenance_entries: usize,
    pub provenance_coverage: Option<f64>,
    pub unknown_backend_pages: usize,
    pub text_layer_replay_stability: Option<f64>,
    pub ocr_replay_stability: Option<f64>,
    pub ocr_page_ratio: Option<f64>,
    pub total_chunks: usize,
    pub printed_mapped_chunks: usize,
    pub printed_mapping_coverage: Option<f64>,
    pub printed_status_coverage: Option<f64>,
    pub printed_detectability_rate: Option<f64>,
    pub printed_detectability_drop_pp: Option<f64>,
    pub printed_mapping_on_detectable: Option<f64>,
    pub invalid_printed_label_count: usize,
    pub invalid_printed_range_count: usize,
    pub clause_chunks_over_900: usize,
    pub max_clause_chunk_words: Option<usize>,
    pub overlap_pair_count: usize,
    pub overlap_compliant_pairs: usize,
    pub overlap_compliance: Option<f64>,
    pub split_sequence_violations: usize,
    pub q025_exemption_count: usize,
    pub non_exempt_oversize_chunks: usize,
    pub normalization_noise_ratio: Option<f64>,
    pub normalization_target_noise_count: usize,
    pub dehyphenation_false_positive_rate: Option<f64>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct HierarchySemanticsReport {
    pub list_items_total: usize,
    pub list_semantics_complete: usize,
    pub list_semantics_completeness: Option<f64>,
    pub nested_parent_depth_violations: usize,
    pub list_parse_candidate_total: usize,
    pub list_parse_fallback_total: usize,
    pub list_parse_fallback_ratio: Option<f64>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct TableSemanticsReport {
    pub table_cells_total: usize,
    pub table_cells_semantics_complete: usize,
    pub table_cell_semantics_completeness: Option<f64>,
    pub invalid_span_count: usize,
    pub header_flag_completeness: Option<f64>,
    pub one_cell_row_ratio: Option<f64>,
    pub asil_rating_coverage: Option<f64>,
    pub asil_malformed_ratio: Option<f64>,
    pub asil_outlier_ratio: Option<f64>,
    pub asil_one_cell_row_ratio: Option<f64>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct CitationParitySummaryReport {
    pub baseline_path: String,
    pub baseline_mode: String,
    pub baseline_run_id: Option<String>,
    pub baseline_checksum: Option<String>,
    pub baseline_created: bool,
    pub baseline_missing: bool,
    pub target_linked_total: usize,
    pub comparable_total: usize,
    pub top1_parity: Option<f64>,
    pub top3_containment: Option<f64>,
    pub page_range_parity: Option<f64>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TableQualityScorecard {
    pub source_manifest: Option<String>,
    pub counters: TableQualityCounters,
    pub table_sparse_row_ratio: Option<f64>,
    pub table_overloaded_row_ratio: Option<f64>,
    pub table_marker_sequence_coverage: Option<f64>,
    pub table_description_coverage: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TableQualityCounters {
    pub table_row_nodes_inserted: usize,
    pub table_sparse_rows_count: usize,
    pub table_overloaded_rows_count: usize,
    pub table_rows_with_markers_count: usize,
    pub table_rows_with_descriptions_count: usize,
    pub table_marker_expected_count: usize,
    pub table_marker_observed_count: usize,
}

#[derive(Debug, Serialize)]
pub struct QualitySummary {
    pub total_checks: usize,
    pub passed: usize,
    pub failed: usize,
    pub pending: usize,
}

#[derive(Debug, Serialize, Clone)]
pub struct QualityCheck {
    pub check_id: String,
    pub name: String,
    pub result: String,
}

#[derive(Debug, Default)]
pub struct StructuralInvariantSummary {
    pub parent_required_missing_count: i64,
    pub dangling_parent_pointer_count: i64,
    pub invalid_table_row_parent_count: i64,
    pub invalid_table_cell_parent_count: i64,
    pub invalid_list_item_parent_count: i64,
    pub invalid_note_parent_count: i64,
    pub invalid_note_item_parent_count: i64,
    pub invalid_paragraph_parent_count: i64,
}

#[derive(Debug, Default)]
pub struct AsilTableAlignmentSummary {
    pub tables_expected: usize,
    pub tables_found: usize,
    pub marker_rows_total: usize,
    pub marker_rows_with_ratings: usize,
    pub marker_rows_malformed_description: usize,
    pub marker_rows_outlier_cell_count: usize,
}

impl AsilTableAlignmentSummary {
    pub fn rating_coverage(&self) -> Option<f64> {
        ratio(self.marker_rows_with_ratings, self.marker_rows_total)
    }

    pub fn malformed_ratio(&self) -> Option<f64> {
        ratio(
            self.marker_rows_malformed_description,
            self.marker_rows_total,
        )
    }

    pub fn outlier_ratio(&self) -> Option<f64> {
        ratio(self.marker_rows_outlier_cell_count, self.marker_rows_total)
    }
}

impl StructuralInvariantSummary {
    pub fn violation_count(&self) -> i64 {
        self.parent_required_missing_count
            + self.dangling_parent_pointer_count
            + self.invalid_table_row_parent_count
            + self.invalid_table_cell_parent_count
            + self.invalid_list_item_parent_count
            + self.invalid_note_parent_count
            + self.invalid_note_item_parent_count
            + self.invalid_paragraph_parent_count
    }
}

#[derive(Debug, Deserialize)]
pub struct RunStateManifest {
    pub active_run_id: Option<String>,
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct IngestRunSnapshot {
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub started_at: Option<String>,
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub processed_parts: Vec<u32>,
    #[serde(default)]
    pub counts: IngestRunCountsSnapshot,
    #[serde(default)]
    pub paths: IngestRunPathsSnapshot,
    #[serde(default)]
    pub db_schema_version: Option<String>,
}

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(default)]
pub struct IngestRunPathsSnapshot {
    pub page_provenance_path: Option<String>,
}

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(default)]
pub struct IngestRunCountsSnapshot {
    pub processed_pdf_count: usize,
    pub text_layer_page_count: usize,
    pub ocr_page_count: usize,
    pub ocr_fallback_page_count: usize,
    pub empty_page_count: usize,
    pub header_lines_removed: usize,
    pub footer_lines_removed: usize,
    pub dehyphenation_merges: usize,
    pub list_parse_candidate_count: usize,
    pub list_parse_fallback_count: usize,
    pub table_row_nodes_inserted: usize,
    pub table_sparse_rows_count: usize,
    pub table_overloaded_rows_count: usize,
    pub table_rows_with_markers_count: usize,
    pub table_rows_with_descriptions_count: usize,
    pub table_marker_expected_count: usize,
    pub table_marker_observed_count: usize,
}

impl IngestRunCountsSnapshot {
    pub fn table_quality_counters(&self) -> TableQualityCounters {
        TableQualityCounters {
            table_row_nodes_inserted: self.table_row_nodes_inserted,
            table_sparse_rows_count: self.table_sparse_rows_count,
            table_overloaded_rows_count: self.table_overloaded_rows_count,
            table_rows_with_markers_count: self.table_rows_with_markers_count,
            table_rows_with_descriptions_count: self.table_rows_with_descriptions_count,
            table_marker_expected_count: self.table_marker_expected_count,
            table_marker_observed_count: self.table_marker_observed_count,
        }
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
pub struct PageProvenanceManifestSnapshot {
    pub entries: Vec<PageProvenanceEntry>,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(default)]
pub struct PageProvenanceEntry {
    pub doc_id: String,
    pub page_pdf: i64,
    pub backend: String,
    pub reason: String,
    pub text_char_count: usize,
    pub ocr_char_count: Option<usize>,
    pub printed_page_label: Option<String>,
    pub printed_page_status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wp2GateStage {
    A,
    B,
}

impl Wp2GateStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::A => "A",
            Self::B => "B",
        }
    }

    pub fn mode_label(self) -> &'static str {
        match self {
            Self::A => "instrumentation",
            Self::B => "hard_gate",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CitationBaselineMode {
    Verify,
    Bootstrap,
}

impl CitationBaselineMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Verify => "verify",
            Self::Bootstrap => "bootstrap",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CitationParityIdentity {
    pub canonical_ref: String,
    pub anchor_identity: String,
    pub page_start: Option<i64>,
    pub page_end: Option<i64>,
}

impl PartialEq for CitationParityIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.canonical_ref == other.canonical_ref
            && self.anchor_identity == other.anchor_identity
            && self.page_start == other.page_start
            && self.page_end == other.page_end
    }
}

impl Eq for CitationParityIdentity {}

impl Hash for CitationParityIdentity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.canonical_ref.hash(state);
        self.anchor_identity.hash(state);
        self.page_start.hash(state);
        self.page_end.hash(state);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CitationParityEntry {
    pub target_id: String,
    pub doc_id: String,
    pub reference: String,
    pub top_results: Vec<CitationParityIdentity>,
}
