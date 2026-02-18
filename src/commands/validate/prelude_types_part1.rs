use std::collections::{HashMap, HashSet};
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::cli::ValidateArgs;
use crate::semantic::{chunk_payload_for_embedding, embedding_text_hash, DEFAULT_MODEL_ID};
use crate::util::{now_utc_string, write_json_pretty};

const DB_SCHEMA_VERSION: &str = "0.4.0";
const TABLE_SPARSE_ROW_RATIO_MAX: f64 = 0.20;
const TABLE_OVERLOADED_ROW_RATIO_MAX: f64 = 0.10;
const TABLE_MARKER_SEQUENCE_COVERAGE_MIN: f64 = 0.90;
const TABLE_DESCRIPTION_COVERAGE_MIN: f64 = 0.90;
const MARKER_EXTRACTION_COVERAGE_MIN: f64 = 0.95;
const MARKER_CITATION_ACCURACY_MIN: f64 = 0.90;
const PARAGRAPH_CITATION_ACCURACY_MIN: f64 = 0.90;
const ASIL_ALIGNMENT_MIN_RATING_COVERAGE: f64 = 0.60;
const ASIL_ALIGNMENT_MAX_MALFORMED_RATIO: f64 = 0.10;
const ASIL_ALIGNMENT_MAX_OUTLIER_RATIO: f64 = 0.15;
const WP2_EXTRACTION_PROVENANCE_COVERAGE_MIN: f64 = 1.0;
const WP2_TEXT_LAYER_REPLAY_STABILITY_MIN: f64 = 0.999;
const WP2_OCR_REPLAY_STABILITY_MIN: f64 = 0.98;
const WP2_PRINTED_MAPPING_DETECTABLE_MIN: f64 = 0.98;
const WP2_PRINTED_DETECTABILITY_DROP_MAX: f64 = 0.05;
const WP2_CLAUSE_MAX_WORDS: usize = 900;
const WP2_OVERLAP_MIN_WORDS: usize = 50;
const WP2_OVERLAP_MAX_WORDS: usize = 100;
const WP2_OVERLAP_COMPLIANCE_MIN: f64 = 0.95;
const WP2_LIST_FALLBACK_RATIO_MAX: f64 = 0.05;
const WP2_ASIL_STRICT_MIN_RATING_COVERAGE: f64 = 0.85;
const WP2_ASIL_STRICT_MAX_MALFORMED_RATIO: f64 = 0.05;
const WP2_ASIL_STRICT_MAX_OUTLIER_RATIO: f64 = 0.08;
const WP2_ASIL_STRICT_MAX_ONE_CELL_RATIO: f64 = 0.25;
const WP2_NOISE_LEAKAGE_GLOBAL_MAX: f64 = 0.001;
const WP2_CITATION_TOP1_MIN: f64 = 0.99;
const WP2_CITATION_TOP3_MIN: f64 = 1.0;
const WP2_CITATION_PAGE_RANGE_MIN: f64 = 0.99;
const WP2_CITATION_BASELINE_MODE_ENV: &str = "WP2_CITATION_BASELINE_MODE";
const WP2_CITATION_BASELINE_PATH_ENV: &str = "WP2_CITATION_BASELINE_PATH";
const WP2_CITATION_BASELINE_DECISION_ENV: &str = "WP2_CITATION_BASELINE_DECISION_ID";
const WP2_CITATION_BASELINE_REASON_ENV: &str = "WP2_CITATION_BASELINE_REASON";

#[derive(Debug, Deserialize, Serialize)]
struct GoldSetManifest {
    manifest_version: u32,
    generated_at: String,
    run_id: String,
    gold_references: Vec<GoldReference>,
}

#[derive(Debug, Deserialize, Serialize)]
struct GoldReference {
    id: String,
    doc_id: String,
    #[serde(rename = "ref")]
    reference: String,
    #[serde(default)]
    target_id: Option<String>,
    #[serde(default)]
    target_ref_raw: Option<String>,
    #[serde(default)]
    canonical_ref: Option<String>,
    #[serde(default)]
    ref_resolution_mode: Option<String>,
    expected_page_pattern: String,
    must_match_terms: Vec<String>,
    #[serde(default)]
    expected_node_type: Option<String>,
    #[serde(default)]
    expected_parent_ref: Option<String>,
    #[serde(default)]
    expected_min_rows: Option<usize>,
    #[serde(default)]
    expected_min_cols: Option<usize>,
    #[serde(default)]
    expected_min_list_items: Option<usize>,
    #[serde(default)]
    expected_anchor_type: Option<String>,
    #[serde(default)]
    expected_marker_label: Option<String>,
    #[serde(default)]
    expected_paragraph_index: Option<usize>,
    status: String,
}

#[derive(Debug)]
struct ReferenceEvaluation {
    skipped: bool,
    found: bool,
    chunk_type: Option<String>,
    page_start: Option<i64>,
    page_end: Option<i64>,
    source_hash: Option<String>,
    has_all_terms: bool,
    has_any_term: bool,
    table_row_count: usize,
    table_cell_count: usize,
    list_item_count: usize,
    lineage_complete: bool,
    hierarchy_ok: bool,
    page_pattern_match: Option<bool>,
}

#[derive(Debug, Serialize)]
struct HierarchyMetrics {
    references_with_lineage: usize,
    table_references_with_rows: usize,
    table_references_with_cells: usize,
    references_with_list_items: usize,
}

#[derive(Debug, Clone, Serialize)]
struct TargetCoverageReport {
    source_manifest: Option<String>,
    target_total: usize,
    target_linked_gold_total: usize,
    covered_target_total: usize,
    missing_target_ids: Vec<String>,
    duplicate_target_ids: Vec<String>,
    unexpected_target_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct FreshnessReport {
    source_manifest_dir: String,
    required_parts: Vec<u32>,
    latest_manifest: Option<String>,
    latest_run_id: Option<String>,
    latest_started_at: Option<String>,
    latest_run_parts: Vec<u32>,
    latest_run_by_part: Vec<PartFreshness>,
    full_target_cycle_run_id: Option<String>,
    stale_parts: Vec<u32>,
}

#[derive(Debug, Clone, Serialize)]
struct PartFreshness {
    part: u32,
    manifest: Option<String>,
    run_id: Option<String>,
    started_at: Option<String>,
}

#[derive(Debug, Serialize)]
struct QualityReport {
    manifest_version: u32,
    run_id: String,
    generated_at: String,
    status: String,
    summary: QualitySummary,
    wp2_stage_policy: Wp2StagePolicy,
    target_coverage: TargetCoverageReport,
    freshness: FreshnessReport,
    hierarchy_metrics: HierarchyMetrics,
    table_quality_scorecard: TableQualityScorecard,
    extraction_fidelity: ExtractionFidelityReport,
    hierarchy_semantics: HierarchySemanticsReport,
    table_semantics: TableSemanticsReport,
    citation_parity: CitationParitySummaryReport,
    semantic_embeddings: SemanticEmbeddingReport,
    semantic_quality: SemanticQualitySummaryReport,
    checks: Vec<QualityCheck>,
    issues: Vec<String>,
    recommendations: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct Wp2StagePolicy {
    requested_stage: String,
    effective_stage: String,
    enforcement_mode: String,
}

#[derive(Debug, Clone, Serialize, Default)]
struct ExtractionFidelityReport {
    source_manifest: Option<String>,
    processed_pages: usize,
    provenance_entries: usize,
    provenance_coverage: Option<f64>,
    unknown_backend_pages: usize,
    text_layer_replay_stability: Option<f64>,
    ocr_replay_stability: Option<f64>,
    ocr_page_ratio: Option<f64>,
    total_chunks: usize,
    printed_mapped_chunks: usize,
    printed_mapping_coverage: Option<f64>,
    printed_status_coverage: Option<f64>,
    printed_detectability_rate: Option<f64>,
    printed_detectability_drop_pp: Option<f64>,
    printed_mapping_on_detectable: Option<f64>,
    invalid_printed_label_count: usize,
    invalid_printed_range_count: usize,
    clause_chunks_over_900: usize,
    max_clause_chunk_words: Option<usize>,
    overlap_pair_count: usize,
    overlap_compliant_pairs: usize,
    overlap_compliance: Option<f64>,
    split_sequence_violations: usize,
    q025_exemption_count: usize,
    non_exempt_oversize_chunks: usize,
    normalization_noise_ratio: Option<f64>,
    normalization_target_noise_count: usize,
    dehyphenation_false_positive_rate: Option<f64>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
struct HierarchySemanticsReport {
    list_items_total: usize,
    list_semantics_complete: usize,
    list_semantics_completeness: Option<f64>,
    nested_parent_depth_violations: usize,
    list_parse_candidate_total: usize,
    list_parse_fallback_total: usize,
    list_parse_fallback_ratio: Option<f64>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
struct TableSemanticsReport {
    table_cells_total: usize,
    table_cells_semantics_complete: usize,
    table_cell_semantics_completeness: Option<f64>,
    invalid_span_count: usize,
    header_flag_completeness: Option<f64>,
    one_cell_row_ratio: Option<f64>,
    asil_rating_coverage: Option<f64>,
    asil_malformed_ratio: Option<f64>,
    asil_outlier_ratio: Option<f64>,
    asil_one_cell_row_ratio: Option<f64>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
struct CitationParitySummaryReport {
    baseline_path: String,
    baseline_mode: String,
    baseline_run_id: Option<String>,
    baseline_checksum: Option<String>,
    baseline_created: bool,
    baseline_missing: bool,
    target_linked_total: usize,
    comparable_total: usize,
    top1_parity: Option<f64>,
    top3_containment: Option<f64>,
    page_range_parity: Option<f64>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct TableQualityScorecard {
    source_manifest: Option<String>,
    counters: TableQualityCounters,
    table_sparse_row_ratio: Option<f64>,
    table_overloaded_row_ratio: Option<f64>,
    table_marker_sequence_coverage: Option<f64>,
    table_description_coverage: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct TableQualityCounters {
    table_row_nodes_inserted: usize,
    table_sparse_rows_count: usize,
    table_overloaded_rows_count: usize,
    table_rows_with_markers_count: usize,
    table_rows_with_descriptions_count: usize,
    table_marker_expected_count: usize,
    table_marker_observed_count: usize,
}

#[derive(Debug, Serialize)]
struct QualitySummary {
    total_checks: usize,
    passed: usize,
    failed: usize,
    pending: usize,
}

#[derive(Debug, Serialize, Clone)]
struct QualityCheck {
    check_id: String,
    name: String,
    result: String,
}

#[derive(Debug, Default)]
struct StructuralInvariantSummary {
    parent_required_missing_count: i64,
    dangling_parent_pointer_count: i64,
    invalid_table_row_parent_count: i64,
    invalid_table_cell_parent_count: i64,
    invalid_list_item_parent_count: i64,
    invalid_note_parent_count: i64,
    invalid_note_item_parent_count: i64,
    invalid_paragraph_parent_count: i64,
}

#[derive(Debug, Default)]
struct AsilTableAlignmentSummary {
    tables_expected: usize,
    tables_found: usize,
    marker_rows_total: usize,
    marker_rows_with_ratings: usize,
    marker_rows_malformed_description: usize,
    marker_rows_outlier_cell_count: usize,
}

impl AsilTableAlignmentSummary {
    fn rating_coverage(&self) -> Option<f64> {
        ratio(self.marker_rows_with_ratings, self.marker_rows_total)
    }

    fn malformed_ratio(&self) -> Option<f64> {
        ratio(
            self.marker_rows_malformed_description,
            self.marker_rows_total,
        )
    }

    fn outlier_ratio(&self) -> Option<f64> {
        ratio(self.marker_rows_outlier_cell_count, self.marker_rows_total)
    }
}

impl StructuralInvariantSummary {
    fn violation_count(&self) -> i64 {
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
struct RunStateManifest {
    active_run_id: Option<String>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct IngestRunSnapshot {
    #[serde(default)]
    run_id: Option<String>,
    #[serde(default)]
    started_at: Option<String>,
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    processed_parts: Vec<u32>,
    #[serde(default)]
    counts: IngestRunCountsSnapshot,
    #[serde(default)]
    paths: IngestRunPathsSnapshot,
    #[serde(default)]
    db_schema_version: Option<String>,
}

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(default)]
struct IngestRunPathsSnapshot {
    page_provenance_path: Option<String>,
}

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(default)]
struct IngestRunCountsSnapshot {
    processed_pdf_count: usize,
    text_layer_page_count: usize,
    ocr_page_count: usize,
    ocr_fallback_page_count: usize,
    empty_page_count: usize,
    header_lines_removed: usize,
    footer_lines_removed: usize,
    dehyphenation_merges: usize,
    list_parse_candidate_count: usize,
    list_parse_fallback_count: usize,
    table_row_nodes_inserted: usize,
    table_sparse_rows_count: usize,
    table_overloaded_rows_count: usize,
    table_rows_with_markers_count: usize,
    table_rows_with_descriptions_count: usize,
    table_marker_expected_count: usize,
    table_marker_observed_count: usize,
}

impl IngestRunCountsSnapshot {
    fn table_quality_counters(&self) -> TableQualityCounters {
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
struct PageProvenanceManifestSnapshot {
    entries: Vec<PageProvenanceEntry>,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(default)]
struct PageProvenanceEntry {
    doc_id: String,
    page_pdf: i64,
    backend: String,
    reason: String,
    text_char_count: usize,
    ocr_char_count: Option<usize>,
    printed_page_label: Option<String>,
    printed_page_status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Wp2GateStage {
    A,
    B,
}

impl Wp2GateStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::A => "A",
            Self::B => "B",
        }
    }

    fn mode_label(self) -> &'static str {
        match self {
            Self::A => "instrumentation",
            Self::B => "hard_gate",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CitationBaselineMode {
    Verify,
    Bootstrap,
}

impl CitationBaselineMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Verify => "verify",
            Self::Bootstrap => "bootstrap",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CitationParityIdentity {
    canonical_ref: String,
    anchor_identity: String,
    page_start: Option<i64>,
    page_end: Option<i64>,
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
struct CitationParityEntry {
    target_id: String,
    doc_id: String,
    reference: String,
    top_results: Vec<CitationParityIdentity>,
}
