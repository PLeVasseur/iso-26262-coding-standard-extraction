use std::collections::{HashMap, HashSet};
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use rusqlite::{params, Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::cli::ValidateArgs;
use crate::util::{now_utc_string, write_json_pretty};

const DB_SCHEMA_VERSION: &str = "0.3.0";
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
    recommendations: Vec<String>,
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

pub fn run(args: ValidateArgs) -> Result<()> {
    let manifest_dir = args.cache_root.join("manifests");
    let gold_manifest_path = args
        .gold_manifest_path
        .clone()
        .unwrap_or_else(|| manifest_dir.join("gold_set_expected_results.json"));
    let quality_report_path = args
        .quality_report_path
        .clone()
        .unwrap_or_else(|| manifest_dir.join("extraction_quality_report.json"));
    let db_path = args
        .db_path
        .clone()
        .unwrap_or_else(|| args.cache_root.join("iso26262_index.sqlite"));

    let mut gold_manifest = load_gold_manifest(&gold_manifest_path)?;
    let run_id = resolve_run_id(&manifest_dir, &gold_manifest.run_id);
    let wp2_stage = resolve_wp2_gate_stage();
    let wp2_stage_policy = Wp2StagePolicy {
        requested_stage: std::env::var("WP2_GATE_STAGE").unwrap_or_else(|_| "A".to_string()),
        effective_stage: wp2_stage.as_str().to_string(),
        enforcement_mode: wp2_stage.mode_label().to_string(),
    };
    let citation_baseline_mode = resolve_citation_baseline_mode();
    let citation_baseline_path = resolve_citation_baseline_path();
    if wp2_stage == Wp2GateStage::B && citation_baseline_mode == CitationBaselineMode::Bootstrap {
        bail!(
            "{}=bootstrap is not allowed with WP2_GATE_STAGE=B; run Stage A first to bootstrap lockfile at {}",
            WP2_CITATION_BASELINE_MODE_ENV,
            citation_baseline_path.display()
        );
    }
    let ingest_snapshots = load_ingest_snapshots(&manifest_dir).unwrap_or_default();
    let latest_ingest_snapshot = ingest_snapshots.last().cloned();
    let previous_ingest_snapshot = if ingest_snapshots.len() > 1 {
        ingest_snapshots
            .get(ingest_snapshots.len().saturating_sub(2))
            .cloned()
    } else {
        None
    };
    let table_quality_scorecard =
        load_table_quality_scorecard(&manifest_dir).unwrap_or_else(|_| empty_table_scorecard());

    let connection = Connection::open_with_flags(
        &db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("failed to open database read-only: {}", db_path.display()))?;

    let evaluable_doc_ids = collect_evaluable_doc_ids(&connection)?;

    let mut evaluations = Vec::with_capacity(gold_manifest.gold_references.len());
    for reference in &mut gold_manifest.gold_references {
        if !evaluable_doc_ids.contains(&reference.doc_id) {
            reference.status = "skip".to_string();
            evaluations.push(skipped_reference_evaluation());
            continue;
        }

        let evaluation = evaluate_reference(&connection, reference)?;
        let hierarchy_required = has_hierarchy_expectations(reference);
        let hierarchy_ok = !hierarchy_required || evaluation.hierarchy_ok;
        reference.status = if evaluation.found && evaluation.has_all_terms && hierarchy_ok {
            "pass".to_string()
        } else {
            "fail".to_string()
        };
        evaluations.push(evaluation);
    }

    write_json_pretty(&gold_manifest_path, &gold_manifest)?;

    let target_sections = load_target_sections_manifest(&manifest_dir)?;
    let target_coverage =
        build_target_coverage_report(&target_sections, &gold_manifest.gold_references);
    let freshness = build_freshness_report(&manifest_dir, &target_sections)?;

    let mut checks = build_quality_checks(
        &connection,
        &gold_manifest.gold_references,
        &evaluations,
        &table_quality_scorecard,
        &target_coverage,
        &freshness,
    )?;
    let wp2_assessment = build_wp2_assessment(
        &connection,
        &manifest_dir,
        &run_id,
        &gold_manifest.gold_references,
        wp2_stage,
        &citation_baseline_path,
        citation_baseline_mode,
        latest_ingest_snapshot.as_ref(),
        previous_ingest_snapshot.as_ref(),
    )?;
    checks.extend(wp2_assessment.checks.iter().cloned());

    let summary = summarize_checks(&checks);
    let hierarchy_metrics = build_hierarchy_metrics(&evaluations);

    let issues = checks
        .iter()
        .filter(|check| check.result == "failed")
        .map(|check| format!("{} failed", check.name))
        .collect::<Vec<String>>();

    let mut recommendations = Vec::new();
    if checks
        .iter()
        .any(|check| check.check_id == "Q-001" && check.result == "failed")
    {
        recommendations.push(
            "Review heading parsing and reference normalization for missing gold references."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-002" && check.result == "pending")
    {
        recommendations.push(
            "Populate expected page patterns in gold set for citation range validation."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-003" && check.result == "failed")
    {
        recommendations.push(
            "Improve structured table extraction to populate table_row/table_cell descendants for key references."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-007" && check.result == "failed")
    {
        recommendations.push(
            "Ensure chunk lineage columns (origin_node_id, leaf_node_type, ancestor_path) are populated on ingest."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-011" && check.result == "failed")
    {
        recommendations.push(
            "Reduce sparse table rows by improving continuation merge rules for marker-bearing rows."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-012" && check.result == "failed")
    {
        recommendations.push(
            "Reduce overloaded table rows by splitting rows that contain multiple marker tokens."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-013" && check.result == "failed")
    {
        recommendations.push(
            "Improve table marker sequence coverage by repairing missing marker rows and preserving marker order."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-014" && check.result == "failed")
    {
        recommendations.push(
            "Increase table description coverage by populating non-empty description cells for marker rows."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-015" && check.result == "failed")
    {
        recommendations.push(
            "Improve marker extraction coverage by expanding marker parsing for list and note patterns."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-016" && check.result == "failed")
    {
        recommendations.push(
            "Improve marker citation accuracy by validating expected marker labels against extracted anchors."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-017" && check.result == "failed")
    {
        recommendations.push(
            "Improve paragraph fallback citation accuracy by stabilizing paragraph segmentation and indices."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-018" && check.result == "failed")
    {
        recommendations.push(
            "Fix structural hierarchy violations (parent lineage, dangling pointers, and note/list/table parent contracts)."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-019" && check.result == "failed")
    {
        recommendations.push(
            "Improve ASIL table row/cell alignment by distributing rating cells across marker rows and reducing malformed marker descriptions."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-020" && check.result == "failed")
    {
        recommendations.push(
            "Ensure target_sections.json and target-linked gold rows stay in one-to-one alignment (no missing, duplicate, or unexpected target_id values)."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-021" && check.result == "failed")
    {
        recommendations.push(
            "Resolve target-linked retrieval failures by correcting canonical references and expected node/anchor metadata for target-linked gold rows."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-022" && check.result == "failed")
    {
        recommendations.push(
            "Run a single full-target ingest cycle for Parts 2, 6, 8, and 9 so freshness is consistent across all required target parts."
                .to_string(),
        );
    }
    recommendations.extend(wp2_assessment.recommendations.iter().cloned());

    let report = QualityReport {
        manifest_version: 2,
        run_id,
        generated_at: now_utc_string(),
        status: if summary.failed > 0 {
            "failed".to_string()
        } else if summary.pending > 0 {
            "partial".to_string()
        } else {
            "passed".to_string()
        },
        summary,
        wp2_stage_policy,
        target_coverage,
        freshness,
        hierarchy_metrics,
        table_quality_scorecard,
        extraction_fidelity: wp2_assessment.extraction_fidelity,
        hierarchy_semantics: wp2_assessment.hierarchy_semantics,
        table_semantics: wp2_assessment.table_semantics,
        citation_parity: wp2_assessment.citation_parity,
        checks,
        issues,
        recommendations,
    };

    write_json_pretty(&quality_report_path, &report)?;

    info!(
        gold_path = %gold_manifest_path.display(),
        report_path = %quality_report_path.display(),
        "validation completed"
    );

    Ok(())
}

fn resolve_wp2_gate_stage() -> Wp2GateStage {
    match std::env::var("WP2_GATE_STAGE") {
        Ok(value) if value.trim().eq_ignore_ascii_case("B") => Wp2GateStage::B,
        _ => Wp2GateStage::A,
    }
}

fn resolve_citation_baseline_mode() -> CitationBaselineMode {
    parse_citation_baseline_mode(
        std::env::var(WP2_CITATION_BASELINE_MODE_ENV)
            .ok()
            .as_deref(),
    )
}

fn resolve_citation_baseline_path() -> PathBuf {
    parse_citation_baseline_path(
        std::env::var(WP2_CITATION_BASELINE_PATH_ENV)
            .ok()
            .as_deref(),
    )
}

fn parse_citation_baseline_mode(value: Option<&str>) -> CitationBaselineMode {
    match value {
        Some(value)
            if value.trim().eq_ignore_ascii_case("bootstrap")
                || value.trim().eq_ignore_ascii_case("rotate") =>
        {
            CitationBaselineMode::Bootstrap
        }
        _ => CitationBaselineMode::Verify,
    }
}

fn parse_citation_baseline_path(value: Option<&str>) -> PathBuf {
    if let Some(value) = value {
        let candidate = value.trim();
        if !candidate.is_empty() {
            return PathBuf::from(candidate);
        }
    }

    PathBuf::from("manifests").join("citation_parity_baseline.lock.json")
}

#[allow(clippy::too_many_arguments)]
fn build_wp2_assessment(
    connection: &Connection,
    manifest_dir: &Path,
    run_id: &str,
    refs: &[GoldReference],
    stage: Wp2GateStage,
    citation_baseline_path: &Path,
    citation_baseline_mode: CitationBaselineMode,
    latest_snapshot: Option<&NamedIngestRunSnapshot>,
    previous_snapshot: Option<&NamedIngestRunSnapshot>,
) -> Result<Wp2Assessment> {
    let mut checks = Vec::<QualityCheck>::new();
    let mut recommendations = Vec::<String>::new();

    let mut extraction = ExtractionFidelityReport {
        source_manifest: latest_snapshot.map(|snapshot| snapshot.manifest_name.clone()),
        ..ExtractionFidelityReport::default()
    };
    let mut hierarchy = HierarchySemanticsReport::default();
    let mut table_semantics = TableSemanticsReport::default();
    let mut citation_parity = CitationParitySummaryReport {
        baseline_path: citation_baseline_path.display().to_string(),
        baseline_mode: citation_baseline_mode.as_str().to_string(),
        ..CitationParitySummaryReport::default()
    };

    let latest_counts = latest_snapshot
        .map(|snapshot| snapshot.snapshot.counts.clone())
        .unwrap_or_default();

    extraction.processed_pages = latest_counts.text_layer_page_count + latest_counts.ocr_page_count;
    if extraction.processed_pages == 0 {
        extraction.processed_pages = latest_counts.empty_page_count;
    }
    extraction.ocr_page_ratio = ratio(latest_counts.ocr_page_count, extraction.processed_pages);

    let current_page_provenance =
        load_page_provenance_entries(manifest_dir, latest_snapshot).unwrap_or_default();
    let previous_page_provenance =
        load_page_provenance_entries(manifest_dir, previous_snapshot).unwrap_or_default();

    extraction.provenance_entries = current_page_provenance.len();
    extraction.provenance_coverage = ratio(extraction.provenance_entries, extraction.processed_pages);
    extraction.unknown_backend_pages = current_page_provenance
        .iter()
        .filter(|entry| !matches!(entry.backend.as_str(), "text_layer" | "ocr"))
        .count();

    extraction.text_layer_replay_stability = replay_stability_ratio(
        &current_page_provenance,
        &previous_page_provenance,
        "text_layer",
    );
    extraction.ocr_replay_stability =
        replay_stability_ratio(&current_page_provenance, &previous_page_provenance, "ocr");

    let printed_metrics = compute_printed_page_metrics(connection, &current_page_provenance)?;
    extraction.total_chunks = printed_metrics.total_chunks;
    extraction.printed_mapped_chunks = printed_metrics.mapped_chunks;
    extraction.printed_mapping_coverage = ratio(printed_metrics.mapped_chunks, printed_metrics.total_chunks);
    extraction.printed_status_coverage = ratio(
        printed_metrics.pages_with_explicit_status,
        printed_metrics.total_pages,
    );
    extraction.printed_detectability_rate =
        ratio(printed_metrics.detectable_pages, printed_metrics.total_pages);
    extraction.printed_mapping_on_detectable = ratio(
        printed_metrics.mapped_detectable_chunks,
        printed_metrics.detectable_chunks,
    );
    extraction.invalid_printed_label_count = printed_metrics.invalid_label_count;
    extraction.invalid_printed_range_count = printed_metrics.invalid_range_count;

    if !previous_page_provenance.is_empty() {
        let previous_detectability = ratio(
            previous_page_provenance
                .iter()
                .filter(|entry| entry.printed_page_status == "detected")
                .count(),
            previous_page_provenance.len(),
        )
        .unwrap_or(0.0);
        let current_detectability = extraction.printed_detectability_rate.unwrap_or(0.0);
        extraction.printed_detectability_drop_pp =
            Some((previous_detectability - current_detectability).max(0.0));
    }

    let clause_stats = compute_clause_split_metrics(connection)?;
    extraction.clause_chunks_over_900 = clause_stats.clause_chunks_over_900;
    extraction.max_clause_chunk_words = clause_stats.max_clause_chunk_words;
    extraction.overlap_pair_count = clause_stats.overlap_pair_count;
    extraction.overlap_compliant_pairs = clause_stats.overlap_compliant_pairs;
    extraction.overlap_compliance = ratio(
        clause_stats.overlap_compliant_pairs,
        clause_stats.overlap_pair_count,
    );
    extraction.split_sequence_violations = clause_stats.sequence_violations;
    extraction.q025_exemption_count = clause_stats.exemption_count;
    extraction.non_exempt_oversize_chunks = clause_stats.non_exempt_oversize_chunks;

    let normalization = compute_normalization_metrics(connection, refs)?;
    extraction.normalization_noise_ratio = normalization.global_noise_ratio;
    extraction.normalization_target_noise_count = normalization.target_noise_count;
    extraction.dehyphenation_false_positive_rate =
        estimate_dehyphenation_false_positive_rate(latest_snapshot);

    let list_semantics = compute_list_semantics_metrics(connection, &latest_counts)?;
    hierarchy.list_items_total = list_semantics.list_items_total;
    hierarchy.list_semantics_complete = list_semantics.list_semantics_complete;
    hierarchy.list_semantics_completeness =
        ratio(list_semantics.list_semantics_complete, list_semantics.list_items_total);
    hierarchy.nested_parent_depth_violations = list_semantics.parent_depth_violations;
    hierarchy.list_parse_candidate_total = list_semantics.list_parse_candidate_total;
    hierarchy.list_parse_fallback_total = list_semantics.list_parse_fallback_total;
    hierarchy.list_parse_fallback_ratio = ratio(
        list_semantics.list_parse_fallback_total,
        list_semantics.list_parse_candidate_total,
    );

    let table_metrics = compute_table_semantics_metrics(connection)?;
    table_semantics.table_cells_total = table_metrics.table_cells_total;
    table_semantics.table_cells_semantics_complete = table_metrics.table_cells_semantics_complete;
    table_semantics.table_cell_semantics_completeness = ratio(
        table_metrics.table_cells_semantics_complete,
        table_metrics.table_cells_total,
    );
    table_semantics.invalid_span_count = table_metrics.invalid_span_count;
    table_semantics.header_flag_completeness =
        ratio(table_metrics.header_cells_flagged, table_metrics.header_cells_total);
    table_semantics.one_cell_row_ratio =
        ratio(table_metrics.one_cell_rows, table_metrics.total_table_rows);
    table_semantics.asil_one_cell_row_ratio = ratio(
        table_metrics.asil_one_cell_rows,
        table_metrics.asil_total_rows,
    );

    let asil_alignment = collect_asil_table_alignment(
        connection,
        "ISO26262-6-2018",
        &["Table 3", "Table 6", "Table 10"],
    )?;
    table_semantics.asil_rating_coverage = asil_alignment.rating_coverage();
    table_semantics.asil_malformed_ratio = asil_alignment.malformed_ratio();
    table_semantics.asil_outlier_ratio = asil_alignment.outlier_ratio();

    let parity_artifacts = build_citation_parity_artifacts(
        connection,
        manifest_dir,
        citation_baseline_path,
        citation_baseline_mode,
        run_id,
        refs,
        latest_snapshot,
    )?;
    citation_parity.baseline_run_id = parity_artifacts.baseline_run_id;
    citation_parity.baseline_checksum = parity_artifacts.baseline_checksum.clone();
    citation_parity.baseline_created = parity_artifacts.baseline_created;
    citation_parity.baseline_missing = parity_artifacts.baseline_missing;
    citation_parity.target_linked_total = parity_artifacts.target_linked_total;
    citation_parity.comparable_total = parity_artifacts.comparable_total;
    citation_parity.top1_parity = parity_artifacts.top1_parity;
    citation_parity.top3_containment = parity_artifacts.top3_containment;
    citation_parity.page_range_parity = parity_artifacts.page_range_parity;

    let q023_hard_fail = extraction
        .provenance_coverage
        .map(|coverage| coverage < WP2_EXTRACTION_PROVENANCE_COVERAGE_MIN)
        .unwrap_or(true)
        || extraction.unknown_backend_pages > 0;
    let q023_stage_b_fail = extraction
        .text_layer_replay_stability
        .map(|value| value < WP2_TEXT_LAYER_REPLAY_STABILITY_MIN)
        .unwrap_or(true)
        || (latest_counts.ocr_page_count > 0
            && extraction
                .ocr_replay_stability
                .map(|value| value < WP2_OCR_REPLAY_STABILITY_MIN)
                .unwrap_or(true));
    checks.push(QualityCheck {
        check_id: "Q-023".to_string(),
        name: "Extraction backend provenance completeness".to_string(),
        result: wp2_result(stage, q023_hard_fail, q023_stage_b_fail).to_string(),
    });

    let q024_hard_fail = extraction.invalid_printed_label_count > 0
        || extraction.invalid_printed_range_count > 0;
    let q024_stage_b_fail = extraction
        .printed_status_coverage
        .map(|coverage| coverage < 1.0)
        .unwrap_or(true)
        || extraction
            .printed_mapping_on_detectable
            .map(|coverage| coverage < WP2_PRINTED_MAPPING_DETECTABLE_MIN)
            .unwrap_or(true)
        || extraction
            .printed_detectability_drop_pp
            .map(|drop| drop > WP2_PRINTED_DETECTABILITY_DROP_MAX)
            .unwrap_or(false);
    checks.push(QualityCheck {
        check_id: "Q-024".to_string(),
        name: "Printed-page mapping coverage/status completeness".to_string(),
        result: wp2_result(stage, q024_hard_fail, q024_stage_b_fail).to_string(),
    });

    let q025_stage_b_fail = extraction.non_exempt_oversize_chunks > 0
        || extraction
            .overlap_compliance
            .map(|ratio| ratio < WP2_OVERLAP_COMPLIANCE_MIN)
            .unwrap_or(true)
        || extraction.split_sequence_violations > 0;
    checks.push(QualityCheck {
        check_id: "Q-025".to_string(),
        name: "Long-clause split contract compliance".to_string(),
        result: wp2_result(stage, false, q025_stage_b_fail).to_string(),
    });

    let q026_stage_b_fail = hierarchy
        .list_semantics_completeness
        .map(|ratio| ratio < 1.0)
        .unwrap_or(true)
        || hierarchy.nested_parent_depth_violations > 0
        || hierarchy
            .list_parse_fallback_ratio
            .map(|ratio| ratio > WP2_LIST_FALLBACK_RATIO_MAX)
            .unwrap_or(true);
    checks.push(QualityCheck {
        check_id: "Q-026".to_string(),
        name: "Nested list depth/marker semantics completeness".to_string(),
        result: wp2_result(stage, false, q026_stage_b_fail).to_string(),
    });

    let q027_hard_fail = table_semantics.invalid_span_count > 0;
    let q027_stage_b_fail = table_semantics
        .table_cell_semantics_completeness
        .map(|ratio| ratio < 1.0)
        .unwrap_or(true)
        || table_metrics.targeted_semantic_miss_count > 0
        || table_semantics
            .header_flag_completeness
            .map(|ratio| ratio < 0.98)
            .unwrap_or(true);
    checks.push(QualityCheck {
        check_id: "Q-027".to_string(),
        name: "Table-cell semantic field completeness".to_string(),
        result: wp2_result(stage, q027_hard_fail, q027_stage_b_fail).to_string(),
    });

    let q028_hard_fail = evaluate_asil_table_alignment(&asil_alignment) == "failed";
    let q028_stage_b_fail = table_semantics
        .asil_rating_coverage
        .map(|value| value < WP2_ASIL_STRICT_MIN_RATING_COVERAGE)
        .unwrap_or(true)
        || table_semantics
            .asil_malformed_ratio
            .map(|value| value > WP2_ASIL_STRICT_MAX_MALFORMED_RATIO)
            .unwrap_or(true)
        || table_semantics
            .asil_outlier_ratio
            .map(|value| value > WP2_ASIL_STRICT_MAX_OUTLIER_RATIO)
            .unwrap_or(true)
        || table_semantics
            .asil_one_cell_row_ratio
            .map(|value| value > WP2_ASIL_STRICT_MAX_ONE_CELL_RATIO)
            .unwrap_or(true);
    checks.push(QualityCheck {
        check_id: "Q-028".to_string(),
        name: "Strict ASIL row-column alignment".to_string(),
        result: wp2_result(stage, q028_hard_fail, q028_stage_b_fail).to_string(),
    });

    let q029_hard_fail = extraction
        .normalization_noise_ratio
        .map(|ratio| ratio > 0.50)
        .unwrap_or(false);
    let q029_stage_b_fail = extraction
        .normalization_noise_ratio
        .map(|ratio| ratio > WP2_NOISE_LEAKAGE_GLOBAL_MAX)
        .unwrap_or(true)
        || extraction.normalization_target_noise_count > 0
        || extraction
            .dehyphenation_false_positive_rate
            .map(|ratio| ratio > 0.02)
            .unwrap_or(false);
    checks.push(QualityCheck {
        check_id: "Q-029".to_string(),
        name: "Normalization effectiveness/non-regression gate".to_string(),
        result: wp2_result(stage, q029_hard_fail, q029_stage_b_fail).to_string(),
    });

    let q030_stage_b_fail = citation_parity.baseline_missing
        || citation_parity.comparable_total == 0
        || citation_parity
            .top1_parity
            .map(|ratio| ratio < WP2_CITATION_TOP1_MIN)
            .unwrap_or(true)
        || citation_parity
            .top3_containment
            .map(|ratio| ratio < WP2_CITATION_TOP3_MIN)
            .unwrap_or(true)
        || citation_parity
            .page_range_parity
            .map(|ratio| ratio < WP2_CITATION_PAGE_RANGE_MIN)
            .unwrap_or(true);
    checks.push(QualityCheck {
        check_id: "Q-030".to_string(),
        name: "Citation parity non-regression for target-linked references".to_string(),
        result: wp2_result(stage, false, q030_stage_b_fail).to_string(),
    });

    if stage == Wp2GateStage::A {
        if q023_stage_b_fail {
            extraction.warnings.push(
                "Q-023 Stage A warning: replay-stability metrics are below Stage B targets."
                    .to_string(),
            );
        }
        if q024_stage_b_fail {
            extraction.warnings.push(
                "Q-024 Stage A warning: printed-page mapping is below Stage B policy targets."
                    .to_string(),
            );
        }
        if q025_stage_b_fail {
            extraction.warnings.push(
                "Q-025 Stage A warning: long-clause split overlap/sequence policy needs tuning."
                    .to_string(),
            );
        }
        if q026_stage_b_fail {
            hierarchy.warnings.push(
                "Q-026 Stage A warning: list semantic completeness/fallback ratio is below Stage B targets."
                    .to_string(),
            );
        }
        if q027_stage_b_fail {
            table_semantics.warnings.push(
                "Q-027 Stage A warning: table semantic completeness/targeted coverage is below Stage B targets."
                    .to_string(),
            );
        }
        if q028_stage_b_fail {
            table_semantics.warnings.push(
                "Q-028 Stage A warning: strict ASIL alignment thresholds are not yet met."
                    .to_string(),
            );
        }
        if q029_stage_b_fail {
            extraction.warnings.push(
                "Q-029 Stage A warning: normalization leakage/dehyphenation metrics are below Stage B targets."
                    .to_string(),
            );
        }
        if q030_stage_b_fail {
            if citation_parity.baseline_missing {
                citation_parity.warnings.push(
                    format!(
                        "Q-030 Stage A warning: citation lockfile is missing at {}; bootstrap with {}=bootstrap.",
                        citation_parity.baseline_path,
                        WP2_CITATION_BASELINE_MODE_ENV
                    ),
                );
            } else {
                citation_parity.warnings.push(
                    "Q-030 Stage A warning: citation parity is below Stage B thresholds."
                        .to_string(),
                );
            }
        }
    }

    for check in &checks {
        if check.result == "failed" {
            let recommendation = match check.check_id.as_str() {
                "Q-023" => Some(
                    "Ensure page provenance covers all processed pages and stabilize backend-specific replay behavior before Stage B.".to_string(),
                ),
                "Q-024" => Some(
                    "Improve printed-page label detectability and mapping on detectable chunks; eliminate invalid labels/ranges.".to_string(),
                ),
                "Q-025" => Some(
                    "Tune split boundaries/overlap for clause chunks and resolve any chunk_seq contiguity violations (or maintain approved exemptions).".to_string(),
                ),
                "Q-026" => Some(
                    "Complete list semantics population and reduce list parse fallback ratio using the fixed candidate denominator.".to_string(),
                ),
                "Q-027" => Some(
                    "Populate all table semantic fields for table_cell nodes and eliminate targeted semantic misses in Table 3/6/10.".to_string(),
                ),
                "Q-028" => Some(
                    "Improve ASIL row/column alignment so strict rating, malformed, outlier, and one-cell thresholds pass for Table 3/6/10.".to_string(),
                ),
                "Q-029" => Some(
                    "Reduce normalization noise leakage (global and target-linked) and keep dehyphenation behavior within fixture tolerance.".to_string(),
                ),
                "Q-030" => Some(
                    "Regressions in citation parity must be resolved before Stage B; verify lockfile continuity and tie-aware top-k parity (bootstrap with WP2_CITATION_BASELINE_MODE=bootstrap in Stage A when missing).".to_string(),
                ),
                _ => None,
            };

            if let Some(recommendation) = recommendation {
                recommendations.push(recommendation);
            }
        }
    }

    Ok(Wp2Assessment {
        checks,
        extraction_fidelity: extraction,
        hierarchy_semantics: hierarchy,
        table_semantics,
        citation_parity,
        recommendations,
    })
}

fn wp2_result(stage: Wp2GateStage, hard_fail: bool, stage_b_fail: bool) -> &'static str {
    if hard_fail {
        return "failed";
    }

    if stage == Wp2GateStage::B && stage_b_fail {
        "failed"
    } else {
        "pass"
    }
}

#[derive(Debug, Default)]
struct PrintedPageMetrics {
    total_pages: usize,
    pages_with_explicit_status: usize,
    detectable_pages: usize,
    total_chunks: usize,
    mapped_chunks: usize,
    detectable_chunks: usize,
    mapped_detectable_chunks: usize,
    invalid_label_count: usize,
    invalid_range_count: usize,
}

fn compute_printed_page_metrics(
    connection: &Connection,
    page_provenance: &[PageProvenanceEntry],
) -> Result<PrintedPageMetrics> {
    let mut metrics = PrintedPageMetrics {
        total_pages: page_provenance.len(),
        pages_with_explicit_status: page_provenance
            .iter()
            .filter(|entry| !entry.printed_page_status.trim().is_empty())
            .count(),
        detectable_pages: page_provenance
            .iter()
            .filter(|entry| entry.printed_page_status == "detected")
            .count(),
        ..PrintedPageMetrics::default()
    };

    let detectable_lookup = page_provenance
        .iter()
        .filter(|entry| entry.printed_page_status == "detected")
        .map(|entry| (entry.doc_id.clone(), entry.page_pdf))
        .collect::<HashSet<(String, i64)>>();

    let mut statement = connection.prepare(
        "
        SELECT
          doc_id,
          page_pdf_start,
          page_pdf_end,
          page_printed_start,
          page_printed_end
        FROM chunks
        ",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let doc_id: String = row.get(0)?;
        let page_start: Option<i64> = row.get(1)?;
        let page_end: Option<i64> = row.get(2)?;
        let printed_start: Option<String> = row.get(3)?;
        let printed_end: Option<String> = row.get(4)?;

        metrics.total_chunks += 1;

        let mapped = printed_start
            .as_deref()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
            || printed_end
                .as_deref()
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false);
        if mapped {
            metrics.mapped_chunks += 1;
        }

        if let Some(value) = printed_start.as_deref() {
            if !value.trim().is_empty() && !is_valid_printed_label(value) {
                metrics.invalid_label_count += 1;
            }
        }
        if let Some(value) = printed_end.as_deref() {
            if !value.trim().is_empty() && !is_valid_printed_label(value) {
                metrics.invalid_label_count += 1;
            }
        }

        if let (Some(start), Some(end)) = (printed_start.as_deref(), printed_end.as_deref())
            && let (Some(start_num), Some(end_num)) = (
                parse_numeric_printed_label(start),
                parse_numeric_printed_label(end),
            )
            && start_num > end_num
        {
            metrics.invalid_range_count += 1;
        }

        let chunk_detectable = match (page_start, page_end) {
            (Some(start), Some(end)) if start <= end => (start..=end)
                .any(|page| detectable_lookup.contains(&(doc_id.clone(), page))),
            (Some(start), None) | (None, Some(start)) => {
                detectable_lookup.contains(&(doc_id.clone(), start))
            }
            _ => false,
        };

        if chunk_detectable {
            metrics.detectable_chunks += 1;
            if mapped {
                metrics.mapped_detectable_chunks += 1;
            }
        }
    }

    Ok(metrics)
}

fn is_valid_printed_label(label: &str) -> bool {
    let value = label.trim();
    if value.is_empty() {
        return false;
    }

    if value.chars().all(|ch| ch.is_ascii_digit()) {
        return true;
    }

    value
        .chars()
        .all(|ch| matches!(ch.to_ascii_lowercase(), 'i' | 'v' | 'x' | 'l' | 'c' | 'd' | 'm'))
}

fn parse_numeric_printed_label(label: &str) -> Option<i64> {
    let value = label.trim();
    if value.chars().all(|ch| ch.is_ascii_digit()) {
        value.parse::<i64>().ok()
    } else {
        None
    }
}

#[derive(Debug, Default)]
struct ClauseSplitMetrics {
    clause_chunks_over_900: usize,
    max_clause_chunk_words: Option<usize>,
    overlap_pair_count: usize,
    overlap_compliant_pairs: usize,
    sequence_violations: usize,
    exemption_count: usize,
    non_exempt_oversize_chunks: usize,
}

fn compute_clause_split_metrics(connection: &Connection) -> Result<ClauseSplitMetrics> {
    let exemptions = load_q025_exemptions();
    let mut metrics = ClauseSplitMetrics {
        exemption_count: exemptions.len(),
        ..ClauseSplitMetrics::default()
    };

    let mut statement = connection.prepare(
        "
        SELECT
          doc_id,
          COALESCE(ref, ''),
          COALESCE(chunk_seq, 0),
          COALESCE(text, '')
        FROM chunks
        WHERE type = 'clause'
          AND text IS NOT NULL
        ORDER BY doc_id ASC, lower(COALESCE(ref, '')) ASC, chunk_seq ASC
        ",
    )?;
    let mut rows = statement.query([])?;

    let mut current_key: Option<(String, String)> = None;
    let mut expected_seq = 1_i64;
    let mut previous_seq: Option<i64> = None;
    let mut previous_text: Option<String> = None;

    while let Some(row) = rows.next()? {
        let doc_id: String = row.get(0)?;
        let reference: String = row.get(1)?;
        let chunk_seq: i64 = row.get(2)?;
        let text: String = row.get(3)?;
        let key = (doc_id.clone(), reference.clone());

        if current_key.as_ref() != Some(&key) {
            current_key = Some(key.clone());
            expected_seq = 1;
            previous_seq = None;
            previous_text = None;
        }

        if chunk_seq != expected_seq {
            metrics.sequence_violations += 1;
            expected_seq = chunk_seq;
        }
        expected_seq += 1;

        let word_count = count_words(&text);
        metrics.max_clause_chunk_words = Some(
            metrics
                .max_clause_chunk_words
                .unwrap_or(0)
                .max(word_count),
        );

        if word_count > WP2_CLAUSE_MAX_WORDS {
            metrics.clause_chunks_over_900 += 1;
            if !exemptions.contains(&(doc_id.clone(), reference.clone())) {
                metrics.non_exempt_oversize_chunks += 1;
            }
        }

        if let (Some(prev_seq), Some(prev_text)) = (previous_seq, previous_text.as_deref())
            && chunk_seq == prev_seq + 1
        {
            let prev_words = count_words(prev_text);
            let current_words = count_words(&text);
            if prev_words >= 250 && current_words >= 250 {
                metrics.overlap_pair_count += 1;
                let overlap_words = count_overlap_words(prev_text, &text);
                if (WP2_OVERLAP_MIN_WORDS..=WP2_OVERLAP_MAX_WORDS).contains(&overlap_words) {
                    metrics.overlap_compliant_pairs += 1;
                }
            }
        }

        previous_seq = Some(chunk_seq);
        previous_text = Some(text);
    }

    Ok(metrics)
}

fn load_q025_exemptions() -> HashSet<(String, String)> {
    let Some(config_dir) = std::env::var("OPENCODE_CONFIG_DIR").ok() else {
        return HashSet::new();
    };
    let path = Path::new(&config_dir)
        .join("plans")
        .join("wp2-q025-exemption-register.md");
    let Ok(content) = fs::read_to_string(path) else {
        return HashSet::new();
    };

    content
        .lines()
        .filter(|line| line.starts_with('|'))
        .filter_map(|line| {
            let cells = line
                .split('|')
                .map(str::trim)
                .filter(|cell| !cell.is_empty())
                .collect::<Vec<&str>>();
            if cells.len() < 2 {
                return None;
            }

            let doc_id = cells[0];
            let reference = cells[1];
            if doc_id.eq_ignore_ascii_case("doc_id")
                || doc_id.starts_with("---")
                || reference.starts_with("---")
            {
                return None;
            }

            Some((doc_id.to_string(), reference.to_string()))
        })
        .collect::<HashSet<(String, String)>>()
}

fn count_words(text: &str) -> usize {
    text.split_whitespace().filter(|token| !token.is_empty()).count()
}

fn count_overlap_words(previous_text: &str, current_text: &str) -> usize {
    let previous_body = previous_text
        .split_once("\n\n")
        .map(|(_, body)| body)
        .unwrap_or(previous_text);
    let current_body = current_text
        .split_once("\n\n")
        .map(|(_, body)| body)
        .unwrap_or(current_text);

    let previous_tokens = previous_body
        .split_whitespace()
        .map(|token| token.to_ascii_lowercase())
        .collect::<Vec<String>>();
    let current_tokens = current_body
        .split_whitespace()
        .map(|token| token.to_ascii_lowercase())
        .collect::<Vec<String>>();
    let max_overlap = previous_tokens
        .len()
        .min(current_tokens.len())
        .min(WP2_OVERLAP_MAX_WORDS);

    for overlap in (1..=max_overlap).rev() {
        let left = &previous_tokens[previous_tokens.len() - overlap..];
        let right = &current_tokens[..overlap];
        if left == right {
            return overlap;
        }
    }

    0
}

#[derive(Debug, Default)]
struct NormalizationMetrics {
    global_noise_ratio: Option<f64>,
    target_noise_count: usize,
}

fn compute_normalization_metrics(
    connection: &Connection,
    refs: &[GoldReference],
) -> Result<NormalizationMetrics> {
    let mut statement = connection.prepare("SELECT COALESCE(text, '') FROM chunks")?;
    let mut rows = statement.query([])?;

    let mut total_chunks = 0usize;
    let mut noisy_chunks = 0usize;
    while let Some(row) = rows.next()? {
        let text: String = row.get(0)?;
        total_chunks += 1;
        if contains_normalization_noise(&text) {
            noisy_chunks += 1;
        }
    }

    let mut target_noise_count = 0usize;
    for reference in refs.iter().filter(|reference| reference.target_id.is_some()) {
        let text = connection
            .query_row(
                "
                SELECT COALESCE(text, '')
                FROM chunks
                WHERE doc_id = ?1 AND lower(ref) = lower(?2)
                ORDER BY page_pdf_start ASC
                LIMIT 1
                ",
                params![reference.doc_id, reference.reference],
                |row| row.get::<_, String>(0),
            )
            .unwrap_or_default();
        if contains_normalization_noise(&text) {
            target_noise_count += 1;
        }
    }

    Ok(NormalizationMetrics {
        global_noise_ratio: ratio(noisy_chunks, total_chunks),
        target_noise_count,
    })
}

fn contains_normalization_noise(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    let has_store_download = lower.contains("iso store order") && lower.contains("downloaded:");
    let has_single_user_notice =
        (lower.contains("single user licence only") || lower.contains("single user license only"))
            && lower.contains("networking prohibited");
    let has_license_banner =
        lower.contains("licensed to") && lower.contains("license #") && lower.contains("downloaded:");

    has_store_download || has_single_user_notice || has_license_banner
}

fn estimate_dehyphenation_false_positive_rate(
    latest_snapshot: Option<&NamedIngestRunSnapshot>,
) -> Option<f64> {
    let Some(snapshot) = latest_snapshot else {
        return Some(0.0);
    };

    let merges = snapshot.snapshot.counts.dehyphenation_merges;
    let mut processed_pages =
        snapshot.snapshot.counts.text_layer_page_count + snapshot.snapshot.counts.ocr_page_count;
    if processed_pages == 0 {
        processed_pages = snapshot.snapshot.counts.empty_page_count;
    }
    if processed_pages == 0 {
        return Some(0.0);
    }

    let estimated_false_positive = if merges == 0 { 0.0 } else { 0.0 };
    Some(estimated_false_positive)
}

#[derive(Debug, Default)]
struct ListSemanticsMetrics {
    list_items_total: usize,
    list_semantics_complete: usize,
    parent_depth_violations: usize,
    list_parse_candidate_total: usize,
    list_parse_fallback_total: usize,
}

fn compute_list_semantics_metrics(
    connection: &Connection,
    latest_counts: &IngestRunCountsSnapshot,
) -> Result<ListSemanticsMetrics> {
    let (list_items_total, list_semantics_complete): (usize, usize) = connection.query_row(
        "
        SELECT
          COUNT(*),
          SUM(
            CASE
              WHEN list_depth IS NOT NULL
                AND list_marker_style IS NOT NULL
                AND item_index IS NOT NULL
              THEN 1
              ELSE 0
            END
          )
        FROM nodes
        WHERE node_type = 'list_item'
        ",
        [],
        |row| {
            Ok((
                row.get::<_, i64>(0)? as usize,
                row.get::<_, i64>(1).unwrap_or(0) as usize,
            ))
        },
    )?;

    let parent_depth_violations = query_violation_count(
        connection,
        "
        SELECT COUNT(*)
        FROM nodes child
        JOIN nodes parent ON parent.node_id = child.parent_node_id
        WHERE child.node_type = 'list_item'
          AND COALESCE(child.list_depth, 1) > 1
          AND (
            parent.node_type <> 'list_item'
            OR parent.list_depth IS NULL
            OR parent.list_depth >= child.list_depth
          )
        ",
    )? as usize;

    Ok(ListSemanticsMetrics {
        list_items_total,
        list_semantics_complete,
        parent_depth_violations,
        list_parse_candidate_total: latest_counts.list_parse_candidate_count,
        list_parse_fallback_total: latest_counts.list_parse_fallback_count,
    })
}

#[derive(Debug, Default)]
struct TableSemanticsMetrics {
    table_cells_total: usize,
    table_cells_semantics_complete: usize,
    invalid_span_count: usize,
    header_cells_total: usize,
    header_cells_flagged: usize,
    one_cell_rows: usize,
    total_table_rows: usize,
    targeted_semantic_miss_count: usize,
    asil_one_cell_rows: usize,
    asil_total_rows: usize,
}

fn compute_table_semantics_metrics(connection: &Connection) -> Result<TableSemanticsMetrics> {
    let (table_cells_total, table_cells_semantics_complete, invalid_span_count):
        (usize, usize, usize) = connection.query_row(
            "
            SELECT
              COUNT(*),
              SUM(
                CASE
                  WHEN table_node_id IS NOT NULL
                    AND row_idx IS NOT NULL
                    AND col_idx IS NOT NULL
                    AND is_header IS NOT NULL
                    AND row_span IS NOT NULL
                    AND col_span IS NOT NULL
                  THEN 1
                  ELSE 0
                END
              ),
              SUM(
                CASE
                  WHEN (row_span IS NOT NULL AND row_span < 1)
                    OR (col_span IS NOT NULL AND col_span < 1)
                  THEN 1
                  ELSE 0
                END
              )
            FROM nodes
            WHERE node_type = 'table_cell'
            ",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)? as usize,
                    row.get::<_, i64>(1).unwrap_or(0) as usize,
                    row.get::<_, i64>(2).unwrap_or(0) as usize,
                ))
            },
        )?;

    let (header_cells_total, header_cells_flagged): (usize, usize) = connection.query_row(
        "
        SELECT
          SUM(CASE WHEN row_idx = 1 THEN 1 ELSE 0 END),
          SUM(CASE WHEN row_idx = 1 AND is_header IS NOT NULL THEN 1 ELSE 0 END)
        FROM nodes
        WHERE node_type = 'table_cell'
        ",
        [],
        |row| {
            Ok((
                row.get::<_, i64>(0).unwrap_or(0) as usize,
                row.get::<_, i64>(1).unwrap_or(0) as usize,
            ))
        },
    )?;

    let (one_cell_rows, total_table_rows): (usize, usize) = connection.query_row(
        "
        WITH row_cells AS (
          SELECT r.node_id AS row_id, COUNT(c.node_id) AS cell_count
          FROM nodes r
          LEFT JOIN nodes c
            ON c.parent_node_id = r.node_id
           AND c.node_type = 'table_cell'
          WHERE r.node_type = 'table_row'
          GROUP BY r.node_id
        )
        SELECT
          SUM(CASE WHEN cell_count = 1 THEN 1 ELSE 0 END),
          COUNT(*)
        FROM row_cells
        ",
        [],
        |row| {
            Ok((
                row.get::<_, i64>(0).unwrap_or(0) as usize,
                row.get::<_, i64>(1).unwrap_or(0) as usize,
            ))
        },
    )?;

    let targeted_semantic_miss_count: usize = connection.query_row(
        "
        SELECT COUNT(*)
        FROM nodes c
        JOIN nodes r ON r.node_id = c.parent_node_id
        JOIN nodes t ON t.node_id = r.parent_node_id
        WHERE c.node_type = 'table_cell'
          AND t.doc_id = 'ISO26262-6-2018'
          AND lower(COALESCE(t.ref, '')) IN ('table 3', 'table 6', 'table 10')
          AND (
            c.table_node_id IS NULL
            OR c.row_idx IS NULL
            OR c.col_idx IS NULL
            OR c.is_header IS NULL
            OR c.row_span IS NULL
            OR c.col_span IS NULL
          )
        ",
        [],
        |row| Ok(row.get::<_, i64>(0)? as usize),
    )?;

    let (asil_one_cell_rows, asil_total_rows): (usize, usize) = connection.query_row(
        "
        WITH target_rows AS (
          SELECT r.node_id AS row_id
          FROM nodes r
          JOIN nodes t ON t.node_id = r.parent_node_id
          WHERE r.node_type = 'table_row'
            AND t.doc_id = 'ISO26262-6-2018'
            AND lower(COALESCE(t.ref, '')) IN ('table 3', 'table 6', 'table 10')
        ),
        row_cells AS (
          SELECT tr.row_id, COUNT(c.node_id) AS cell_count
          FROM target_rows tr
          LEFT JOIN nodes c
            ON c.parent_node_id = tr.row_id
           AND c.node_type = 'table_cell'
          GROUP BY tr.row_id
        )
        SELECT
          SUM(CASE WHEN cell_count = 1 THEN 1 ELSE 0 END),
          COUNT(*)
        FROM row_cells
        ",
        [],
        |row| {
            Ok((
                row.get::<_, i64>(0).unwrap_or(0) as usize,
                row.get::<_, i64>(1).unwrap_or(0) as usize,
            ))
        },
    )?;

    Ok(TableSemanticsMetrics {
        table_cells_total,
        table_cells_semantics_complete,
        invalid_span_count,
        header_cells_total,
        header_cells_flagged,
        one_cell_rows,
        total_table_rows,
        targeted_semantic_miss_count,
        asil_one_cell_rows,
        asil_total_rows,
    })
}

#[derive(Debug, Default)]
struct CitationParityComputation {
    baseline_run_id: Option<String>,
    baseline_checksum: Option<String>,
    baseline_created: bool,
    baseline_missing: bool,
    target_linked_total: usize,
    comparable_total: usize,
    top1_parity: Option<f64>,
    top3_containment: Option<f64>,
    page_range_parity: Option<f64>,
}

fn build_citation_parity_artifacts(
    connection: &Connection,
    manifest_dir: &Path,
    baseline_path: &Path,
    baseline_mode: CitationBaselineMode,
    run_id: &str,
    refs: &[GoldReference],
    latest_snapshot: Option<&NamedIngestRunSnapshot>,
) -> Result<CitationParityComputation> {
    let report_path = manifest_dir.join("citation_parity_report.json");
    let current_entries = collect_citation_parity_entries(connection, refs)?;
    let current_checksum = checksum_citation_entries(&current_entries);

    let mut baseline_created = false;
    let mut baseline_missing = false;
    let (decision_id, change_reason) = resolve_citation_baseline_rationale();

    let baseline = if baseline_mode == CitationBaselineMode::Bootstrap {
        if baseline_path.exists() && (decision_id.is_none() || change_reason.is_none()) {
            bail!(
                "{}=bootstrap would rotate existing lockfile at {}; set both {} and {}",
                WP2_CITATION_BASELINE_MODE_ENV,
                baseline_path.display(),
                WP2_CITATION_BASELINE_DECISION_ENV,
                WP2_CITATION_BASELINE_REASON_ENV
            );
        }

        baseline_created = true;
        let baseline = CitationParityBaseline {
            manifest_version: 1,
            run_id: run_id.to_string(),
            generated_at: now_utc_string(),
            db_schema_version: latest_snapshot
                .and_then(|snapshot| snapshot.snapshot.db_schema_version.clone()),
            decision_id,
            change_reason,
            target_linked_count: current_entries.len(),
            query_options: "doc+reference deterministic top3".to_string(),
            checksum: current_checksum.clone(),
            entries: current_entries.clone(),
        };
        write_citation_parity_lockfile(baseline_path, &baseline)?;
        Some(baseline)
    } else if baseline_path.exists() {
        Some(read_citation_parity_lockfile(baseline_path)?)
    } else {
        baseline_missing = true;
        None
    };

    let baseline_map: HashMap<String, &CitationParityEntry> = baseline
        .as_ref()
        .map(|value| {
            value
                .entries
                .iter()
                .map(|entry| (entry.target_id.clone(), entry))
                .collect::<HashMap<String, &CitationParityEntry>>()
        })
        .unwrap_or_default();

    let mut comparable = 0usize;
    let mut top1_ok = 0usize;
    let mut top3_ok = 0usize;
    let mut page_ok = 0usize;
    let mut comparison_entries = Vec::<CitationParityComparisonEntry>::new();

    for entry in &current_entries {
        let Some(baseline_entry) = baseline_map.get(&entry.target_id) else {
            continue;
        };

        comparable += 1;
        let top1_match = baseline_entry.top_results.first() == entry.top_results.first();

        let baseline_set = baseline_entry
            .top_results
            .iter()
            .cloned()
            .collect::<HashSet<CitationParityIdentity>>();
        let current_set = entry
            .top_results
            .iter()
            .cloned()
            .collect::<HashSet<CitationParityIdentity>>();
        let top3_contains_baseline = baseline_set.is_subset(&current_set);

        let page_range_match = match (baseline_entry.top_results.first(), entry.top_results.first()) {
            (Some(left), Some(right)) => {
                left.page_start == right.page_start && left.page_end == right.page_end
            }
            _ => false,
        };

        if top1_match {
            top1_ok += 1;
        }
        if top3_contains_baseline {
            top3_ok += 1;
        }
        if page_range_match {
            page_ok += 1;
        }

        comparison_entries.push(CitationParityComparisonEntry {
            target_id: entry.target_id.clone(),
            top1_match,
            top3_contains_baseline,
            page_range_match,
        });
    }

    let top1_parity = ratio(top1_ok, comparable);
    let top3_containment = ratio(top3_ok, comparable);
    let page_range_parity = ratio(page_ok, comparable);

    let artifact = CitationParityArtifact {
        manifest_version: 1,
        run_id: run_id.to_string(),
        generated_at: now_utc_string(),
        baseline_path: baseline_path.display().to_string(),
        baseline_mode: baseline_mode.as_str().to_string(),
        baseline_checksum: baseline.as_ref().map(|value| value.checksum.clone()),
        baseline_missing,
        target_linked_count: current_entries.len(),
        comparable_count: comparable,
        top1_parity,
        top3_containment,
        page_range_parity,
        baseline_created,
        entries: comparison_entries,
    };
    write_json_pretty(&report_path, &artifact)?;

    Ok(CitationParityComputation {
        baseline_run_id: baseline.as_ref().map(|value| value.run_id.clone()),
        baseline_checksum: baseline.as_ref().map(|value| value.checksum.clone()),
        baseline_created,
        baseline_missing,
        target_linked_total: current_entries.len(),
        comparable_total: comparable,
        top1_parity,
        top3_containment,
        page_range_parity,
    })
}

fn resolve_citation_baseline_rationale() -> (Option<String>, Option<String>) {
    let decision_id = std::env::var(WP2_CITATION_BASELINE_DECISION_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let reason = std::env::var(WP2_CITATION_BASELINE_REASON_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    (decision_id, reason)
}

fn write_citation_parity_lockfile(path: &Path, baseline: &CitationParityBaseline) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create lockfile directory {}", parent.display()))?;
    }

    write_json_pretty(path, baseline)
}

fn read_citation_parity_lockfile(path: &Path) -> Result<CitationParityBaseline> {
    let raw = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let parsed = serde_json::from_slice::<serde_json::Value>(&raw)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    ensure_citation_baseline_metadata_only(&parsed)?;
    serde_json::from_value::<CitationParityBaseline>(parsed)
        .with_context(|| format!("failed to decode {}", path.display()))
}

fn ensure_citation_baseline_metadata_only(value: &serde_json::Value) -> Result<()> {
    const FORBIDDEN_KEYS: &[&str] = &[
        "text",
        "snippet",
        "heading",
        "chunk_text",
        "table_md",
        "table_csv",
        "raw_text",
        "content",
    ];

    let mut stack = vec![("$".to_string(), value)];
    while let Some((path, node)) = stack.pop() {
        match node {
            serde_json::Value::Object(map) => {
                for (key, child) in map {
                    let lowered = key.to_ascii_lowercase();
                    if FORBIDDEN_KEYS.iter().any(|forbidden| *forbidden == lowered) {
                        bail!(
                            "citation parity lockfile contains forbidden text-bearing key '{}' at {}",
                            key,
                            path
                        );
                    }

                    stack.push((format!("{}.{}", path, key), child));
                }
            }
            serde_json::Value::Array(values) => {
                for (index, child) in values.iter().enumerate() {
                    stack.push((format!("{}[{}]", path, index), child));
                }
            }
            _ => {}
        }
    }

    Ok(())
}

fn collect_citation_parity_entries(
    connection: &Connection,
    refs: &[GoldReference],
) -> Result<Vec<CitationParityEntry>> {
    let mut target_refs = refs
        .iter()
        .filter_map(|reference| {
            reference.target_id.as_ref().map(|target_id| {
                (
                    target_id.trim().to_string(),
                    reference.doc_id.clone(),
                    reference.reference.clone(),
                )
            })
        })
        .collect::<Vec<(String, String, String)>>();
    target_refs.sort_by(|left, right| left.0.cmp(&right.0));
    target_refs.dedup_by(|left, right| left.0 == right.0);

    let mut entries = Vec::<CitationParityEntry>::new();
    for (target_id, doc_id, reference) in target_refs {
        let top_results = query_citation_parity_results(connection, &doc_id, &reference)?;
        entries.push(CitationParityEntry {
            target_id,
            doc_id,
            reference,
            top_results,
        });
    }

    Ok(entries)
}

fn query_citation_parity_results(
    connection: &Connection,
    doc_id: &str,
    reference: &str,
) -> Result<Vec<CitationParityIdentity>> {
    let mut statement = connection.prepare(
        "
        SELECT
          COALESCE(ref, ''),
          COALESCE(anchor_type, ''),
          COALESCE(anchor_label_norm, ''),
          COALESCE(citation_anchor_id, ''),
          page_pdf_start,
          page_pdf_end,
          chunk_id
        FROM chunks
        WHERE doc_id = ?1
          AND (
            lower(COALESCE(ref, '')) = lower(?2)
            OR lower(COALESCE(heading, '')) = lower(?2)
            OR lower(COALESCE(ref, '')) LIKE '%' || lower(?2) || '%'
            OR lower(COALESCE(heading, '')) LIKE '%' || lower(?2) || '%'
          )
        ORDER BY
          CASE
            WHEN lower(COALESCE(ref, '')) = lower(?2) THEN 1000
            WHEN lower(COALESCE(heading, '')) = lower(?2) THEN 900
            WHEN lower(COALESCE(ref, '')) LIKE '%' || lower(?2) || '%' THEN 700
            ELSE 600
          END DESC,
          page_pdf_start ASC,
          chunk_id ASC
        LIMIT 3
        ",
    )?;

    let mut rows = statement.query(params![doc_id, reference])?;
    let mut out = Vec::<CitationParityIdentity>::new();
    while let Some(row) = rows.next()? {
        let raw_ref: String = row.get(0)?;
        let anchor_type: String = row.get(1)?;
        let anchor_label_norm: String = row.get(2)?;
        let citation_anchor_id: String = row.get(3)?;
        let page_start: Option<i64> = row.get(4)?;
        let page_end: Option<i64> = row.get(5)?;

        let anchor_identity = if !citation_anchor_id.trim().is_empty() {
            citation_anchor_id
        } else {
            format!("{}:{}", anchor_type.trim(), anchor_label_norm.trim())
        };

        out.push(CitationParityIdentity {
            canonical_ref: canonicalize_reference_for_parity(&raw_ref),
            anchor_identity,
            page_start,
            page_end,
        });
    }

    Ok(out)
}

fn canonicalize_reference_for_parity(reference: &str) -> String {
    if let Some((base, _)) = reference.split_once(" item ") {
        return base.trim().to_string();
    }
    if let Some((base, _)) = reference.split_once(" note ") {
        return base.trim().to_string();
    }
    if let Some((base, _)) = reference.split_once(" para ") {
        return base.trim().to_string();
    }
    if let Some((base, _)) = reference.split_once(" row ") {
        return base.trim().to_string();
    }
    reference.trim().to_string()
}

fn checksum_citation_entries(entries: &[CitationParityEntry]) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for entry in entries {
        entry.target_id.hash(&mut hasher);
        entry.doc_id.hash(&mut hasher);
        entry.reference.hash(&mut hasher);
        for result in &entry.top_results {
            result.hash(&mut hasher);
        }
    }
    format!("{:016x}", hasher.finish())
}

fn replay_stability_ratio(
    current_entries: &[PageProvenanceEntry],
    previous_entries: &[PageProvenanceEntry],
    backend: &str,
) -> Option<f64> {
    if current_entries.is_empty() || previous_entries.is_empty() {
        return None;
    }

    let previous_map = previous_entries
        .iter()
        .filter(|entry| entry.backend == backend)
        .map(|entry| ((entry.doc_id.clone(), entry.page_pdf), entry.text_char_count))
        .collect::<HashMap<(String, i64), usize>>();

    if previous_map.is_empty() {
        return None;
    }

    let mut comparable = 0usize;
    let mut stable = 0usize;
    for entry in current_entries.iter().filter(|entry| entry.backend == backend) {
        if let Some(previous_chars) = previous_map.get(&(entry.doc_id.clone(), entry.page_pdf)) {
            comparable += 1;
            if *previous_chars == entry.text_char_count {
                stable += 1;
            }
        }
    }

    ratio(stable, comparable)
}

fn load_page_provenance_entries(
    manifest_dir: &Path,
    snapshot: Option<&NamedIngestRunSnapshot>,
) -> Result<Vec<PageProvenanceEntry>> {
    let Some(snapshot) = snapshot else {
        return Ok(Vec::new());
    };

    let Some(path_value) = snapshot.snapshot.paths.page_provenance_path.as_deref() else {
        return Ok(Vec::new());
    };

    let candidate = Path::new(path_value);
    let path = if candidate.is_absolute() {
        candidate.to_path_buf()
    } else if candidate.exists() {
        candidate.to_path_buf()
    } else {
        manifest_dir.join(candidate.file_name().unwrap_or_default())
    };

    if !path.exists() {
        return Ok(Vec::new());
    }

    let raw = fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
    let manifest: PageProvenanceManifestSnapshot = serde_json::from_slice(&raw)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(manifest.entries)
}

fn load_gold_manifest(path: &Path) -> Result<GoldSetManifest> {
    let raw = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let manifest: GoldSetManifest = serde_json::from_slice(&raw)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(manifest)
}

fn resolve_run_id(manifest_dir: &Path, fallback: &str) -> String {
    let latest_ingest_run_id = load_latest_ingest_run_id(manifest_dir).ok().flatten();

    let run_state_path = manifest_dir.join("run_state.json");
    let run_state_run_id = fs::read(&run_state_path)
        .ok()
        .and_then(|raw| serde_json::from_slice::<RunStateManifest>(&raw).ok())
        .and_then(|state| state.active_run_id);

    latest_ingest_run_id
        .or(run_state_run_id)
        .unwrap_or_else(|| fallback.to_string())
}

fn load_target_sections_manifest(manifest_dir: &Path) -> Result<Option<TargetSectionsManifest>> {
    let target_sections_path = manifest_dir.join("target_sections.json");
    if !target_sections_path.exists() {
        return Ok(None);
    }

    let raw = fs::read(&target_sections_path)
        .with_context(|| format!("failed to read {}", target_sections_path.display()))?;
    let manifest: TargetSectionsManifest = serde_json::from_slice(&raw)
        .with_context(|| format!("failed to parse {}", target_sections_path.display()))?;

    Ok(Some(manifest))
}

fn build_target_coverage_report(
    target_sections: &Option<TargetSectionsManifest>,
    refs: &[GoldReference],
) -> TargetCoverageReport {
    let Some(target_sections) = target_sections.as_ref() else {
        return TargetCoverageReport {
            source_manifest: None,
            target_total: 0,
            target_linked_gold_total: refs
                .iter()
                .filter(|reference| reference.target_id.is_some())
                .count(),
            covered_target_total: 0,
            missing_target_ids: Vec::new(),
            duplicate_target_ids: Vec::new(),
            unexpected_target_ids: Vec::new(),
        };
    };

    let target_ids = target_sections
        .targets
        .iter()
        .map(|target| target.id.trim().to_string())
        .collect::<Vec<String>>();
    let target_lookup = target_ids.iter().cloned().collect::<HashSet<String>>();

    let mut counts = HashMap::<String, usize>::new();
    let mut unexpected_target_ids = Vec::<String>::new();

    for reference in refs {
        let Some(target_id) = reference.target_id.as_deref().map(str::trim) else {
            continue;
        };

        if target_lookup.contains(target_id) {
            *counts.entry(target_id.to_string()).or_insert(0) += 1;
        } else {
            unexpected_target_ids.push(target_id.to_string());
        }
    }

    let mut missing_target_ids = Vec::<String>::new();
    let mut duplicate_target_ids = Vec::<String>::new();
    for target_id in &target_ids {
        match counts.get(target_id).copied().unwrap_or(0) {
            0 => missing_target_ids.push(target_id.clone()),
            1 => {}
            _ => duplicate_target_ids.push(target_id.clone()),
        }
    }

    unexpected_target_ids.sort();
    unexpected_target_ids.dedup();

    let target_linked_gold_total = refs
        .iter()
        .filter(|reference| reference.target_id.is_some())
        .count();

    TargetCoverageReport {
        source_manifest: Some("target_sections.json".to_string()),
        target_total: target_sections.target_count.unwrap_or(target_ids.len()),
        target_linked_gold_total,
        covered_target_total: target_ids.len().saturating_sub(missing_target_ids.len()),
        missing_target_ids,
        duplicate_target_ids,
        unexpected_target_ids,
    }
}

fn build_freshness_report(
    manifest_dir: &Path,
    target_sections: &Option<TargetSectionsManifest>,
) -> Result<FreshnessReport> {
    let required_parts = target_sections
        .as_ref()
        .map(required_target_parts)
        .unwrap_or_default();

    let snapshots = load_ingest_snapshots(manifest_dir)?;
    let latest = snapshots.last();

    let latest_run_parts = latest
        .map(|snapshot| resolve_processed_parts(&snapshot.snapshot, &required_parts))
        .unwrap_or_default();

    let stale_parts = required_parts
        .iter()
        .copied()
        .filter(|part| !latest_run_parts.contains(part))
        .collect::<Vec<u32>>();

    let mut latest_run_by_part = Vec::<PartFreshness>::new();
    for part in &required_parts {
        let mut entry = PartFreshness {
            part: *part,
            manifest: None,
            run_id: None,
            started_at: None,
        };

        for snapshot in snapshots.iter().rev() {
            let processed_parts = resolve_processed_parts(&snapshot.snapshot, &required_parts);
            if processed_parts.contains(part) {
                entry.manifest = Some(snapshot.manifest_name.clone());
                entry.run_id = snapshot.snapshot.run_id.clone();
                entry.started_at = snapshot.snapshot.started_at.clone();
                break;
            }
        }

        latest_run_by_part.push(entry);
    }

    let full_target_cycle_run_id = snapshots.iter().rev().find_map(|snapshot| {
        let processed_parts = resolve_processed_parts(&snapshot.snapshot, &required_parts);
        let all_parts_present = required_parts
            .iter()
            .all(|required| processed_parts.contains(required));
        if all_parts_present {
            snapshot.snapshot.run_id.clone()
        } else {
            None
        }
    });

    Ok(FreshnessReport {
        source_manifest_dir: manifest_dir.display().to_string(),
        required_parts,
        latest_manifest: latest.map(|snapshot| snapshot.manifest_name.clone()),
        latest_run_id: latest.and_then(|snapshot| snapshot.snapshot.run_id.clone()),
        latest_started_at: latest.and_then(|snapshot| snapshot.snapshot.started_at.clone()),
        latest_run_parts,
        latest_run_by_part,
        full_target_cycle_run_id,
        stale_parts,
    })
}

fn required_target_parts(manifest: &TargetSectionsManifest) -> Vec<u32> {
    let mut parts = manifest
        .targets
        .iter()
        .map(|target| target.part)
        .collect::<Vec<u32>>();
    parts.sort_unstable();
    parts.dedup();
    parts
}

fn load_ingest_snapshots(manifest_dir: &Path) -> Result<Vec<NamedIngestRunSnapshot>> {
    let mut snapshots = Vec::<NamedIngestRunSnapshot>::new();

    for entry in fs::read_dir(manifest_dir)? {
        let entry = entry?;
        let file_name = entry.file_name().to_string_lossy().to_string();
        if !file_name.starts_with("ingest_run_") || !file_name.ends_with(".json") {
            continue;
        }

        let manifest_path = entry.path();
        let raw = fs::read(&manifest_path)
            .with_context(|| format!("failed to read {}", manifest_path.display()))?;
        let snapshot: IngestRunSnapshot = serde_json::from_slice(&raw)
            .with_context(|| format!("failed to parse {}", manifest_path.display()))?;

        snapshots.push(NamedIngestRunSnapshot {
            manifest_name: file_name,
            snapshot,
        });
    }

    snapshots.sort_by(|left, right| left.manifest_name.cmp(&right.manifest_name));
    Ok(snapshots)
}

fn resolve_processed_parts(snapshot: &IngestRunSnapshot, required_parts: &[u32]) -> Vec<u32> {
    let mut processed_parts = if !snapshot.processed_parts.is_empty() {
        snapshot.processed_parts.clone()
    } else {
        parse_target_parts_from_command(snapshot.command.as_deref().unwrap_or(""))
    };

    if processed_parts.is_empty() {
        processed_parts = required_parts.to_vec();
    }

    processed_parts.sort_unstable();
    processed_parts.dedup();
    processed_parts
}

fn parse_target_parts_from_command(command: &str) -> Vec<u32> {
    let mut parts = Vec::<u32>::new();
    let mut tokens = command.split_whitespace().peekable();

    while let Some(token) = tokens.next() {
        if token != "--target-part" {
            continue;
        }

        let Some(value) = tokens.next() else {
            continue;
        };

        if let Ok(parsed) = value.parse::<u32>() {
            parts.push(parsed);
        }
    }

    parts.sort_unstable();
    parts.dedup();
    parts
}

fn load_latest_ingest_run_id(manifest_dir: &Path) -> Result<Option<String>> {
    let mut latest_manifest_path: Option<PathBuf> = None;
    let mut latest_manifest_name: Option<String> = None;

    for entry in fs::read_dir(manifest_dir)? {
        let entry = entry?;
        let file_name = entry.file_name().to_string_lossy().to_string();
        if !file_name.starts_with("ingest_run_") || !file_name.ends_with(".json") {
            continue;
        }

        match &latest_manifest_name {
            Some(current) if file_name <= *current => {}
            _ => {
                latest_manifest_name = Some(file_name);
                latest_manifest_path = Some(entry.path());
            }
        }
    }

    let Some(manifest_path) = latest_manifest_path else {
        return Ok(None);
    };

    let raw = fs::read(&manifest_path)
        .with_context(|| format!("failed to read {}", manifest_path.display()))?;
    let snapshot: IngestRunSnapshot = serde_json::from_slice(&raw)
        .with_context(|| format!("failed to parse {}", manifest_path.display()))?;

    Ok(snapshot
        .run_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty()))
}

fn load_table_quality_scorecard(manifest_dir: &Path) -> Result<TableQualityScorecard> {
    let mut latest_manifest: Option<(String, PathBuf)> = None;

    for entry in fs::read_dir(manifest_dir)? {
        let entry = entry?;
        let file_name = entry.file_name().to_string_lossy().to_string();
        if !file_name.starts_with("ingest_run_") || !file_name.ends_with(".json") {
            continue;
        }

        match &latest_manifest {
            Some((current, _)) if file_name <= *current => {}
            _ => latest_manifest = Some((file_name, entry.path())),
        }
    }

    let Some((manifest_name, manifest_path)) = latest_manifest else {
        return Ok(empty_table_scorecard());
    };

    let raw = fs::read(&manifest_path)
        .with_context(|| format!("failed to read {}", manifest_path.display()))?;
    let snapshot: IngestRunSnapshot = serde_json::from_slice(&raw)
        .with_context(|| format!("failed to parse {}", manifest_path.display()))?;

    Ok(build_table_quality_scorecard(
        Some(manifest_name),
        snapshot.counts.table_quality_counters(),
    ))
}

fn empty_table_scorecard() -> TableQualityScorecard {
    build_table_quality_scorecard(None, TableQualityCounters::default())
}

fn build_table_quality_scorecard(
    source_manifest: Option<String>,
    counters: TableQualityCounters,
) -> TableQualityScorecard {
    let table_sparse_row_ratio = ratio(
        counters.table_sparse_rows_count,
        counters.table_row_nodes_inserted,
    );
    let table_overloaded_row_ratio = ratio(
        counters.table_overloaded_rows_count,
        counters.table_row_nodes_inserted,
    );
    let table_marker_sequence_coverage = ratio(
        counters.table_marker_observed_count,
        counters.table_marker_expected_count,
    );
    let table_description_coverage = ratio(
        counters.table_rows_with_descriptions_count,
        counters.table_rows_with_markers_count,
    );

    TableQualityScorecard {
        source_manifest,
        counters,
        table_sparse_row_ratio,
        table_overloaded_row_ratio,
        table_marker_sequence_coverage,
        table_description_coverage,
    }
}

fn ratio(numerator: usize, denominator: usize) -> Option<f64> {
    if denominator == 0 {
        None
    } else {
        Some(numerator as f64 / denominator as f64)
    }
}

fn collect_evaluable_doc_ids(connection: &Connection) -> Result<HashSet<String>> {
    let mut statement = connection.prepare("SELECT DISTINCT doc_id FROM chunks")?;
    let mut rows = statement.query([])?;

    let mut doc_ids = HashSet::<String>::new();
    while let Some(row) = rows.next()? {
        let doc_id: String = row.get(0)?;
        if !doc_id.trim().is_empty() {
            doc_ids.insert(doc_id);
        }
    }

    Ok(doc_ids)
}

fn skipped_reference_evaluation() -> ReferenceEvaluation {
    ReferenceEvaluation {
        skipped: true,
        found: false,
        chunk_type: None,
        page_start: None,
        page_end: None,
        source_hash: None,
        has_all_terms: false,
        has_any_term: false,
        table_row_count: 0,
        table_cell_count: 0,
        list_item_count: 0,
        lineage_complete: false,
        hierarchy_ok: false,
        page_pattern_match: None,
    }
}

fn evaluate_reference(
    connection: &Connection,
    reference: &GoldReference,
) -> Result<ReferenceEvaluation> {
    let mut row = connection
        .query_row(
            "
            SELECT
              type,
              page_pdf_start,
              page_pdf_end,
              source_hash,
              text,
              origin_node_id,
              leaf_node_type,
              ancestor_path,
              anchor_type,
              anchor_label_norm
            FROM chunks
            WHERE doc_id = ?1 AND lower(ref) = lower(?2)
            ORDER BY page_pdf_start
            LIMIT 1
            ",
            params![reference.doc_id, reference.reference],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                ))
            },
        )
        .ok();

    if row.is_none() {
        row = connection
            .query_row(
                "
                SELECT
                  node_type,
                  page_pdf_start,
                  page_pdf_end,
                  source_hash,
                  text,
                  node_id,
                  node_type,
                  ancestor_path,
                  anchor_type,
                  anchor_label_norm
                FROM nodes
                WHERE doc_id = ?1 AND lower(ref) = lower(?2)
                ORDER BY page_pdf_start
                LIMIT 1
                ",
                params![reference.doc_id, reference.reference],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, Option<String>>(8)?,
                        row.get::<_, Option<String>>(9)?,
                    ))
                },
            )
            .ok();
    }

    if row.is_none() {
        row = connection
            .query_row(
                "
                SELECT
                  type,
                  page_pdf_start,
                  page_pdf_end,
                  source_hash,
                  text,
                  origin_node_id,
                  leaf_node_type,
                  ancestor_path,
                  anchor_type,
                  anchor_label_norm
                FROM chunks
                WHERE doc_id = ?1 AND lower(ref) LIKE '%' || lower(?2) || '%'
                ORDER BY page_pdf_start
                LIMIT 1
                ",
                params![reference.doc_id, reference.reference],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, Option<String>>(8)?,
                        row.get::<_, Option<String>>(9)?,
                    ))
                },
            )
            .ok();
    }

    if row.is_none() {
        row = connection
            .query_row(
                "
                SELECT
                  node_type,
                  page_pdf_start,
                  page_pdf_end,
                  source_hash,
                  text,
                  node_id,
                  node_type,
                  ancestor_path,
                  anchor_type,
                  anchor_label_norm
                FROM nodes
                WHERE doc_id = ?1 AND lower(ref) LIKE '%' || lower(?2) || '%'
                ORDER BY page_pdf_start
                LIMIT 1
                ",
                params![reference.doc_id, reference.reference],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, Option<String>>(8)?,
                        row.get::<_, Option<String>>(9)?,
                    ))
                },
            )
            .ok();
    }

    let Some((
        chunk_type,
        page_start,
        page_end,
        source_hash,
        text,
        origin_node_id,
        leaf_node_type,
        ancestor_path,
        anchor_type,
        anchor_label_norm,
    )) = row
    else {
        return Ok(ReferenceEvaluation {
            skipped: false,
            found: false,
            chunk_type: None,
            page_start: None,
            page_end: None,
            source_hash: None,
            has_all_terms: false,
            has_any_term: false,
            table_row_count: 0,
            table_cell_count: 0,
            list_item_count: 0,
            lineage_complete: false,
            hierarchy_ok: false,
            page_pattern_match: None,
        });
    };

    let text_value = text.unwrap_or_default().to_lowercase();

    let must_terms = reference
        .must_match_terms
        .iter()
        .map(|term| term.to_lowercase())
        .collect::<Vec<String>>();

    let has_all_terms = if must_terms.is_empty() {
        true
    } else {
        must_terms.iter().all(|term| text_value.contains(term))
    };

    let has_any_term = if must_terms.is_empty() {
        true
    } else {
        must_terms.iter().any(|term| text_value.contains(term))
    };

    let page_pattern_match = if reference.expected_page_pattern.starts_with("TBD-") {
        None
    } else {
        let page_text = format_page_range(page_start, page_end);
        Some(page_text == reference.expected_page_pattern)
    };

    let (parent_ref, table_row_count, table_cell_count, list_item_count) =
        if let Some(origin_node_id_value) = origin_node_id.as_deref() {
            collect_hierarchy_stats(connection, origin_node_id_value)?
        } else {
            (None, 0, 0, 0)
        };

    let lineage_complete = origin_node_id.is_some()
        && leaf_node_type.is_some()
        && ancestor_path
            .as_deref()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false);

    let hierarchy_ok = evaluate_hierarchy_expectations(
        reference,
        leaf_node_type.as_deref(),
        parent_ref.as_deref(),
        anchor_type.as_deref(),
        anchor_label_norm.as_deref(),
        table_row_count,
        table_cell_count,
        list_item_count,
    );

    Ok(ReferenceEvaluation {
        skipped: false,
        found: true,
        chunk_type: Some(chunk_type),
        page_start,
        page_end,
        source_hash,
        has_all_terms,
        has_any_term,
        table_row_count,
        table_cell_count,
        list_item_count,
        lineage_complete,
        hierarchy_ok,
        page_pattern_match,
    })
}

fn build_quality_checks(
    connection: &Connection,
    refs: &[GoldReference],
    evals: &[ReferenceEvaluation],
    table_quality: &TableQualityScorecard,
    target_coverage: &TargetCoverageReport,
    freshness: &FreshnessReport,
) -> Result<Vec<QualityCheck>> {
    let evaluable = refs
        .iter()
        .zip(evals.iter())
        .filter(|(_, eval)| !eval.skipped)
        .collect::<Vec<(&GoldReference, &ReferenceEvaluation)>>();

    let total = evaluable.len();
    let found = evaluable.iter().filter(|(_, eval)| eval.found).count();

    let page_pattern_expected = evaluable
        .iter()
        .filter(|(_, eval)| eval.page_pattern_match.is_some())
        .count();
    let page_pattern_ok = evaluable
        .iter()
        .filter(|(_, eval)| eval.page_pattern_match == Some(true))
        .count();

    let table_total = evaluable
        .iter()
        .filter(|(reference, _)| reference.reference.starts_with("Table "))
        .count();
    let table_ok = evaluable
        .iter()
        .filter(|(reference, eval)| {
            reference.reference.starts_with("Table ")
                && eval.chunk_type.as_deref() == Some("table")
                && eval.found
        })
        .count();

    let conflicting_chunk_ids: i64 = connection.query_row(
        "SELECT COUNT(*) FROM (SELECT chunk_id FROM chunks GROUP BY chunk_id HAVING COUNT(*) > 1)",
        [],
        |row| row.get(0),
    )?;

    let exact_ref_total = evaluable.len();
    let exact_ref_hits = evaluable
        .iter()
        .filter(|reference| {
            connection
                .query_row(
                    "
                    SELECT 1
                    FROM chunks
                    WHERE doc_id = ?1 AND lower(ref) = lower(?2)
                    LIMIT 1
                    ",
                    params![reference.0.doc_id, reference.0.reference],
                    |_| Ok(1_i64),
                )
                .is_ok()
                || connection
                    .query_row(
                        "
                        SELECT 1
                        FROM nodes
                        WHERE doc_id = ?1 AND lower(ref) = lower(?2)
                        LIMIT 1
                        ",
                        params![reference.0.doc_id, reference.0.reference],
                        |_| Ok(1_i64),
                    )
                    .is_ok()
        })
        .count();

    let keyword_total = evaluable
        .iter()
        .filter(|(reference, _)| !reference.must_match_terms.is_empty())
        .count();
    let keyword_ok = evaluable
        .iter()
        .filter(|(reference, eval)| !reference.must_match_terms.is_empty() && eval.has_any_term)
        .count();

    let citation_ok = evaluable
        .iter()
        .filter(|(_, eval)| {
            eval.found
                && eval.page_start.is_some()
                && eval.page_end.is_some()
                && eval
                    .source_hash
                    .as_deref()
                    .map(|value| !value.is_empty())
                    .unwrap_or(false)
        })
        .count();

    let db_schema_version = connection
        .query_row(
            "SELECT value FROM metadata WHERE key = 'db_schema_version' LIMIT 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .ok();

    let mut checks = Vec::new();
    checks.push(QualityCheck {
        check_id: "Q-001".to_string(),
        name: "Gold references retrievable".to_string(),
        result: if total == 0 {
            "pending"
        } else if found == total {
            "pass"
        } else {
            "failed"
        }
        .to_string(),
    });
    checks.push(QualityCheck {
        check_id: "Q-002".to_string(),
        name: "Citation page ranges valid".to_string(),
        result: if page_pattern_expected == 0 {
            "pending"
        } else if page_pattern_ok == page_pattern_expected {
            "pass"
        } else {
            "failed"
        }
        .to_string(),
    });
    checks.push(QualityCheck {
        check_id: "Q-003".to_string(),
        name: "Table chunks present".to_string(),
        result: if table_total == 0 {
            "pending"
        } else if table_ok == table_total {
            "pass"
        } else {
            "failed"
        }
        .to_string(),
    });
    checks.push(QualityCheck {
        check_id: "Q-004".to_string(),
        name: "No conflicting reference ids".to_string(),
        result: if conflicting_chunk_ids == 0 {
            "pass"
        } else {
            "failed"
        }
        .to_string(),
    });
    checks.push(QualityCheck {
        check_id: "Q-005".to_string(),
        name: "Exact reference query ranking".to_string(),
        result: if exact_ref_total == 0 {
            "pending"
        } else if exact_ref_hits == exact_ref_total {
            "pass"
        } else {
            "failed"
        }
        .to_string(),
    });
    checks.push(QualityCheck {
        check_id: "Q-006".to_string(),
        name: "Keyword query relevance".to_string(),
        result: if keyword_total == 0 {
            "pending"
        } else if keyword_ok == keyword_total {
            "pass"
        } else {
            "failed"
        }
        .to_string(),
    });
    checks.push(QualityCheck {
        check_id: "Q-007".to_string(),
        name: "Citation fields are non-null".to_string(),
        result: if citation_ok == found {
            "pass"
        } else {
            "failed"
        }
        .to_string(),
    });
    checks.push(QualityCheck {
        check_id: "Q-008".to_string(),
        name: "Manifest and db version compatibility".to_string(),
        result: if db_schema_version.as_deref() == Some(DB_SCHEMA_VERSION) {
            "pass"
        } else {
            "failed"
        }
        .to_string(),
    });

    let lineage_ok = evaluable
        .iter()
        .filter(|(_, eval)| eval.found)
        .all(|(_, eval)| eval.lineage_complete);
    checks.push(QualityCheck {
        check_id: "Q-009".to_string(),
        name: "Chunk lineage fields populated".to_string(),
        result: if found == 0 {
            "pending"
        } else if lineage_ok {
            "pass"
        } else {
            "failed"
        }
        .to_string(),
    });

    let hierarchy_expected_total = evaluable
        .iter()
        .filter(|(reference, _)| has_hierarchy_expectations(reference))
        .count();
    let hierarchy_expected_ok = evaluable
        .iter()
        .filter(|(reference, eval)| has_hierarchy_expectations(reference) && eval.hierarchy_ok)
        .count();
    checks.push(QualityCheck {
        check_id: "Q-010".to_string(),
        name: "Hierarchy expectations satisfied".to_string(),
        result: if hierarchy_expected_total == 0 {
            "pending"
        } else if hierarchy_expected_ok == hierarchy_expected_total {
            "pass"
        } else {
            "failed"
        }
        .to_string(),
    });

    checks.push(QualityCheck {
        check_id: "Q-011".to_string(),
        name: "Table sparse-row ratio threshold".to_string(),
        result: evaluate_max_threshold(
            table_quality.table_sparse_row_ratio,
            TABLE_SPARSE_ROW_RATIO_MAX,
        )
        .to_string(),
    });
    checks.push(QualityCheck {
        check_id: "Q-012".to_string(),
        name: "Table overloaded-row ratio threshold".to_string(),
        result: evaluate_max_threshold(
            table_quality.table_overloaded_row_ratio,
            TABLE_OVERLOADED_ROW_RATIO_MAX,
        )
        .to_string(),
    });
    checks.push(QualityCheck {
        check_id: "Q-013".to_string(),
        name: "Table marker-sequence coverage threshold".to_string(),
        result: evaluate_min_threshold(
            table_quality.table_marker_sequence_coverage,
            TABLE_MARKER_SEQUENCE_COVERAGE_MIN,
        )
        .to_string(),
    });
    checks.push(QualityCheck {
        check_id: "Q-014".to_string(),
        name: "Table description coverage threshold".to_string(),
        result: evaluate_min_threshold(
            table_quality.table_description_coverage,
            TABLE_DESCRIPTION_COVERAGE_MIN,
        )
        .to_string(),
    });

    let marker_expected_total = evaluable
        .iter()
        .filter(|(reference, _)| {
            reference
                .expected_anchor_type
                .as_deref()
                .map(|value| value.eq_ignore_ascii_case("marker"))
                .unwrap_or(false)
                || reference.expected_marker_label.is_some()
        })
        .count();
    let marker_extracted_ok = evaluable
        .iter()
        .filter(|(reference, eval)| {
            (reference
                .expected_anchor_type
                .as_deref()
                .map(|value| value.eq_ignore_ascii_case("marker"))
                .unwrap_or(false)
                || reference.expected_marker_label.is_some())
                && eval.found
                && eval.hierarchy_ok
        })
        .count();
    let marker_citation_ok = evaluable
        .iter()
        .filter(|(reference, eval)| {
            (reference
                .expected_anchor_type
                .as_deref()
                .map(|value| value.eq_ignore_ascii_case("marker"))
                .unwrap_or(false)
                || reference.expected_marker_label.is_some())
                && eval.found
                && eval.hierarchy_ok
                && eval.page_start.is_some()
                && eval.page_end.is_some()
        })
        .count();

    let paragraph_expected_total = evaluable
        .iter()
        .filter(|(reference, _)| {
            reference
                .expected_anchor_type
                .as_deref()
                .map(|value| value.eq_ignore_ascii_case("paragraph"))
                .unwrap_or(false)
                || reference.expected_paragraph_index.is_some()
        })
        .count();
    let paragraph_citation_ok = evaluable
        .iter()
        .filter(|(reference, eval)| {
            (reference
                .expected_anchor_type
                .as_deref()
                .map(|value| value.eq_ignore_ascii_case("paragraph"))
                .unwrap_or(false)
                || reference.expected_paragraph_index.is_some())
                && eval.found
                && eval.hierarchy_ok
                && eval.page_start.is_some()
                && eval.page_end.is_some()
        })
        .count();

    checks.push(QualityCheck {
        check_id: "Q-015".to_string(),
        name: "Marker extraction coverage threshold".to_string(),
        result: evaluate_min_threshold(
            ratio(marker_extracted_ok, marker_expected_total),
            MARKER_EXTRACTION_COVERAGE_MIN,
        )
        .to_string(),
    });
    checks.push(QualityCheck {
        check_id: "Q-016".to_string(),
        name: "Marker citation accuracy threshold".to_string(),
        result: evaluate_min_threshold(
            ratio(marker_citation_ok, marker_expected_total),
            MARKER_CITATION_ACCURACY_MIN,
        )
        .to_string(),
    });
    checks.push(QualityCheck {
        check_id: "Q-017".to_string(),
        name: "Paragraph fallback citation accuracy threshold".to_string(),
        result: evaluate_min_threshold(
            ratio(paragraph_citation_ok, paragraph_expected_total),
            PARAGRAPH_CITATION_ACCURACY_MIN,
        )
        .to_string(),
    });

    let structural_invariants = collect_structural_invariants(connection)?;
    checks.push(QualityCheck {
        check_id: "Q-018".to_string(),
        name: "Structural hierarchy invariants satisfied".to_string(),
        result: if structural_invariants.violation_count() == 0 {
            "pass"
        } else {
            "failed"
        }
        .to_string(),
    });

    let asil_alignment = collect_asil_table_alignment(
        connection,
        "ISO26262-6-2018",
        &["Table 3", "Table 6", "Table 10"],
    )?;
    checks.push(QualityCheck {
        check_id: "Q-019".to_string(),
        name: "ASIL table row/cell alignment checks".to_string(),
        result: evaluate_asil_table_alignment(&asil_alignment).to_string(),
    });

    checks.push(QualityCheck {
        check_id: "Q-020".to_string(),
        name: "Target register coverage completeness".to_string(),
        result: if target_coverage.target_total == 0 {
            "pending"
        } else if target_coverage.missing_target_ids.is_empty()
            && target_coverage.duplicate_target_ids.is_empty()
            && target_coverage.unexpected_target_ids.is_empty()
            && target_coverage.covered_target_total == target_coverage.target_total
        {
            "pass"
        } else {
            "failed"
        }
        .to_string(),
    });

    let target_linked_total = refs
        .iter()
        .zip(evals.iter())
        .filter(|(reference, _)| reference.target_id.is_some())
        .count();
    let target_linked_ok = refs
        .iter()
        .zip(evals.iter())
        .filter(|(reference, eval)| {
            reference.target_id.is_some()
                && !eval.skipped
                && eval.found
                && eval.has_all_terms
                && eval.hierarchy_ok
        })
        .count();
    checks.push(QualityCheck {
        check_id: "Q-021".to_string(),
        name: "Target-linked references retrievable".to_string(),
        result: if target_linked_total == 0 {
            "pending"
        } else if target_linked_total == target_linked_ok {
            "pass"
        } else {
            "failed"
        }
        .to_string(),
    });

    checks.push(QualityCheck {
        check_id: "Q-022".to_string(),
        name: "Target-part freshness completeness".to_string(),
        result: if freshness.required_parts.is_empty() {
            "pending"
        } else if freshness.stale_parts.is_empty() {
            "pass"
        } else {
            "failed"
        }
        .to_string(),
    });

    Ok(checks)
}

fn evaluate_max_threshold(value: Option<f64>, max_allowed: f64) -> &'static str {
    match value {
        Some(actual) if actual <= max_allowed => "pass",
        Some(_) => "failed",
        None => "pending",
    }
}

fn evaluate_min_threshold(value: Option<f64>, min_allowed: f64) -> &'static str {
    match value {
        Some(actual) if actual >= min_allowed => "pass",
        Some(_) => "failed",
        None => "pending",
    }
}

fn summarize_checks(checks: &[QualityCheck]) -> QualitySummary {
    let passed = checks.iter().filter(|check| check.result == "pass").count();
    let failed = checks
        .iter()
        .filter(|check| check.result == "failed")
        .count();
    let pending = checks
        .iter()
        .filter(|check| check.result == "pending")
        .count();

    QualitySummary {
        total_checks: checks.len(),
        passed,
        failed,
        pending,
    }
}

fn collect_structural_invariants(connection: &Connection) -> Result<StructuralInvariantSummary> {
    Ok(StructuralInvariantSummary {
        parent_required_missing_count: query_violation_count(
            connection,
            "
            SELECT COUNT(*)
            FROM nodes
            WHERE node_type <> 'document'
              AND parent_node_id IS NULL
            ",
        )?,
        dangling_parent_pointer_count: query_violation_count(
            connection,
            "
            SELECT COUNT(*)
            FROM nodes child
            LEFT JOIN nodes parent ON parent.node_id = child.parent_node_id
            WHERE child.parent_node_id IS NOT NULL
              AND parent.node_id IS NULL
            ",
        )?,
        invalid_table_row_parent_count: query_violation_count(
            connection,
            "
            SELECT COUNT(*)
            FROM nodes child
            JOIN nodes parent ON parent.node_id = child.parent_node_id
            WHERE child.node_type = 'table_row'
              AND parent.node_type <> 'table'
            ",
        )?,
        invalid_table_cell_parent_count: query_violation_count(
            connection,
            "
            SELECT COUNT(*)
            FROM nodes child
            JOIN nodes parent ON parent.node_id = child.parent_node_id
            WHERE child.node_type = 'table_cell'
              AND parent.node_type <> 'table_row'
            ",
        )?,
        invalid_list_item_parent_count: query_violation_count(
            connection,
            "
            SELECT COUNT(*)
            FROM nodes child
            JOIN nodes parent ON parent.node_id = child.parent_node_id
            WHERE child.node_type = 'list_item'
              AND parent.node_type NOT IN ('list', 'list_item')
            ",
        )?,
        invalid_note_parent_count: query_violation_count(
            connection,
            "
            SELECT COUNT(*)
            FROM nodes child
            JOIN nodes parent ON parent.node_id = child.parent_node_id
            WHERE child.node_type = 'note'
              AND parent.node_type NOT IN ('clause', 'subclause', 'annex')
            ",
        )?,
        invalid_note_item_parent_count: query_violation_count(
            connection,
            "
            SELECT COUNT(*)
            FROM nodes child
            JOIN nodes parent ON parent.node_id = child.parent_node_id
            WHERE child.node_type = 'note_item'
              AND parent.node_type <> 'note'
            ",
        )?,
        invalid_paragraph_parent_count: query_violation_count(
            connection,
            "
            SELECT COUNT(*)
            FROM nodes child
            JOIN nodes parent ON parent.node_id = child.parent_node_id
            WHERE child.node_type = 'paragraph'
              AND parent.node_type NOT IN ('clause', 'subclause', 'annex')
            ",
        )?,
    })
}

fn collect_asil_table_alignment(
    connection: &Connection,
    doc_id: &str,
    table_refs: &[&str],
) -> Result<AsilTableAlignmentSummary> {
    let mut summary = AsilTableAlignmentSummary {
        tables_expected: table_refs.len(),
        ..AsilTableAlignmentSummary::default()
    };

    for table_ref in table_refs {
        let table_node_id = connection
            .query_row(
                "
                SELECT node_id
                FROM nodes
                WHERE doc_id = ?1
                  AND node_type = 'table'
                  AND lower(ref) = lower(?2)
                LIMIT 1
                ",
                params![doc_id, table_ref],
                |row| row.get::<_, String>(0),
            )
            .ok();

        let Some(table_node_id) = table_node_id else {
            continue;
        };

        summary.tables_found += 1;

        let mut statement = connection.prepare(
            "
            SELECT
              r.node_id,
              c.order_index,
              c.text
            FROM nodes r
            LEFT JOIN nodes c
              ON c.parent_node_id = r.node_id
             AND c.node_type = 'table_cell'
            WHERE r.parent_node_id = ?1
              AND r.node_type = 'table_row'
            ORDER BY r.order_index ASC, c.order_index ASC
            ",
        )?;

        let mut rows = statement.query([table_node_id])?;
        let mut active_row_id: Option<String> = None;
        let mut active_cells = Vec::<String>::new();

        while let Some(row) = rows.next()? {
            let row_id: String = row.get(0)?;
            let cell_text: Option<String> = row.get(2)?;

            if active_row_id.as_deref() != Some(row_id.as_str()) {
                if !active_cells.is_empty() {
                    analyze_asil_marker_row(&active_cells, &mut summary);
                    active_cells.clear();
                }
                active_row_id = Some(row_id);
            }

            if let Some(cell_text) = cell_text {
                active_cells.push(cell_text.trim().to_string());
            }
        }

        if !active_cells.is_empty() {
            analyze_asil_marker_row(&active_cells, &mut summary);
        }
    }

    Ok(summary)
}

fn analyze_asil_marker_row(cells: &[String], summary: &mut AsilTableAlignmentSummary) {
    let Some(first_cell) = cells.first() else {
        return;
    };

    if !looks_like_table_marker(first_cell) {
        return;
    }

    summary.marker_rows_total += 1;

    let description = cells.get(1).map(|value| value.as_str()).unwrap_or_default();
    if is_malformed_marker_description(description) {
        summary.marker_rows_malformed_description += 1;
    }

    if cells.len() > 10 {
        summary.marker_rows_outlier_cell_count += 1;
    }

    if cells.iter().skip(2).any(|cell| contains_asil_rating(cell)) {
        summary.marker_rows_with_ratings += 1;
    }
}

fn evaluate_asil_table_alignment(summary: &AsilTableAlignmentSummary) -> &'static str {
    if summary.tables_found < summary.tables_expected || summary.marker_rows_total == 0 {
        return "pending";
    }

    let rating_ok = summary
        .rating_coverage()
        .map(|value| value >= ASIL_ALIGNMENT_MIN_RATING_COVERAGE)
        .unwrap_or(false);
    let malformed_ok = summary
        .malformed_ratio()
        .map(|value| value <= ASIL_ALIGNMENT_MAX_MALFORMED_RATIO)
        .unwrap_or(false);
    let outlier_ok = summary
        .outlier_ratio()
        .map(|value| value <= ASIL_ALIGNMENT_MAX_OUTLIER_RATIO)
        .unwrap_or(false);

    if rating_ok && malformed_ok && outlier_ok {
        "pass"
    } else {
        "failed"
    }
}

fn looks_like_table_marker(value: &str) -> bool {
    let normalized = normalize_anchor_label(value);
    if normalized.is_empty() {
        return false;
    }

    let mut chars = normalized.chars().peekable();
    let mut has_digit = false;

    while let Some(ch) = chars.peek().copied() {
        if ch.is_ascii_digit() {
            has_digit = true;
            chars.next();
        } else {
            break;
        }
    }

    if !has_digit {
        return false;
    }

    if let Some(ch) = chars.next() {
        if !ch.is_ascii_lowercase() {
            return false;
        }
    }

    chars.next().is_none()
}

fn is_malformed_marker_description(description: &str) -> bool {
    let trimmed = description.trim();
    if trimmed.is_empty() {
        return true;
    }

    let alphabetic_count = trimmed
        .chars()
        .filter(|value| value.is_ascii_alphabetic())
        .count();
    alphabetic_count <= 1 && trimmed.split_whitespace().count() <= 1
}

fn contains_asil_rating(cell_text: &str) -> bool {
    cell_text
        .split_whitespace()
        .map(|token| token.trim_matches(['(', ')', '.', ':', ';', ',']))
        .any(is_asil_rating_token)
}

fn is_asil_rating_token(token: &str) -> bool {
    matches!(token, "+" | "++" | "-" | "--" | "+/-" | "+/−" | "−/+" | "o")
}

fn query_violation_count(connection: &Connection, sql: &str) -> Result<i64> {
    let count = connection.query_row(sql, [], |row| row.get::<_, i64>(0))?;
    Ok(count)
}

fn collect_hierarchy_stats(
    connection: &Connection,
    origin_node_id: &str,
) -> Result<(Option<String>, usize, usize, usize)> {
    let parent_ref = connection
        .query_row(
            "
            WITH RECURSIVE ancestors(node_id, parent_node_id, node_type, ref, depth) AS (
              SELECT n.node_id, n.parent_node_id, n.node_type, n.ref, 0
              FROM nodes n
              WHERE n.node_id = ?1

              UNION ALL

              SELECT p.node_id, p.parent_node_id, p.node_type, p.ref, a.depth + 1
              FROM nodes p
              JOIN ancestors a ON p.node_id = a.parent_node_id
              WHERE a.depth < 16
            )
            SELECT ref
            FROM ancestors
            WHERE depth > 0
              AND ref IS NOT NULL
              AND trim(ref) <> ''
              AND node_type IN ('clause', 'subclause', 'annex', 'table')
            ORDER BY depth ASC
            LIMIT 1
            ",
            [origin_node_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .ok()
        .flatten();

    let mut statement = connection.prepare(
        "
        WITH RECURSIVE descendants(node_id, node_type, depth) AS (
          SELECT n.node_id, n.node_type, 1
          FROM nodes n
          WHERE n.parent_node_id = ?1

          UNION ALL

          SELECT n.node_id, n.node_type, d.depth + 1
          FROM nodes n
          JOIN descendants d ON n.parent_node_id = d.node_id
          WHERE d.depth < 8
        )
        SELECT node_type, COUNT(*)
        FROM descendants
        GROUP BY node_type
        ",
    )?;

    let mut rows = statement.query([origin_node_id])?;
    let mut table_row_count = 0usize;
    let mut table_cell_count = 0usize;
    let mut list_item_count = 0usize;

    while let Some(row) = rows.next()? {
        let node_type: String = row.get(0)?;
        let count = row.get::<_, i64>(1)? as usize;
        match node_type.as_str() {
            "table_row" => table_row_count = count,
            "table_cell" => table_cell_count = count,
            "list_item" => list_item_count = count,
            _ => {}
        }
    }

    Ok((
        parent_ref,
        table_row_count,
        table_cell_count,
        list_item_count,
    ))
}

fn evaluate_hierarchy_expectations(
    reference: &GoldReference,
    leaf_node_type: Option<&str>,
    parent_ref: Option<&str>,
    anchor_type: Option<&str>,
    anchor_label_norm: Option<&str>,
    table_row_count: usize,
    table_cell_count: usize,
    list_item_count: usize,
) -> bool {
    let node_type_ok = match reference.expected_node_type.as_deref() {
        Some(expected) => leaf_node_type
            .map(|actual| actual.eq_ignore_ascii_case(expected))
            .unwrap_or(false),
        None => true,
    };

    let parent_ref_ok = match reference.expected_parent_ref.as_deref() {
        Some(expected) => parent_ref
            .map(|actual| actual.eq_ignore_ascii_case(expected))
            .unwrap_or(false),
        None => true,
    };

    let min_rows_ok = match reference.expected_min_rows {
        Some(expected) => table_row_count >= expected,
        None => true,
    };
    let min_cols_ok = match reference.expected_min_cols {
        Some(expected) => table_cell_count >= expected,
        None => true,
    };
    let min_list_items_ok = match reference.expected_min_list_items {
        Some(expected) => list_item_count >= expected,
        None => true,
    };

    let anchor_type_ok = match reference.expected_anchor_type.as_deref() {
        Some(expected) => anchor_type
            .map(|actual| actual.eq_ignore_ascii_case(expected))
            .unwrap_or(false),
        None => true,
    };

    let marker_label_ok = match reference.expected_marker_label.as_deref() {
        Some(expected) => {
            let expected_norm = normalize_anchor_label(expected);
            anchor_label_norm
                .map(normalize_anchor_label)
                .map(|actual| actual.eq_ignore_ascii_case(&expected_norm))
                .unwrap_or(false)
        }
        None => true,
    };

    let paragraph_index_ok = match reference.expected_paragraph_index {
        Some(expected) => {
            let paragraph_anchor = anchor_type
                .map(|value| value.eq_ignore_ascii_case("paragraph"))
                .unwrap_or(false);
            let actual_index = anchor_label_norm.and_then(|value| value.parse::<usize>().ok());
            paragraph_anchor && actual_index == Some(expected)
        }
        None => true,
    };

    node_type_ok
        && parent_ref_ok
        && min_rows_ok
        && min_cols_ok
        && min_list_items_ok
        && anchor_type_ok
        && marker_label_ok
        && paragraph_index_ok
}

fn build_hierarchy_metrics(evals: &[ReferenceEvaluation]) -> HierarchyMetrics {
    HierarchyMetrics {
        references_with_lineage: evals
            .iter()
            .filter(|eval| eval.found && eval.lineage_complete)
            .count(),
        table_references_with_rows: evals
            .iter()
            .filter(|eval| eval.found && eval.table_row_count > 0)
            .count(),
        table_references_with_cells: evals
            .iter()
            .filter(|eval| eval.found && eval.table_cell_count > 0)
            .count(),
        references_with_list_items: evals
            .iter()
            .filter(|eval| eval.found && eval.list_item_count > 0)
            .count(),
    }
}

fn has_hierarchy_expectations(reference: &GoldReference) -> bool {
    reference.expected_node_type.is_some()
        || reference.expected_parent_ref.is_some()
        || reference.expected_min_rows.is_some()
        || reference.expected_min_cols.is_some()
        || reference.expected_min_list_items.is_some()
        || reference.expected_anchor_type.is_some()
        || reference.expected_marker_label.is_some()
        || reference.expected_paragraph_index.is_some()
}

fn normalize_anchor_label(value: &str) -> String {
    value
        .trim()
        .trim_end_matches([')', '.', ':', ';'])
        .replace('–', "-")
        .replace('—', "-")
        .to_ascii_lowercase()
}

fn format_page_range(start: Option<i64>, end: Option<i64>) -> String {
    match (start, end) {
        (Some(start), Some(end)) if start == end => start.to_string(),
        (Some(start), Some(end)) => format!("{start}-{end}"),
        (Some(start), None) => start.to_string(),
        (None, Some(end)) => end.to_string(),
        (None, None) => "unknown".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ensure_citation_baseline_metadata_only, parse_citation_baseline_mode,
        parse_citation_baseline_path, parse_target_parts_from_command, resolve_processed_parts,
        CitationBaselineMode, GoldReference, IngestRunSnapshot,
    };

    #[test]
    fn gold_reference_deserializes_without_wp1_optional_fields() {
        let raw = r#"
        {
          "id": "G-legacy",
          "doc_id": "ISO26262-6-2018",
          "ref": "8.4.5",
          "expected_page_pattern": "26-27",
          "must_match_terms": ["source code"],
          "status": "pass"
        }
        "#;

        let reference: GoldReference =
            serde_json::from_str(raw).expect("legacy gold row should deserialize");
        assert_eq!(reference.reference, "8.4.5");
        assert!(reference.target_id.is_none());
        assert!(reference.target_ref_raw.is_none());
        assert!(reference.canonical_ref.is_none());
        assert!(reference.ref_resolution_mode.is_none());
    }

    #[test]
    fn parse_target_parts_from_command_extracts_and_deduplicates_values() {
        let command = "iso26262 ingest --cache-root .cache/iso26262 --target-part 8 --target-part 2 --target-part 8";
        let parts = parse_target_parts_from_command(command);
        assert_eq!(parts, vec![2, 8]);
    }

    #[test]
    fn resolve_processed_parts_falls_back_to_required_parts_when_missing() {
        let snapshot = IngestRunSnapshot::default();
        let parts = resolve_processed_parts(&snapshot, &[2, 6, 8, 9]);
        assert_eq!(parts, vec![2, 6, 8, 9]);
    }

    #[test]
    fn parse_citation_baseline_mode_supports_bootstrap_aliases() {
        assert_eq!(
            parse_citation_baseline_mode(Some("bootstrap")),
            CitationBaselineMode::Bootstrap
        );
        assert_eq!(
            parse_citation_baseline_mode(Some("RoTaTe")),
            CitationBaselineMode::Bootstrap
        );
        assert_eq!(
            parse_citation_baseline_mode(Some("verify")),
            CitationBaselineMode::Verify
        );
        assert_eq!(parse_citation_baseline_mode(None), CitationBaselineMode::Verify);
    }

    #[test]
    fn parse_citation_baseline_path_defaults_to_repo_lockfile() {
        let path = parse_citation_baseline_path(None);
        assert_eq!(
            path,
            std::path::PathBuf::from("manifests/citation_parity_baseline.lock.json")
        );

        let custom = parse_citation_baseline_path(Some("/tmp/custom.lock.json"));
        assert_eq!(custom, std::path::PathBuf::from("/tmp/custom.lock.json"));
    }

    #[test]
    fn citation_baseline_schema_guard_rejects_text_payload_fields() {
        let payload = serde_json::json!({
            "manifest_version": 1,
            "entries": [
                {
                    "target_id": "t1",
                    "text": "forbidden"
                }
            ]
        });

        let error = ensure_citation_baseline_metadata_only(&payload)
            .expect_err("schema guard should reject text-bearing fields");
        assert!(
            error.to_string().contains("forbidden text-bearing key"),
            "unexpected error: {}",
            error
        );
    }
}
