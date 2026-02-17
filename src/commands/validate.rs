use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags, params};
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
    target_coverage: TargetCoverageReport,
    freshness: FreshnessReport,
    hierarchy_metrics: HierarchyMetrics,
    table_quality_scorecard: TableQualityScorecard,
    checks: Vec<QualityCheck>,
    issues: Vec<String>,
    recommendations: Vec<String>,
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

#[derive(Debug, Serialize)]
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

#[derive(Debug, Deserialize, Default)]
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
    counts: TableQualityCounters,
}

#[derive(Debug)]
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

    let checks = build_quality_checks(
        &connection,
        &gold_manifest.gold_references,
        &evaluations,
        &table_quality_scorecard,
        &target_coverage,
        &freshness,
    )?;
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
        target_coverage,
        freshness,
        hierarchy_metrics,
        table_quality_scorecard,
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
        snapshot.counts,
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
        GoldReference, IngestRunSnapshot, parse_target_parts_from_command, resolve_processed_parts,
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
}
