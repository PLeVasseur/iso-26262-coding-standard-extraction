use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::cli::ValidateArgs;
use crate::util::{now_utc_string, write_json_pretty};

const DB_SCHEMA_VERSION: &str = "0.1.0";

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
    status: String,
}

#[derive(Debug)]
struct ReferenceEvaluation {
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

#[derive(Debug, Serialize)]
struct QualityReport {
    manifest_version: u32,
    run_id: String,
    generated_at: String,
    status: String,
    summary: QualitySummary,
    hierarchy_metrics: HierarchyMetrics,
    checks: Vec<QualityCheck>,
    issues: Vec<String>,
    recommendations: Vec<String>,
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

#[derive(Debug, Deserialize)]
struct RunStateManifest {
    active_run_id: Option<String>,
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

    let connection = Connection::open_with_flags(
        &db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("failed to open database read-only: {}", db_path.display()))?;

    let mut evaluations = Vec::with_capacity(gold_manifest.gold_references.len());
    for reference in &mut gold_manifest.gold_references {
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

    let checks = build_quality_checks(&connection, &gold_manifest.gold_references, &evaluations)?;
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

    let report = QualityReport {
        manifest_version: 1,
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
        hierarchy_metrics,
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
    let run_state_path = manifest_dir.join("run_state.json");
    let parsed = fs::read(&run_state_path)
        .ok()
        .and_then(|raw| serde_json::from_slice::<RunStateManifest>(&raw).ok())
        .and_then(|state| state.active_run_id);

    parsed.unwrap_or_else(|| fallback.to_string())
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
              ancestor_path
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
                ))
            },
        )
        .ok();

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
                  ancestor_path
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
    )) = row
    else {
        return Ok(ReferenceEvaluation {
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
        table_row_count,
        table_cell_count,
        list_item_count,
    );

    Ok(ReferenceEvaluation {
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
) -> Result<Vec<QualityCheck>> {
    let total = refs.len();
    let found = evals.iter().filter(|eval| eval.found).count();

    let page_pattern_expected = evals
        .iter()
        .filter(|eval| eval.page_pattern_match.is_some())
        .count();
    let page_pattern_ok = evals
        .iter()
        .filter(|eval| eval.page_pattern_match == Some(true))
        .count();

    let table_total = refs
        .iter()
        .filter(|reference| reference.reference.starts_with("Table "))
        .count();
    let table_ok = refs
        .iter()
        .zip(evals.iter())
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

    let exact_ref_total = refs.len();
    let exact_ref_hits = refs
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
                    params![reference.doc_id, reference.reference],
                    |_| Ok(1_i64),
                )
                .is_ok()
        })
        .count();

    let keyword_total = refs
        .iter()
        .filter(|reference| !reference.must_match_terms.is_empty())
        .count();
    let keyword_ok = refs
        .iter()
        .zip(evals.iter())
        .filter(|(reference, eval)| !reference.must_match_terms.is_empty() && eval.has_any_term)
        .count();

    let citation_ok = evals
        .iter()
        .filter(|eval| {
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
        result: if found == total { "pass" } else { "failed" }.to_string(),
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
        result: if exact_ref_hits == exact_ref_total {
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

    let lineage_ok = evals
        .iter()
        .filter(|eval| eval.found)
        .all(|eval| eval.lineage_complete);
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

    let hierarchy_expected_total = refs
        .iter()
        .filter(|reference| has_hierarchy_expectations(reference))
        .count();
    let hierarchy_expected_ok = refs
        .iter()
        .zip(evals.iter())
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

    Ok(checks)
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

fn collect_hierarchy_stats(
    connection: &Connection,
    origin_node_id: &str,
) -> Result<(Option<String>, usize, usize, usize)> {
    let parent_ref = connection
        .query_row(
            "
            SELECT p.ref
            FROM nodes n
            LEFT JOIN nodes p ON p.node_id = n.parent_node_id
            WHERE n.node_id = ?1
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

    node_type_ok && parent_ref_ok && min_rows_ok && min_cols_ok && min_list_items_ok
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
