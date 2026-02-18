use super::*;

pub fn build_quality_checks(
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

pub fn evaluate_max_threshold(value: Option<f64>, max_allowed: f64) -> &'static str {
    match value {
        Some(actual) if actual <= max_allowed => "pass",
        Some(_) => "failed",
        None => "pending",
    }
}

pub fn evaluate_min_threshold(value: Option<f64>, min_allowed: f64) -> &'static str {
    match value {
        Some(actual) if actual >= min_allowed => "pass",
        Some(_) => "failed",
        None => "pending",
    }
}

pub fn summarize_checks(checks: &[QualityCheck]) -> QualitySummary {
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

