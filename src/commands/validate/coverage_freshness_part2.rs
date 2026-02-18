use super::*;

pub fn empty_table_scorecard() -> TableQualityScorecard {
    build_table_quality_scorecard(None, TableQualityCounters::default())
}

pub fn build_table_quality_scorecard(
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

pub fn ratio(numerator: usize, denominator: usize) -> Option<f64> {
    if denominator == 0 {
        None
    } else {
        Some(numerator as f64 / denominator as f64)
    }
}

pub fn collect_evaluable_doc_ids(connection: &Connection) -> Result<HashSet<String>> {
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

pub fn skipped_reference_evaluation() -> ReferenceEvaluation {
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

pub fn evaluate_reference(
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

