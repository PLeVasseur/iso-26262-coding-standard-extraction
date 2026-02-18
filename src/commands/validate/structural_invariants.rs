use super::*;

pub fn collect_structural_invariants(connection: &Connection) -> Result<StructuralInvariantSummary> {
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

pub fn collect_asil_table_alignment(
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

pub fn analyze_asil_marker_row(cells: &[String], summary: &mut AsilTableAlignmentSummary) {
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

pub fn evaluate_asil_table_alignment(summary: &AsilTableAlignmentSummary) -> &'static str {
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

pub fn looks_like_table_marker(value: &str) -> bool {
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

pub fn is_malformed_marker_description(description: &str) -> bool {
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

pub fn contains_asil_rating(cell_text: &str) -> bool {
    cell_text
        .split_whitespace()
        .map(|token| token.trim_matches(['(', ')', '.', ':', ';', ',']))
        .any(is_asil_rating_token)
}

pub fn is_asil_rating_token(token: &str) -> bool {
    matches!(token, "+" | "++" | "-" | "--" | "+/-" | "+/−" | "−/+" | "o")
}

pub fn query_violation_count(connection: &Connection, sql: &str) -> Result<i64> {
    let count = connection.query_row(sql, [], |row| row.get::<_, i64>(0))?;
    Ok(count)
}

pub fn collect_hierarchy_stats(
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

pub fn evaluate_hierarchy_expectations(
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

pub fn build_hierarchy_metrics(evals: &[ReferenceEvaluation]) -> HierarchyMetrics {
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

pub fn has_hierarchy_expectations(reference: &GoldReference) -> bool {
    reference.expected_node_type.is_some()
        || reference.expected_parent_ref.is_some()
        || reference.expected_min_rows.is_some()
        || reference.expected_min_cols.is_some()
        || reference.expected_min_list_items.is_some()
        || reference.expected_anchor_type.is_some()
        || reference.expected_marker_label.is_some()
        || reference.expected_paragraph_index.is_some()
}

pub fn normalize_anchor_label(value: &str) -> String {
    value
        .trim()
        .trim_end_matches([')', '.', ':', ';'])
        .replace('–', "-")
        .replace('—', "-")
        .to_ascii_lowercase()
}

