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

