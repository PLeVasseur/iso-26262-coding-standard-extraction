use super::*;

pub fn compute_list_semantics_metrics(
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
pub struct TableSemanticsMetrics {
    pub table_cells_total: usize,
    pub table_cells_semantics_complete: usize,
    pub invalid_span_count: usize,
    pub header_cells_total: usize,
    pub header_cells_flagged: usize,
    pub one_cell_rows: usize,
    pub total_table_rows: usize,
    pub targeted_semantic_miss_count: usize,
    pub asil_one_cell_rows: usize,
    pub asil_total_rows: usize,
}

pub fn compute_table_semantics_metrics(connection: &Connection) -> Result<TableSemanticsMetrics> {
    let (table_cells_total, table_cells_semantics_complete, invalid_span_count): (
        usize,
        usize,
        usize,
    ) = connection.query_row(
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
pub struct CitationParityComputation {
    pub baseline_run_id: Option<String>,
    pub baseline_checksum: Option<String>,
    pub baseline_created: bool,
    pub baseline_missing: bool,
    pub target_linked_total: usize,
    pub comparable_total: usize,
    pub top1_parity: Option<f64>,
    pub top3_containment: Option<f64>,
    pub page_range_parity: Option<f64>,
}

#[derive(Debug, Default)]
pub struct SemanticEmbeddingMetrics {
    pub active_model_id: String,
    pub embedding_dim: Option<usize>,
    pub eligible_chunks: usize,
    pub embedded_chunks: usize,
    pub stale_rows: usize,
    pub embedding_rows_for_active_model: usize,
}

pub fn compute_semantic_embedding_metrics(connection: &Connection) -> Result<SemanticEmbeddingMetrics> {
    let has_embedding_tables: i64 = connection.query_row(
        "
        SELECT COUNT(*)
        FROM sqlite_master
        WHERE type = 'table'
          AND name IN ('embedding_models', 'chunk_embeddings')
        ",
        [],
        |row| row.get(0),
    )?;
    if has_embedding_tables < 2 {
        return Ok(SemanticEmbeddingMetrics {
            active_model_id: DEFAULT_MODEL_ID.to_string(),
            ..SemanticEmbeddingMetrics::default()
        });
    }

    let active_model = connection
        .query_row(
            "
            SELECT model_id, dimensions
            FROM embedding_models
            ORDER BY CASE WHEN model_id = ?1 THEN 0 ELSE 1 END, created_at DESC, model_id ASC
            LIMIT 1
            ",
            params![DEFAULT_MODEL_ID],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1).ok().map(|value| value as usize),
                ))
            },
        )
        .ok();

    let (active_model_id, embedding_dim) =
        active_model.unwrap_or_else(|| (DEFAULT_MODEL_ID.to_string(), None));

    let mut statement = connection.prepare(
        "
        SELECT
          chunk_id,
          lower(COALESCE(type, '')),
          COALESCE(ref, ''),
          COALESCE(heading, ''),
          text,
          table_md
        FROM chunks
        ORDER BY chunk_id ASC
        ",
    )?;
    let mut rows = statement.query([])?;

    let mut eligible_chunks = 0usize;
    let mut embedded_chunks = 0usize;
    let mut stale_rows = 0usize;

    while let Some(row) = rows.next()? {
        let chunk_id = row.get::<_, String>(0)?;
        let chunk_type = row.get::<_, String>(1)?;
        let reference = row.get::<_, String>(2)?;
        let heading = row.get::<_, String>(3)?;
        let text = row.get::<_, Option<String>>(4)?;
        let table_md = row.get::<_, Option<String>>(5)?;

        let payload = chunk_payload_for_embedding(
            &chunk_type,
            &reference,
            &heading,
            text.as_deref(),
            table_md.as_deref(),
        );
        let Some(payload) = payload else {
            continue;
        };

        eligible_chunks += 1;
        let expected_hash = embedding_text_hash(&payload);

        let existing = connection
            .query_row(
                "
                SELECT text_hash, embedding_dim
                FROM chunk_embeddings
                WHERE chunk_id = ?1 AND model_id = ?2
                LIMIT 1
                ",
                params![chunk_id, active_model_id],
                |existing_row| {
                    Ok((
                        existing_row.get::<_, String>(0)?,
                        existing_row
                            .get::<_, i64>(1)
                            .ok()
                            .map(|value| value as usize),
                    ))
                },
            )
            .ok();

        match existing {
            Some((actual_hash, actual_dim)) => {
                embedded_chunks += 1;
                let stale_hash = actual_hash != expected_hash;
                let stale_dim = match (embedding_dim, actual_dim) {
                    (Some(expected), Some(actual)) => expected != actual,
                    _ => false,
                };
                if stale_hash || stale_dim {
                    stale_rows += 1;
                }
            }
            None => {
                stale_rows += 1;
            }
        }
    }

    let embedding_rows_for_active_model: usize = connection.query_row(
        "
        SELECT COUNT(*)
        FROM chunk_embeddings
        WHERE model_id = ?1
        ",
        params![active_model_id],
        |row| Ok(row.get::<_, i64>(0)? as usize),
    )?;

    Ok(SemanticEmbeddingMetrics {
        active_model_id,
        embedding_dim,
        eligible_chunks,
        embedded_chunks,
        stale_rows,
        embedding_rows_for_active_model,
    })
}
