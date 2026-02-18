fn fetch_descendants(connection: &Connection, origin_node_id: &str) -> Result<Vec<DescendantNode>> {
    let mut statement = connection.prepare(
        "
        WITH RECURSIVE descendants(
          node_id, parent_node_id, node_type, ref, heading,
          order_index, page_pdf_start, page_pdf_end, text, depth
        ) AS (
          SELECT
            n.node_id,
            n.parent_node_id,
            n.node_type,
            n.ref,
            n.heading,
            n.order_index,
            n.page_pdf_start,
            n.page_pdf_end,
            n.text,
            1
          FROM nodes n
          WHERE n.parent_node_id = ?1

          UNION ALL

          SELECT
            n.node_id,
            n.parent_node_id,
            n.node_type,
            n.ref,
            n.heading,
            n.order_index,
            n.page_pdf_start,
            n.page_pdf_end,
            n.text,
            d.depth + 1
          FROM nodes n
          JOIN descendants d ON n.parent_node_id = d.node_id
          WHERE d.depth < 8
        )
        SELECT
          node_id,
          parent_node_id,
          node_type,
          ref,
          heading,
          order_index,
          page_pdf_start,
          page_pdf_end,
          substr(COALESCE(text, ''), 1, 180)
        FROM descendants
        ORDER BY depth, order_index, node_id
        LIMIT 256
        ",
    )?;

    let mut rows = statement.query(params![origin_node_id])?;
    let mut descendants = Vec::new();

    while let Some(row) = rows.next()? {
        descendants.push(DescendantNode {
            node_id: row.get(0)?,
            parent_node_id: row.get(1)?,
            node_type: row.get(2)?,
            reference: row.get(3)?,
            heading: row.get(4)?,
            order_index: row.get(5)?,
            page_pdf_start: row.get(6)?,
            page_pdf_end: row.get(7)?,
            text_preview: row
                .get::<_, Option<String>>(8)?
                .map(|value| condense_whitespace(&value)),
        });
    }

    Ok(descendants)
}

fn to_fts_query(query_text: &str) -> String {
    query_text
        .split_whitespace()
        .filter(|token| !token.trim().is_empty())
        .map(|token| format!("\"{}\"", token.replace('"', "")))
        .collect::<Vec<String>>()
        .join(" ")
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

fn render_citation(candidate: &QueryCandidate) -> String {
    let reference = if candidate.reference.is_empty() {
        "(unreferenced chunk)".to_string()
    } else {
        candidate.reference.clone()
    };

    let reference_with_anchor = match (
        candidate.anchor_type.as_deref(),
        candidate.anchor_label_norm.as_deref(),
    ) {
        (Some("marker"), Some(label)) if !label.is_empty() => {
            let base = marker_base_reference(&reference);
            if label.starts_with("NOTE") {
                format!("{base}, {label}")
            } else {
                format!("{base}({label})")
            }
        }
        (Some("paragraph"), Some(label)) if !label.is_empty() => {
            let base = marker_base_reference(&reference);
            format!("{base}, para {label}")
        }
        _ => reference,
    };

    format!(
        "ISO 26262-{}:{}, {}, PDF pages {}",
        candidate.part,
        candidate.year,
        reference_with_anchor,
        format_page_range(candidate.page_pdf_start, candidate.page_pdf_end)
    )
}

fn marker_base_reference(reference: &str) -> String {
    if let Some((base, _)) = reference.split_once(" item ") {
        return base.to_string();
    }

    if let Some((base, _)) = reference.split_once(" note ") {
        return base.to_string();
    }

    if let Some((base, _)) = reference.split_once(" para ") {
        return base.to_string();
    }

    if let Some((base, _)) = reference.split_once(" row ") {
        return base.to_string();
    }

    reference.to_string()
}

fn resolve_parent_ref(
    connection: &Connection,
    origin_node_id: Option<&str>,
) -> Result<Option<String>> {
    let Some(origin_node_id) = origin_node_id else {
        return Ok(None);
    };

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

    Ok(parent_ref)
}

fn condense_whitespace(input: &str) -> String {
    input.split_whitespace().collect::<Vec<&str>>().join(" ")
}
