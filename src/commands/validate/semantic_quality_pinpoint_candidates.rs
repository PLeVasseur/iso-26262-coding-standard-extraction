use super::*;

pub fn collect_pinpoint_unit_candidates(
    connection: &Connection,
    chunk_id: &str,
    parent_anchor_id: Option<&str>,
) -> Result<Vec<PinpointUnitEval>> {
    let (chunk_text, table_md, table_node_id, chunk_anchor_id) = connection
        .query_row(
            "
            SELECT COALESCE(text, ''), COALESCE(table_md, ''), origin_node_id, citation_anchor_id
            FROM chunks
            WHERE chunk_id = ?1
            LIMIT 1
            ",
            [chunk_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            },
        )
        .optional()?
        .unwrap_or_else(|| (String::new(), String::new(), None, None));

    let parent_anchor = parent_anchor_id
        .map(str::to_string)
        .or(chunk_anchor_id)
        .unwrap_or_default();

    let mut units = Vec::<PinpointUnitEval>::new();
    let has_table_structure = table_node_id.is_some() || !table_md.trim().is_empty();
    if !has_table_structure {
        units.extend(sentence_units_for_pinpoint(
            chunk_id,
            &chunk_text,
            &parent_anchor,
        ));
    }

    if let Some(table_node_id) = table_node_id.as_deref() {
        units.extend(table_node_units_for_pinpoint(
            connection,
            chunk_id,
            table_node_id,
            &parent_anchor,
        )?);
    }
    if units.is_empty() && !table_md.trim().is_empty() {
        units.extend(table_markdown_units_for_pinpoint(
            chunk_id,
            &table_md,
            &parent_anchor,
        ));
    }
    if units.is_empty() {
        units.extend(sentence_units_for_pinpoint(
            chunk_id,
            &chunk_text,
            &parent_anchor,
        ));
    }

    Ok(units)
}

pub fn sentence_units_for_pinpoint(
    chunk_id: &str,
    chunk_text: &str,
    parent_anchor: &str,
) -> Vec<PinpointUnitEval> {
    let mut out = Vec::<PinpointUnitEval>::new();
    let mut start = 0usize;
    for (index, character) in chunk_text.char_indices() {
        if !matches!(character, '.' | '!' | '?' | ';' | '\n') {
            continue;
        }
        let end = index + character.len_utf8();
        let snippet = condense_whitespace(chunk_text.get(start..end).unwrap_or_default());
        if snippet.len() >= 24 {
            let unit_id = format!("{chunk_id}:sentence:{:03}", out.len() + 1);
            out.push(PinpointUnitEval {
                unit_id,
                unit_type: "sentence_window".to_string(),
                score: 0.0,
                text_preview: snippet,
                row_key: None,
                token_signature: String::new(),
                citation_anchor_compatible: pinpoint_anchor_compatible(
                    Some(parent_anchor),
                    Some(parent_anchor),
                ),
            });
        }
        start = end;
    }
    out
}

pub fn table_node_units_for_pinpoint(
    connection: &Connection,
    chunk_id: &str,
    table_node_id: &str,
    parent_anchor: &str,
) -> Result<Vec<PinpointUnitEval>> {
    let mut statement = connection.prepare(
        "
        SELECT node_id, node_type, COALESCE(text, ''), row_idx, citation_anchor_id
        FROM nodes
        WHERE table_node_id = ?1
          AND node_type IN ('table_row', 'table_cell')
        ORDER BY row_idx ASC, col_idx ASC, node_id ASC
        LIMIT 512
        ",
    )?;
    let mut rows = statement.query([table_node_id])?;

    let mut units = Vec::<PinpointUnitEval>::new();
    while let Some(row) = rows.next()? {
        let text = condense_whitespace(&row.get::<_, String>(2)?);
        if text.is_empty() {
            continue;
        }

        let anchor = row.get::<_, Option<String>>(4)?;
        let row_idx = row.get::<_, Option<i64>>(3)?;
        units.push(PinpointUnitEval {
            unit_id: row.get::<_, String>(0)?,
            unit_type: row.get::<_, String>(1)?,
            score: 0.0,
            text_preview: text,
            row_key: row_idx.map(|value| format!("{chunk_id}:{value}")),
            token_signature: String::new(),
            citation_anchor_compatible: pinpoint_anchor_compatible(
                anchor.as_deref(),
                Some(parent_anchor),
            ),
        });
    }
    Ok(units)
}

pub fn table_markdown_units_for_pinpoint(
    chunk_id: &str,
    table_md: &str,
    parent_anchor: &str,
) -> Vec<PinpointUnitEval> {
    let mut out = Vec::<PinpointUnitEval>::new();
    let mut row_idx = 0i64;
    for line in table_md.lines() {
        if !line.trim_start().starts_with('|') {
            continue;
        }
        let text = condense_whitespace(line);
        if text.is_empty() {
            continue;
        }
        row_idx += 1;
        out.push(PinpointUnitEval {
            unit_id: format!("{chunk_id}:table_md_row:{row_idx:03}"),
            unit_type: "table_row".to_string(),
            score: 0.0,
            text_preview: text,
            row_key: Some(format!("{chunk_id}:{row_idx}")),
            token_signature: String::new(),
            citation_anchor_compatible: pinpoint_anchor_compatible(
                Some(parent_anchor),
                Some(parent_anchor),
            ),
        });
    }
    out
}
