fn increment_node_type_stat(stats: &mut ChunkInsertStats, node_type: NodeType) {
    match node_type {
        NodeType::SectionHeading => {}
        NodeType::Clause => stats.clause_nodes_inserted += 1,
        NodeType::Subclause => stats.subclause_nodes_inserted += 1,
        NodeType::Annex => stats.annex_nodes_inserted += 1,
        NodeType::Paragraph => stats.paragraph_nodes_inserted += 1,
        NodeType::Table => stats.table_nodes_inserted += 1,
        NodeType::TableRow => stats.table_row_nodes_inserted += 1,
        NodeType::TableCell => stats.table_cell_nodes_inserted += 1,
        NodeType::List => stats.list_nodes_inserted += 1,
        NodeType::ListItem => stats.list_item_nodes_inserted += 1,
        NodeType::Note => stats.note_nodes_inserted += 1,
        NodeType::NoteItem => stats.note_item_nodes_inserted += 1,
        NodeType::RequirementAtom => stats.requirement_atom_nodes_inserted += 1,
        NodeType::Document | NodeType::Page => {}
    }
}

fn chunk_origin_node_type(chunk_type: ChunkType, reference: &str) -> NodeType {
    match chunk_type {
        ChunkType::Clause => {
            let depth = reference.split('.').count();
            if depth > 2 {
                NodeType::Subclause
            } else {
                NodeType::Clause
            }
        }
        ChunkType::Table => NodeType::Table,
        ChunkType::Annex => NodeType::Annex,
    }
}

fn find_parent_clause_node_id(
    reference: &str,
    clause_ref_to_node_id: &HashMap<String, String>,
) -> Option<String> {
    let mut parts = reference.split('.').collect::<Vec<&str>>();
    while parts.len() > 1 {
        parts.pop();
        let parent_ref = parts.join(".");
        if let Some(parent) = clause_ref_to_node_id.get(&parent_ref) {
            return Some(parent.clone());
        }
    }

    None
}

fn find_section_node_id(
    reference: &str,
    section_ref_to_node_id: &HashMap<String, String>,
) -> Option<String> {
    let section_ref = reference.split('.').next()?.trim();
    if section_ref.is_empty() {
        return None;
    }

    section_ref_to_node_id.get(section_ref).cloned()
}

fn build_ancestor_path(
    parent_node_id: Option<&str>,
    node_paths: &HashMap<String, String>,
    node_type: NodeType,
    reference: &str,
    heading: &str,
) -> String {
    let node_label = if !reference.is_empty() {
        format!("{}:{}", node_type.as_str(), reference)
    } else if !heading.is_empty() {
        format!("{}:{}", node_type.as_str(), heading)
    } else {
        format!("{}:unlabeled", node_type.as_str())
    };

    if let Some(parent) = parent_node_id.and_then(|node_id| node_paths.get(node_id)) {
        format!("{} > {}", parent, node_label)
    } else {
        node_label
    }
}

#[allow(clippy::too_many_arguments)]
fn insert_node(
    statement: &mut rusqlite::Statement<'_>,
    node_id: &str,
    parent_node_id: Option<&str>,
    doc_id: &str,
    node_type: NodeType,
    ref_value: Option<&str>,
    ref_path: Option<&str>,
    heading: Option<&str>,
    order_index: i64,
    page_start: Option<i64>,
    page_end: Option<i64>,
    text: Option<&str>,
    source_hash: &str,
    ancestor_path: &str,
    anchor_type: Option<&str>,
    anchor_label_raw: Option<&str>,
    anchor_label_norm: Option<&str>,
    anchor_order: Option<i64>,
    citation_anchor_id: Option<&str>,
    list_depth: Option<i64>,
    list_marker_style: Option<&str>,
    item_index: Option<i64>,
    table_node_id: Option<&str>,
    row_idx: Option<i64>,
    col_idx: Option<i64>,
    is_header: Option<i64>,
    row_span: Option<i64>,
    col_span: Option<i64>,
) -> Result<()> {
    statement.execute(params![
        node_id,
        parent_node_id,
        doc_id,
        node_type.as_str(),
        ref_value,
        ref_path,
        heading,
        order_index,
        page_start,
        page_end,
        text,
        source_hash,
        ancestor_path,
        anchor_type,
        anchor_label_raw,
        anchor_label_norm,
        anchor_order,
        citation_anchor_id,
        list_depth,
        list_marker_style,
        item_index,
        table_node_id,
        row_idx,
        col_idx,
        is_header,
        row_span,
        col_span
    ])?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_table_child_nodes(
    node_statement: &mut rusqlite::Statement<'_>,
    doc_id: &str,
    table_node_id: &str,
    table_ancestor_path: &str,
    table_reference: &str,
    parsed_table: &ParsedTableRows,
    page_start: i64,
    page_end: i64,
    source_hash: &str,
    node_order_index: &mut i64,
    stats: &mut ChunkInsertStats,
) -> Result<()> {
    let header_row_count = infer_table_header_rows(&parsed_table.rows);

    for (row_idx, row_cells) in parsed_table.rows.iter().enumerate() {
        let row_node_id = format!("{}:row:{:03}", table_node_id, row_idx + 1);
        let row_ref = format!("{} row {}", table_reference, row_idx + 1);
        let row_heading = format!("{} row {}", table_reference, row_idx + 1);
        let row_text = row_cells.join(" | ");
        let row_path = format!("{} > table_row:{}", table_ancestor_path, row_idx + 1);
        let row_order = (row_idx + 1) as i64;
        let row_label = (row_idx + 1).to_string();
        let row_is_header = if row_idx < header_row_count { 1 } else { 0 };
        let row_anchor_id = build_citation_anchor_id(
            doc_id,
            table_reference,
            "table_row",
            Some(&row_label),
            Some(row_order),
        );

        insert_node(
            node_statement,
            &row_node_id,
            Some(table_node_id),
            doc_id,
            NodeType::TableRow,
            Some(&row_ref),
            Some(&row_ref),
            Some(&row_heading),
            *node_order_index,
            Some(page_start),
            Some(page_end),
            Some(&row_text),
            source_hash,
            &row_path,
            Some("table_row"),
            None,
            Some(&row_label),
            Some(row_order),
            Some(&row_anchor_id),
            None,
            None,
            None,
            Some(table_node_id),
            Some((row_idx + 1) as i64),
            None,
            Some(row_is_header),
            Some(1),
            None,
        )?;

        *node_order_index += 1;
        stats.nodes_total += 1;
        increment_node_type_stat(stats, NodeType::TableRow);

        for (col_idx, cell_text) in row_cells.iter().enumerate() {
            let cell_node_id = format!(
                "{}:cell:{:03}:{:03}",
                table_node_id,
                row_idx + 1,
                col_idx + 1
            );
            let cell_ref = format!("{} r{}c{}", table_reference, row_idx + 1, col_idx + 1);
            let cell_heading = format!("{} r{}c{}", table_reference, row_idx + 1, col_idx + 1);
            let cell_path = format!("{} > table_cell:r{}c{}", row_path, row_idx + 1, col_idx + 1);
            let cell_order = ((row_idx * 1000) + col_idx + 1) as i64;
            let cell_label = format!("r{}c{}", row_idx + 1, col_idx + 1);
            let cell_anchor_id = build_citation_anchor_id(
                doc_id,
                table_reference,
                "table_cell",
                Some(&cell_label),
                Some(cell_order),
            );

            insert_node(
                node_statement,
                &cell_node_id,
                Some(&row_node_id),
                doc_id,
                NodeType::TableCell,
                Some(&cell_ref),
                Some(&cell_ref),
                Some(&cell_heading),
                *node_order_index,
                Some(page_start),
                Some(page_end),
                Some(cell_text),
                source_hash,
                &cell_path,
                Some("table_cell"),
                None,
                Some(&cell_label),
                Some(cell_order),
                Some(&cell_anchor_id),
                None,
                None,
                None,
                Some(table_node_id),
                Some((row_idx + 1) as i64),
                Some((col_idx + 1) as i64),
                Some(row_is_header),
                Some(1),
                Some(1),
            )?;

            *node_order_index += 1;
            stats.nodes_total += 1;
            increment_node_type_stat(stats, NodeType::TableCell);
        }
    }

    Ok(())
}
