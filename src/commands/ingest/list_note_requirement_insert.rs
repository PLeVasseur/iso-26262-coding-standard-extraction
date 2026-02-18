#[allow(clippy::too_many_arguments)]
fn insert_list_nodes(
    node_statement: &mut rusqlite::Statement<'_>,
    doc_id: &str,
    parent_node_id: &str,
    parent_path: &str,
    reference: &str,
    list_items: &[ListItemDraft],
    page_start: i64,
    page_end: i64,
    source_hash: &str,
    node_order_index: &mut i64,
    stats: &mut ChunkInsertStats,
) -> Result<()> {
    let list_node_id = format!("{}:list:001", parent_node_id);
    let list_ref = format!("{} list", reference);
    let list_heading = format!("{} list", reference);
    let list_path = format!("{} > list:{}", parent_path, reference);

    insert_node(
        node_statement,
        &list_node_id,
        Some(parent_node_id),
        doc_id,
        NodeType::List,
        Some(&list_ref),
        Some(&list_ref),
        Some(&list_heading),
        *node_order_index,
        Some(page_start),
        Some(page_end),
        None,
        source_hash,
        &list_path,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )?;

    *node_order_index += 1;
    stats.nodes_total += 1;
    increment_node_type_stat(stats, NodeType::List);

    let mut last_item_node_id_by_depth = HashMap::<i64, String>::new();
    let mut item_index_by_parent = HashMap::<String, i64>::new();

    for (item_idx, item) in list_items.iter().enumerate() {
        let mut effective_depth = item.depth.max(1);
        while effective_depth > 1
            && !last_item_node_id_by_depth.contains_key(&(effective_depth - 1))
        {
            effective_depth -= 1;
        }

        let parent_item_node_id = if effective_depth == 1 {
            list_node_id.clone()
        } else {
            last_item_node_id_by_depth
                .get(&(effective_depth - 1))
                .cloned()
                .unwrap_or_else(|| list_node_id.clone())
        };

        last_item_node_id_by_depth.retain(|depth, _| *depth < effective_depth);

        let item_index = {
            let entry = item_index_by_parent
                .entry(parent_item_node_id.clone())
                .or_insert(0);
            *entry += 1;
            *entry
        };

        let list_item_node_id = format!("{}:item:{:03}", list_node_id, item_idx + 1);
        let list_item_ref = format!("{} item {}", reference, item_idx + 1);
        let list_item_heading = format!("{} {}", item.marker, item.text);
        let list_item_path = format!(
            "{} > list_item:d{}:{}",
            list_path, effective_depth, item_index
        );
        let marker_order = (item_idx + 1) as i64;
        let marker_anchor_id = build_citation_anchor_id(
            doc_id,
            reference,
            "marker",
            Some(&item.marker_norm),
            Some(marker_order),
        );

        insert_node(
            node_statement,
            &list_item_node_id,
            Some(&parent_item_node_id),
            doc_id,
            NodeType::ListItem,
            Some(&list_item_ref),
            Some(&list_item_ref),
            Some(&list_item_heading),
            *node_order_index,
            Some(page_start),
            Some(page_end),
            Some(&item.text),
            source_hash,
            &list_item_path,
            Some("marker"),
            Some(&item.marker),
            Some(&item.marker_norm),
            Some(marker_order),
            Some(&marker_anchor_id),
            Some(effective_depth),
            Some(item.marker_style.as_str()),
            Some(item_index),
            None,
            None,
            None,
            None,
            None,
            None,
        )?;

        last_item_node_id_by_depth.insert(effective_depth, list_item_node_id.clone());

        *node_order_index += 1;
        stats.nodes_total += 1;
        increment_node_type_stat(stats, NodeType::ListItem);
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_note_nodes(
    node_statement: &mut rusqlite::Statement<'_>,
    doc_id: &str,
    parent_node_id: &str,
    parent_path: &str,
    reference: &str,
    note_items: &[NoteItemDraft],
    page_start: i64,
    page_end: i64,
    source_hash: &str,
    node_order_index: &mut i64,
    stats: &mut ChunkInsertStats,
) -> Result<()> {
    let note_node_id = format!("{}:note:001", parent_node_id);
    let note_ref = format!("{} note", reference);
    let note_heading = format!("{} note", reference);
    let note_path = format!("{} > note:{}", parent_path, reference);

    insert_node(
        node_statement,
        &note_node_id,
        Some(parent_node_id),
        doc_id,
        NodeType::Note,
        Some(&note_ref),
        Some(&note_ref),
        Some(&note_heading),
        *node_order_index,
        Some(page_start),
        Some(page_end),
        None,
        source_hash,
        &note_path,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )?;

    *node_order_index += 1;
    stats.nodes_total += 1;
    increment_node_type_stat(stats, NodeType::Note);

    for (item_idx, item) in note_items.iter().enumerate() {
        let note_item_node_id = format!("{}:item:{:03}", note_node_id, item_idx + 1);
        let note_item_ref = format!("{} note {}", reference, item_idx + 1);
        let note_item_heading = format!("{} {}", item.marker, item.text);
        let note_item_path = format!("{} > note_item:{}", note_path, item_idx + 1);
        let marker_order = (item_idx + 1) as i64;
        let marker_anchor_id = build_citation_anchor_id(
            doc_id,
            reference,
            "marker",
            Some(&item.marker_norm),
            Some(marker_order),
        );

        insert_node(
            node_statement,
            &note_item_node_id,
            Some(&note_node_id),
            doc_id,
            NodeType::NoteItem,
            Some(&note_item_ref),
            Some(&note_item_ref),
            Some(&note_item_heading),
            *node_order_index,
            Some(page_start),
            Some(page_end),
            Some(&item.text),
            source_hash,
            &note_item_path,
            Some("marker"),
            Some(&item.marker),
            Some(&item.marker_norm),
            Some(marker_order),
            Some(&marker_anchor_id),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;

        *node_order_index += 1;
        stats.nodes_total += 1;
        increment_node_type_stat(stats, NodeType::NoteItem);
    }

    Ok(())
}

fn parse_requirement_atoms(
    text: &str,
    heading: &str,
    split_regex: &Regex,
    keyword_regex: &Regex,
) -> Vec<String> {
    let body = extract_body_lines(text, heading).join(" ");

    split_regex
        .split(&body)
        .map(str::trim)
        .filter(|sentence| !sentence.is_empty())
        .filter(|sentence| keyword_regex.is_match(sentence))
        .map(ToOwned::to_owned)
        .collect::<Vec<String>>()
}

#[allow(clippy::too_many_arguments)]
fn insert_requirement_atom_nodes(
    node_statement: &mut rusqlite::Statement<'_>,
    doc_id: &str,
    parent_node_id: &str,
    parent_path: &str,
    reference: &str,
    atoms: &[String],
    page_start: i64,
    page_end: i64,
    source_hash: &str,
    node_order_index: &mut i64,
    stats: &mut ChunkInsertStats,
) -> Result<()> {
    for (index, atom) in atoms.iter().enumerate() {
        let node_id = format!("{}:req:{:03}", parent_node_id, index + 1);
        let atom_ref = format!("{} req {}", reference, index + 1);
        let atom_path = format!("{} > requirement_atom:{}", parent_path, index + 1);
        let atom_heading = format!("Requirement atom {}", index + 1);

        insert_node(
            node_statement,
            &node_id,
            Some(parent_node_id),
            doc_id,
            NodeType::RequirementAtom,
            Some(&atom_ref),
            Some(&atom_ref),
            Some(&atom_heading),
            *node_order_index,
            Some(page_start),
            Some(page_end),
            Some(atom),
            source_hash,
            &atom_path,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )?;

        *node_order_index += 1;
        stats.nodes_total += 1;
        increment_node_type_stat(stats, NodeType::RequirementAtom);
    }

    Ok(())
}

