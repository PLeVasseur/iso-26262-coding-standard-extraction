use super::*;

#[allow(clippy::too_many_arguments)]
pub fn insert_structured_chunks_for_pdf(
    chunk_statement: &mut rusqlite::Statement<'_>,
    node_statement: &mut rusqlite::Statement<'_>,
    doc_id: &str,
    source_hash: &str,
    structured_chunks: &[StructuredChunkDraft],
    page_printed_labels: &[Option<String>],
    regexes: &IngestRegexes,
    state: &mut PdfNodeState,
    stats: &mut ChunkInsertStats,
) -> Result<()> {
    let mut node_key_counts = HashMap::<String, i64>::new();
    let mut chunk_key_counts = HashMap::<String, i64>::new();
    let mut chunk_seq_by_ref = HashMap::<String, i64>::new();

    for chunk in structured_chunks {
        let origin_node_type = chunk_origin_node_type(chunk.chunk_type, &chunk.reference);
        let parent_node_id = match chunk.chunk_type {
            ChunkType::Table => state
                .last_clause_node_id
                .clone()
                .unwrap_or_else(|| state.document_node_id.clone()),
            ChunkType::Clause => {
                find_parent_clause_node_id(&chunk.reference, &state.clause_ref_to_node_id)
                    .or_else(|| {
                        find_section_node_id(&chunk.reference, &state.section_ref_to_node_id)
                    })
                    .unwrap_or_else(|| state.document_node_id.clone())
            }
            ChunkType::Annex => state.document_node_id.clone(),
        };

        let ref_key = sanitize_ref_for_id(&chunk.reference);
        let node_count = node_key_counts
            .entry(format!("{}:{}", origin_node_type.as_str(), ref_key))
            .and_modify(|value| *value += 1)
            .or_insert(1);

        let origin_node_id = format!(
            "{}:node:{}:{}:{:03}",
            doc_id,
            origin_node_type.as_str(),
            ref_key,
            node_count
        );

        let ancestor_path = build_ancestor_path(
            Some(&parent_node_id),
            &state.node_paths,
            origin_node_type,
            &chunk.reference,
            &chunk.heading,
        );
        let structured_seq = {
            let next = chunk_seq_by_ref
                .entry(chunk.reference.clone())
                .and_modify(|value| *value += 1)
                .or_insert(1);
            *next
        };
        let node_anchor_type = match origin_node_type {
            NodeType::Clause | NodeType::Subclause | NodeType::Annex | NodeType::Table => {
                Some("clause")
            }
            _ => None,
        };
        let node_anchor_order = node_anchor_type.map(|_| structured_seq);
        let node_anchor_id = node_anchor_type.map(|anchor_type| {
            build_citation_anchor_id(
                doc_id,
                &chunk.reference,
                anchor_type,
                Some(&chunk.reference),
                node_anchor_order,
            )
        });

        insert_node(
            node_statement,
            &origin_node_id,
            Some(&parent_node_id),
            doc_id,
            origin_node_type,
            Some(&chunk.reference),
            Some(&chunk.ref_path),
            Some(&chunk.heading),
            state.node_order_index,
            Some(chunk.page_start),
            Some(chunk.page_end),
            Some(&chunk.text),
            source_hash,
            &ancestor_path,
            node_anchor_type,
            node_anchor_type.map(|_| chunk.reference.as_str()),
            node_anchor_type.map(|_| chunk.reference.as_str()),
            node_anchor_order,
            node_anchor_id.as_deref(),
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

        state
            .node_paths
            .insert(origin_node_id.clone(), ancestor_path.clone());
        state.node_order_index += 1;
        stats.nodes_total += 1;
        increment_node_type_stat(stats, origin_node_type);

        if matches!(origin_node_type, NodeType::Clause | NodeType::Subclause) {
            state
                .clause_ref_to_node_id
                .insert(chunk.reference.clone(), origin_node_id.clone());
            state.last_clause_node_id = Some(origin_node_id.clone());
        }

        let (table_md, table_csv, parsed_table_rows) = if chunk.chunk_type == ChunkType::Table {
            let parsed =
                parse_table_rows(&chunk.text, &chunk.heading, &regexes.table_cell_split_regex);
            (parsed.markdown.clone(), parsed.csv.clone(), Some(parsed))
        } else {
            (None::<String>, None::<String>, None::<ParsedTableRows>)
        };

        if parsed_table_rows
            .as_ref()
            .is_some_and(|parsed| parsed.used_fallback)
        {
            stats.table_raw_fallback_count += 1;
        }

        if let Some(parsed) = parsed_table_rows.as_ref() {
            stats.table_sparse_rows_count += parsed.quality.sparse_rows_count;
            stats.table_overloaded_rows_count += parsed.quality.overloaded_rows_count;
            stats.table_rows_with_markers_count += parsed.quality.rows_with_markers_count;
            stats.table_rows_with_descriptions_count += parsed.quality.rows_with_descriptions_count;
            stats.table_marker_expected_count += parsed.quality.marker_expected_count;
            stats.table_marker_observed_count += parsed.quality.marker_observed_count;
        }

        let chunk_count = chunk_key_counts
            .entry(format!("{}:{}", chunk.chunk_type.as_str(), ref_key))
            .and_modify(|value| *value += 1)
            .or_insert(1);

        let chunk_id = format!(
            "{}:{}:{}:{:03}",
            doc_id,
            chunk.chunk_type.as_str(),
            ref_key,
            chunk_count
        );
        let chunk_anchor_type = Some("clause");
        let chunk_anchor_order = Some(structured_seq);
        let chunk_anchor_id = Some(build_citation_anchor_id(
            doc_id,
            &chunk.reference,
            "clause",
            Some(&chunk.reference),
            chunk_anchor_order,
        ));
        let (chunk_page_printed_start, chunk_page_printed_end) =
            printed_page_labels_for_range(page_printed_labels, chunk.page_start, chunk.page_end);

        chunk_statement.execute(params![
            chunk_id,
            doc_id,
            chunk.chunk_type.as_str(),
            &chunk.reference,
            &chunk.ref_path,
            &chunk.heading,
            structured_seq,
            chunk.page_start,
            chunk.page_end,
            &chunk_page_printed_start,
            &chunk_page_printed_end,
            &chunk.text,
            &table_md,
            &table_csv,
            source_hash,
            &origin_node_id,
            origin_node_type.as_str(),
            &ancestor_path,
            chunk_anchor_type,
            chunk_anchor_type.map(|_| chunk.reference.as_str()),
            chunk_anchor_type.map(|_| chunk.reference.as_str()),
            chunk_anchor_order,
            chunk_anchor_id.as_deref()
        ])?;

        stats.structured_chunks_inserted += 1;
        match chunk.chunk_type {
            ChunkType::Clause => stats.clause_chunks_inserted += 1,
            ChunkType::Table => stats.table_chunks_inserted += 1,
            ChunkType::Annex => stats.annex_chunks_inserted += 1,
        }

        if let Some(parsed) = parsed_table_rows {
            insert_table_child_nodes(
                node_statement,
                doc_id,
                &origin_node_id,
                &ancestor_path,
                &chunk.reference,
                &parsed,
                chunk.page_start,
                chunk.page_end,
                source_hash,
                &mut state.node_order_index,
                stats,
            )?;
        }

        if matches!(
            origin_node_type,
            NodeType::Clause | NodeType::Subclause | NodeType::Annex
        ) {
            let paragraphs = parse_paragraphs(
                &chunk.text,
                &chunk.heading,
                &regexes.list_item_regex,
                &regexes.note_item_regex,
            );
            if !paragraphs.is_empty() {
                insert_paragraph_nodes(
                    node_statement,
                    doc_id,
                    &origin_node_id,
                    &ancestor_path,
                    &chunk.reference,
                    &paragraphs,
                    chunk.page_start,
                    chunk.page_end,
                    source_hash,
                    &mut state.node_order_index,
                    stats,
                )?;
            }

            let note_items = parse_note_items(
                &chunk.text,
                &chunk.heading,
                &regexes.note_item_regex,
                &regexes.list_item_regex,
            );
            if !note_items.is_empty() {
                insert_note_nodes(
                    node_statement,
                    doc_id,
                    &origin_node_id,
                    &ancestor_path,
                    &chunk.reference,
                    &note_items,
                    chunk.page_start,
                    chunk.page_end,
                    source_hash,
                    &mut state.node_order_index,
                    stats,
                )?;
            }

            let (list_items, list_fallback, had_list_candidates) = parse_list_items(
                &chunk.text,
                &chunk.heading,
                &regexes.list_item_regex,
                &regexes.note_item_regex,
            );
            if had_list_candidates {
                stats.list_parse_candidate_count += 1;
            }
            if !list_items.is_empty() {
                insert_list_nodes(
                    node_statement,
                    doc_id,
                    &origin_node_id,
                    &ancestor_path,
                    &chunk.reference,
                    &list_items,
                    chunk.page_start,
                    chunk.page_end,
                    source_hash,
                    &mut state.node_order_index,
                    stats,
                )?;
            } else if list_fallback {
                stats.list_parse_fallback_count += 1;
            }

            let requirement_atoms = parse_requirement_atoms(
                &chunk.text,
                &chunk.heading,
                &regexes.requirement_split_regex,
                &regexes.requirement_keyword_regex,
            );
            if !requirement_atoms.is_empty() {
                insert_requirement_atom_nodes(
                    node_statement,
                    doc_id,
                    &origin_node_id,
                    &ancestor_path,
                    &chunk.reference,
                    &requirement_atoms,
                    chunk.page_start,
                    chunk.page_end,
                    source_hash,
                    &mut state.node_order_index,
                    stats,
                )?;
            }
        }
    }

    Ok(())
}
