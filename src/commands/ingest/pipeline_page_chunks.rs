use super::*;

#[allow(clippy::too_many_arguments)]
pub fn seed_page_chunks_for_pdf(
    chunk_statement: &mut rusqlite::Statement<'_>,
    node_statement: &mut rusqlite::Statement<'_>,
    doc_id: &str,
    source_hash: &str,
    pages: &[String],
    page_printed_labels: &[Option<String>],
    state: &mut PdfNodeState,
    stats: &mut ChunkInsertStats,
) -> Result<()> {
    for (index, page_text) in pages.iter().enumerate() {
        let text = page_text.trim();
        if text.is_empty() {
            continue;
        }

        let page_number = (index + 1) as i64;
        let chunk_id = format!("{}:page:{:04}", doc_id, page_number);
        let page_ref = format!("PDF page {}", page_number);
        let heading = format!("Page {}", page_number);
        let page_printed_label = printed_page_label_for(page_printed_labels, page_number);
        let page_node_id = format!("{}:node:page:{:04}", doc_id, page_number);
        let page_ancestor_path = build_ancestor_path(
            Some(&state.document_node_id),
            &state.node_paths,
            NodeType::Page,
            &page_ref,
            &heading,
        );

        insert_node(
            node_statement,
            &page_node_id,
            Some(&state.document_node_id),
            doc_id,
            NodeType::Page,
            Some(&page_ref),
            Some(&page_ref),
            Some(&heading),
            state.node_order_index,
            Some(page_number),
            Some(page_number),
            Some(text),
            source_hash,
            &page_ancestor_path,
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
        state.node_order_index += 1;
        stats.nodes_total += 1;

        chunk_statement.execute(params![
            chunk_id,
            doc_id,
            "page",
            &page_ref,
            &page_ref,
            &heading,
            page_number,
            page_number,
            page_number,
            &page_printed_label,
            &page_printed_label,
            text,
            Option::<String>::None,
            Option::<String>::None,
            source_hash,
            &page_node_id,
            NodeType::Page.as_str(),
            &page_ancestor_path,
            Option::<String>::None,
            Option::<String>::None,
            Option::<String>::None,
            Option::<i64>::None,
            Option::<String>::None
        ])?;
        stats.page_chunks_inserted += 1;
    }

    Ok(())
}
