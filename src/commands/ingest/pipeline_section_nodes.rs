use super::*;

#[allow(clippy::too_many_arguments)]
pub fn insert_section_heading_nodes(
    node_statement: &mut rusqlite::Statement<'_>,
    doc_id: &str,
    source_hash: &str,
    section_headings: &[SectionHeadingDraft],
    state: &mut PdfNodeState,
    stats: &mut ChunkInsertStats,
) -> Result<()> {
    for section in section_headings {
        let section_node_id = format!(
            "{}:node:section_heading:{}",
            doc_id,
            sanitize_ref_for_id(&section.reference)
        );

        let section_path = build_ancestor_path(
            Some(&state.document_node_id),
            &state.node_paths,
            NodeType::SectionHeading,
            &section.reference,
            &section.heading,
        );
        let section_anchor_order = section.reference.parse::<i64>().ok();
        let section_anchor_id = build_citation_anchor_id(
            doc_id,
            &section.reference,
            "clause",
            Some(&section.reference),
            section_anchor_order,
        );

        insert_node(
            node_statement,
            &section_node_id,
            Some(&state.document_node_id),
            doc_id,
            NodeType::SectionHeading,
            Some(&section.reference),
            Some(&section.reference),
            Some(&section.heading),
            state.node_order_index,
            Some(section.page_pdf),
            Some(section.page_pdf),
            Some(&section.heading),
            source_hash,
            &section_path,
            Some("clause"),
            Some(&section.reference),
            Some(&section.reference),
            section_anchor_order,
            Some(&section_anchor_id),
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
            .section_ref_to_node_id
            .insert(section.reference.clone(), section_node_id.clone());
        state.node_paths.insert(section_node_id, section_path);
        state.node_order_index += 1;
        stats.nodes_total += 1;
        increment_node_type_stat(stats, NodeType::SectionHeading);
    }

    Ok(())
}
