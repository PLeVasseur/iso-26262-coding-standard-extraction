#[allow(clippy::too_many_arguments)]
fn write_json_response(
    query_text: &str,
    limit: usize,
    part_filter: Option<u32>,
    chunk_type_filter: Option<String>,
    node_type_filter: Option<String>,
    retrieval: RetrievalMetadata,
    results: Vec<QueryResult>,
) -> Result<()> {
    let response = QueryResponse {
        query: query_text.to_string(),
        limit,
        returned: results.len(),
        part_filter,
        chunk_type_filter,
        node_type_filter,
        retrieval,
        results,
    };

    let mut output = io::BufWriter::new(io::stdout().lock());
    serde_json::to_writer_pretty(&mut output, &response)
        .context("failed to serialize query json output")?;
    writeln!(output)?;
    output.flush()?;
    Ok(())
}

fn write_text_response(
    query_text: &str,
    retrieval: &RetrievalMetadata,
    results: &[QueryResult],
) -> Result<()> {
    let mut output = io::BufWriter::new(io::stdout().lock());

    writeln!(output, "Query: {query_text}")?;
    writeln!(
        output,
        "Retrieval: requested={} effective={} fusion={} fallback_used={} timeout_ms={} duration_ms={:.3}",
        retrieval.requested_mode,
        retrieval.effective_mode,
        retrieval.fusion,
        retrieval.fallback_used,
        retrieval.timeout_ms,
        retrieval.query_duration_ms,
    )?;
    writeln!(
        output,
        "Candidates: lexical={} semantic={} fused={} (k lexical={} semantic={}) pinpoint={} max_units={}",
        retrieval.lexical_candidate_count,
        retrieval.semantic_candidate_count,
        retrieval.fused_candidate_count,
        retrieval.lexical_k,
        retrieval.semantic_k,
        retrieval.pinpoint_enabled,
        retrieval.pinpoint_max_units,
    )?;
    writeln!(output, "Results: {}", results.len())?;

    for result in results {
        let reference = if result.reference.is_empty() {
            "(unreferenced)"
        } else {
            &result.reference
        };

        writeln!(
            output,
            "{}.\tISO 26262-{}:{}\t{}\t{}\tpages {}",
            result.rank,
            result.part,
            result.year,
            result.chunk_type,
            reference,
            format_page_range(result.page_pdf_start, result.page_pdf_end)
        )?;
        writeln!(
            output,
            "\tmatch={} score={:.6} chunk_id={}",
            result.match_kind, result.score, result.chunk_id
        )?;
        writeln!(output, "\tsources={}", result.source_tags.join(","))?;
        if let Some(lexical_rank) = result.rank_trace.lexical_rank {
            writeln!(output, "\tlexical_rank: {lexical_rank}")?;
        }
        if let Some(semantic_rank) = result.rank_trace.semantic_rank {
            writeln!(output, "\tsemantic_rank: {semantic_rank}")?;
        }
        if let Some(rrf_score) = result.rank_trace.rrf_score {
            writeln!(output, "\trrf_score: {rrf_score:.6}")?;
        }
        if let Some(origin_node_id) = &result.origin_node_id {
            writeln!(output, "\torigin_node_id: {origin_node_id}")?;
        }
        if let Some(leaf_node_type) = &result.leaf_node_type {
            writeln!(output, "\tleaf_node_type: {leaf_node_type}")?;
        }
        if let Some(parent_ref) = &result.parent_ref {
            writeln!(output, "\tparent_ref: {parent_ref}")?;
        }
        if let Some(anchor_type) = &result.anchor_type {
            writeln!(output, "\tanchor_type: {anchor_type}")?;
        }
        if let Some(anchor_label_raw) = &result.anchor_label_raw {
            writeln!(output, "\tanchor_label_raw: {anchor_label_raw}")?;
        }
        if let Some(anchor_label_norm) = &result.anchor_label_norm {
            writeln!(output, "\tanchor_label_norm: {anchor_label_norm}")?;
        }
        if let Some(anchor_order) = result.anchor_order {
            writeln!(output, "\tanchor_order: {anchor_order}")?;
        }
        if let Some(citation_anchor_id) = &result.citation_anchor_id {
            writeln!(output, "\tcitation_anchor_id: {citation_anchor_id}")?;
        }
        writeln!(output, "\tcitation: {}", result.citation)?;
        writeln!(output, "\tsnippet: {}", result.snippet)?;
        if let Some(fallback_used) = result.pinpoint_fallback_used {
            writeln!(output, "\tpinpoint_fallback_used: {fallback_used}")?;
        }
        if let Some(pinpoint_units) = &result.pinpoint_units {
            for (pinpoint_rank, unit) in pinpoint_units.iter().enumerate() {
                writeln!(
                    output,
                    "\tpinpoint[{}]: type={} score={:.5} unit_id={}",
                    pinpoint_rank + 1,
                    unit.unit_type,
                    unit.score,
                    unit.unit_id
                )?;
                if let Some(row_key) = &unit.row_key {
                    writeln!(output, "\t  row_key: {row_key}")?;
                }
                writeln!(output, "\t  text: {}", unit.text_preview)?;
            }
        }
    }

    output.flush()?;
    Ok(())
}
