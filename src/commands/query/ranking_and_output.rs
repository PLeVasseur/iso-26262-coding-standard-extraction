fn upsert_candidate(dedup: &mut HashMap<String, QueryCandidate>, mut candidate: QueryCandidate) {
    let Some(existing) = dedup.get_mut(&candidate.chunk_id) else {
        dedup.insert(candidate.chunk_id.clone(), candidate);
        return;
    };

    let replace_existing = candidate.score > existing.score;
    if replace_existing {
        inherit_candidate_traces(&mut candidate, existing);
        *existing = candidate;
    } else {
        inherit_candidate_traces(existing, &candidate);
    }
}

fn inherit_candidate_traces(target: &mut QueryCandidate, source: &QueryCandidate) {
    for tag in &source.source_tags {
        if target.source_tags.iter().all(|value| value != tag) {
            target.source_tags.push(tag.clone());
        }
    }

    target.lexical_rank = target.lexical_rank.or(source.lexical_rank);
    target.semantic_rank = target.semantic_rank.or(source.semantic_rank);
    target.lexical_score = target.lexical_score.or(source.lexical_score);
    target.semantic_score = target.semantic_score.or(source.semantic_score);
    target.rrf_score = target.rrf_score.or(source.rrf_score);
}

fn to_results(
    connection: &Connection,
    candidates: Vec<QueryCandidate>,
    with_ancestors: bool,
    with_descendants: bool,
) -> Result<Vec<QueryResult>> {
    let mut out = Vec::with_capacity(candidates.len());

    for (index, candidate) in candidates.into_iter().enumerate() {
        let citation = render_citation(&candidate);

        let ancestor_nodes = if with_ancestors {
            candidate
                .ancestor_path
                .as_deref()
                .map(|value| value.split(" > ").map(ToOwned::to_owned).collect())
        } else {
            None
        };

        let descendants = if with_descendants {
            if let Some(origin_node_id) = candidate.origin_node_id.as_deref() {
                Some(fetch_descendants(connection, origin_node_id)?)
            } else {
                Some(Vec::new())
            }
        } else {
            None
        };

        out.push(QueryResult {
            rank: index + 1,
            score: candidate.score,
            match_kind: candidate.match_kind,
            source_tags: candidate.source_tags,
            rank_trace: QueryRankTrace {
                lexical_rank: candidate.lexical_rank,
                semantic_rank: candidate.semantic_rank,
                lexical_score: candidate.lexical_score,
                semantic_score: candidate.semantic_score,
                rrf_score: candidate.rrf_score,
            },
            chunk_id: candidate.chunk_id,
            doc_id: candidate.doc_id,
            part: candidate.part,
            year: candidate.year,
            chunk_type: candidate.chunk_type,
            parent_ref: resolve_parent_ref(connection, candidate.origin_node_id.as_deref())?,
            reference: candidate.reference,
            heading: candidate.heading,
            page_pdf_start: candidate.page_pdf_start,
            page_pdf_end: candidate.page_pdf_end,
            source_hash: candidate.source_hash,
            snippet: condense_whitespace(&candidate.snippet),
            citation,
            origin_node_id: candidate.origin_node_id,
            leaf_node_type: candidate.leaf_node_type,
            ancestor_path: candidate.ancestor_path,
            anchor_type: candidate.anchor_type,
            anchor_label_raw: candidate.anchor_label_raw,
            anchor_label_norm: candidate.anchor_label_norm,
            anchor_order: candidate.anchor_order,
            citation_anchor_id: candidate.citation_anchor_id,
            ancestor_nodes,
            descendants,
        });
    }

    Ok(out)
}

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
        "Retrieval: requested={} effective={} fusion={} fallback_used={}",
        retrieval.requested_mode,
        retrieval.effective_mode,
        retrieval.fusion,
        retrieval.fallback_used
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
    }

    output.flush()?;
    Ok(())
}
