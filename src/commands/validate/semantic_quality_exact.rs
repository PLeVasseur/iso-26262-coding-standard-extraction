#[derive(Debug, Clone)]
struct ExactIntentProbeEvaluation {
    query: SemanticEvalQuery,
    eval: QueryEvalRecord,
}

fn evaluate_exact_intent_probes(
    connection: &Connection,
    refs: &[GoldReference],
    model_id: &str,
    embedding_dim: usize,
    retrieval_limit: usize,
) -> Result<Vec<ExactIntentProbeEvaluation>> {
    let probes = build_exact_intent_probe_queries(connection, refs)?;
    let mut out = Vec::<ExactIntentProbeEvaluation>::with_capacity(probes.len());
    for probe in probes {
        let eval =
            evaluate_semantic_query(connection, &probe, model_id, embedding_dim, retrieval_limit)?;
        out.push(ExactIntentProbeEvaluation { query: probe, eval });
    }
    Ok(out)
}

fn build_exact_intent_probe_queries(
    connection: &Connection,
    refs: &[GoldReference],
) -> Result<Vec<SemanticEvalQuery>> {
    let mut deduped = refs
        .iter()
        .map(|reference| {
            (
                reference.doc_id.trim().to_string(),
                reference.reference.trim().to_string(),
            )
        })
        .filter(|(doc_id, reference)| !doc_id.is_empty() && !reference.is_empty())
        .collect::<Vec<(String, String)>>();
    deduped.sort();
    deduped.dedup();

    let mut queries = Vec::<SemanticEvalQuery>::new();
    for (doc_id, reference) in deduped {
        let Some(expected_chunk_id) = resolve_reference_chunk_id(connection, &doc_id, &reference)?
        else {
            continue;
        };

        if !is_high_confidence_exact_probe(connection, &doc_id, &reference)? {
            continue;
        }

        let part_filter = resolve_doc_part(connection, &doc_id)?;
        let judged_chunk_ids = bootstrap_judged_chunk_ids(connection, &expected_chunk_id)?;
        queries.push(SemanticEvalQuery {
            query_id: format!("exact-probe-{:03}", queries.len() + 1),
            query_text: reference.clone(),
            intent: "exact_ref_probe".to_string(),
            expected_chunk_ids: vec![expected_chunk_id],
            judged_chunk_ids,
            expected_refs: vec![reference],
            must_hit_top1: true,
            part_filter,
            chunk_type_filter: None,
            notes: Some("query-path exact intent probe".to_string()),
        });
    }

    Ok(queries)
}

fn is_high_confidence_exact_probe(
    connection: &Connection,
    doc_id: &str,
    reference: &str,
) -> Result<bool> {
    let exact_match_count = connection.query_row(
        "
        SELECT COUNT(*)
        FROM chunks
        WHERE doc_id = ?1
          AND lower(COALESCE(ref, '')) = lower(?2)
        ",
        params![doc_id, reference],
        |row| row.get::<_, usize>(0),
    )?;
    Ok(exact_match_count == 1)
}
