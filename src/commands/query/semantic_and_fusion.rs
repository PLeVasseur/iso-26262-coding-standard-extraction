struct SemanticIndexStatus {
    available: bool,
    reason: Option<String>,
}

fn semantic_index_status(connection: &Connection, model_id: &str) -> Result<SemanticIndexStatus> {
    let embeddings_table_exists = connection
        .query_row(
            "
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name = 'chunk_embeddings'
            LIMIT 1
            ",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .is_some();

    if !embeddings_table_exists {
        return Ok(SemanticIndexStatus {
            available: false,
            reason: Some(
                "chunk_embeddings table is missing; run ingest on schema 0.4.0+".to_string(),
            ),
        });
    }

    let model_exists = connection
        .query_row(
            "SELECT 1 FROM embedding_models WHERE model_id = ?1 LIMIT 1",
            [model_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .is_some();
    if !model_exists {
        return Ok(SemanticIndexStatus {
            available: false,
            reason: Some(format!("embedding model '{model_id}' is not registered")),
        });
    }

    let embedding_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM chunk_embeddings WHERE model_id = ?1",
        [model_id],
        |row| row.get(0),
    )?;

    if embedding_count <= 0 {
        return Ok(SemanticIndexStatus {
            available: false,
            reason: Some(format!("semantic index is empty for model '{model_id}'")),
        });
    }

    Ok(SemanticIndexStatus {
        available: true,
        reason: None,
    })
}

#[allow(clippy::too_many_arguments)]
fn collect_semantic_candidates(
    connection: &Connection,
    query_text: &str,
    part_filter: Option<u32>,
    chunk_type_filter: Option<&str>,
    node_type_filter: Option<&str>,
    model_id: &str,
    embedding_dim: usize,
    candidate_limit: usize,
) -> Result<Vec<QueryCandidate>> {
    let query_embedding = embed_text_local(query_text, embedding_dim);
    let mut statement = connection.prepare(
        "
        SELECT
          c.chunk_id,
          c.doc_id,
          d.part,
          d.year,
          c.type,
          COALESCE(c.ref, ''),
          COALESCE(c.heading, ''),
          c.page_pdf_start,
          c.page_pdf_end,
          COALESCE(c.source_hash, ''),
          substr(COALESCE(c.text, ''), 1, 420),
          c.origin_node_id,
          c.leaf_node_type,
          c.ancestor_path,
          c.anchor_type,
          c.anchor_label_raw,
          c.anchor_label_norm,
          c.anchor_order,
          c.citation_anchor_id,
          ce.embedding,
          ce.embedding_dim
        FROM chunk_embeddings ce
        JOIN chunks c ON c.chunk_id = ce.chunk_id
        JOIN docs d ON d.doc_id = c.doc_id
        WHERE
          ce.model_id = ?1
          AND (?2 IS NULL OR d.part = ?2)
          AND (?3 IS NULL OR c.type = ?3)
          AND (?4 IS NULL OR lower(COALESCE(c.leaf_node_type, c.type)) = lower(?4))
        ",
    )?;

    let mut rows = statement.query(params![
        model_id,
        part_filter.map(i64::from),
        chunk_type_filter,
        node_type_filter,
    ])?;

    let mut out = Vec::<QueryCandidate>::new();
    while let Some(row) = rows.next()? {
        let row_dim = row.get::<_, i64>(20)? as usize;
        if row_dim != embedding_dim {
            continue;
        }

        let embedding_blob = row.get::<_, Vec<u8>>(19)?;
        let Some(candidate_embedding) = decode_embedding_blob(&embedding_blob, embedding_dim)
        else {
            continue;
        };

        let semantic_score = cosine_similarity(&query_embedding, &candidate_embedding);
        out.push(QueryCandidate {
            score: semantic_score,
            match_kind: "semantic_cosine".to_string(),
            source_tags: vec!["semantic".to_string()],
            lexical_rank: None,
            semantic_rank: None,
            lexical_score: None,
            semantic_score: Some(semantic_score),
            rrf_score: None,
            chunk_id: row.get(0)?,
            doc_id: row.get(1)?,
            part: row.get::<_, u32>(2)?,
            year: row.get::<_, u32>(3)?,
            chunk_type: row.get(4)?,
            reference: row.get(5)?,
            heading: row.get(6)?,
            page_pdf_start: row.get(7)?,
            page_pdf_end: row.get(8)?,
            source_hash: row.get(9)?,
            snippet: row.get(10)?,
            origin_node_id: row.get(11)?,
            leaf_node_type: row.get(12)?,
            ancestor_path: row.get(13)?,
            anchor_type: row.get(14)?,
            anchor_label_raw: row.get(15)?,
            anchor_label_norm: row.get(16)?,
            anchor_order: row.get(17)?,
            citation_anchor_id: row.get(18)?,
        });
    }

    sort_candidates(&mut out);
    if out.len() > candidate_limit {
        out.truncate(candidate_limit);
    }
    for (index, candidate) in out.iter_mut().enumerate() {
        candidate.semantic_rank = Some(index + 1);
        candidate.semantic_score = Some(candidate.score);
    }

    Ok(out)
}

fn fuse_rrf_candidates(
    lexical_candidates: &[QueryCandidate],
    semantic_candidates: &[QueryCandidate],
    rrf_k: u32,
    fusion_mode: FusionMode,
) -> Result<Vec<QueryCandidate>> {
    let FusionMode::Rrf = fusion_mode;
    let mut merged = HashMap::<String, QueryCandidate>::new();
    let rrf_base = f64::from(rrf_k.max(1));

    for (index, candidate) in lexical_candidates.iter().enumerate() {
        let rank = candidate.lexical_rank.unwrap_or(index + 1);
        let contribution = 1.0 / (rrf_base + rank as f64);
        let entry = merged
            .entry(candidate.chunk_id.clone())
            .or_insert_with(|| seed_fusion_candidate(candidate));
        entry.score += contribution;
        entry.rrf_score = Some(entry.score);
        entry.lexical_rank = Some(rank);
        entry.lexical_score = candidate.lexical_score.or(Some(candidate.score));
        merge_source_tag(entry, "lexical");
    }

    for (index, candidate) in semantic_candidates.iter().enumerate() {
        let rank = candidate.semantic_rank.unwrap_or(index + 1);
        let contribution = 1.0 / (rrf_base + rank as f64);
        let entry = merged
            .entry(candidate.chunk_id.clone())
            .or_insert_with(|| seed_fusion_candidate(candidate));
        entry.score += contribution;
        entry.rrf_score = Some(entry.score);
        entry.semantic_rank = Some(rank);
        entry.semantic_score = candidate.semantic_score.or(Some(candidate.score));
        merge_source_tag(entry, "semantic");
    }

    let mut out = merged
        .into_values()
        .map(|mut value| {
            value.match_kind = match (value.lexical_rank, value.semantic_rank) {
                (Some(_), Some(_)) => "hybrid_rrf",
                (Some(_), None) => "lexical_rrf",
                (None, Some(_)) => "semantic_rrf",
                (None, None) => "hybrid_rrf",
            }
            .to_string();
            value
        })
        .collect::<Vec<QueryCandidate>>();

    sort_candidates(&mut out);
    Ok(out)
}

fn seed_fusion_candidate(candidate: &QueryCandidate) -> QueryCandidate {
    let mut seeded = candidate.clone();
    seeded.score = 0.0;
    seeded.rrf_score = Some(0.0);
    seeded.source_tags = Vec::new();
    seeded
}

fn merge_source_tag(candidate: &mut QueryCandidate, source: &str) {
    if candidate.source_tags.iter().all(|value| value != source) {
        candidate.source_tags.push(source.to_string());
    }
}

fn is_exact_intent_query(query_text: &str) -> bool {
    let trimmed = query_text.trim();
    if trimmed.is_empty() {
        return false;
    }

    let lowered = trimmed.to_ascii_lowercase();
    if lowered.starts_with("table ") {
        return lowered
            .split_whitespace()
            .nth(1)
            .map(|value| value.chars().all(|character| character.is_ascii_digit()))
            .unwrap_or(false);
    }

    if lowered.starts_with("annex ") {
        return lowered
            .split_whitespace()
            .nth(1)
            .map(|value| {
                value
                    .chars()
                    .all(|character| character.is_ascii_alphabetic())
            })
            .unwrap_or(false);
    }

    let first_token = lowered.split_whitespace().next().unwrap_or("");
    looks_like_clause_reference(first_token)
}

fn looks_like_clause_reference(value: &str) -> bool {
    let parts = value.split('.').collect::<Vec<&str>>();
    if parts.len() < 2 {
        return false;
    }

    parts
        .iter()
        .all(|part| !part.is_empty() && part.chars().all(|character| character.is_ascii_digit()))
}
