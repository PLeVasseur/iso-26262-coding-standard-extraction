fn collect_lexical_candidates(
    connection: &Connection,
    query_text: &str,
    part_filter: Option<u32>,
    chunk_type_filter: Option<&str>,
    node_type_filter: Option<&str>,
    candidate_limit: usize,
) -> Result<Vec<QueryCandidate>> {
    let mut dedup = HashMap::<String, QueryCandidate>::new();

    for candidate in query_exact_matches(
        connection,
        query_text,
        part_filter,
        chunk_type_filter,
        node_type_filter,
        candidate_limit,
    )? {
        upsert_candidate(&mut dedup, candidate);
    }

    for candidate in query_fts_matches(
        connection,
        query_text,
        part_filter,
        chunk_type_filter,
        node_type_filter,
        candidate_limit,
    )? {
        upsert_candidate(&mut dedup, candidate);
    }

    if node_type_filter.is_some() {
        for candidate in query_node_matches(
            connection,
            query_text,
            part_filter,
            chunk_type_filter,
            node_type_filter,
            candidate_limit,
        )? {
            upsert_candidate(&mut dedup, candidate);
        }
    }

    let mut candidates = dedup.into_values().collect::<Vec<QueryCandidate>>();
    sort_candidates(&mut candidates);
    if candidates.len() > candidate_limit {
        candidates.truncate(candidate_limit);
    }

    for (index, candidate) in candidates.iter_mut().enumerate() {
        candidate.lexical_rank = Some(index + 1);
        candidate.lexical_score = Some(candidate.score);
    }

    Ok(candidates)
}

fn query_exact_matches(
    connection: &Connection,
    query_text: &str,
    part_filter: Option<u32>,
    chunk_type_filter: Option<&str>,
    node_type_filter: Option<&str>,
    candidate_limit: usize,
) -> Result<Vec<QueryCandidate>> {
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
          c.citation_anchor_id
        FROM chunks c
        JOIN docs d ON d.doc_id = c.doc_id
        WHERE
          (?2 IS NULL OR d.part = ?2)
          AND (?3 IS NULL OR c.type = ?3)
          AND (?4 IS NULL OR lower(COALESCE(c.leaf_node_type, c.type)) = lower(?4))
          AND (
            lower(c.ref) = lower(?1)
            OR lower(c.heading) = lower(?1)
            OR lower(c.ref) LIKE '%' || lower(?1) || '%'
            OR lower(c.heading) LIKE '%' || lower(?1) || '%'
          )
        LIMIT ?5
        ",
    )?;

    let mut rows = statement.query(params![
        query_text,
        part_filter.map(i64::from),
        chunk_type_filter,
        node_type_filter,
        candidate_limit as i64,
    ])?;

    let mut out = Vec::new();
    let query_lower = query_text.to_lowercase();

    while let Some(row) = rows.next()? {
        let reference: String = row.get(5)?;
        let heading: String = row.get(6)?;

        let ref_lower = reference.to_lowercase();
        let heading_lower = heading.to_lowercase();

        let (score, match_kind) = if ref_lower == query_lower {
            (1_000.0, "exact_ref")
        } else if heading_lower == query_lower {
            (900.0, "exact_heading")
        } else if ref_lower.contains(&query_lower) {
            (700.0, "ref_contains")
        } else {
            (600.0, "heading_contains")
        };

        out.push(QueryCandidate {
            score,
            match_kind: match_kind.to_string(),
            source_tags: vec!["lexical_exact".to_string()],
            lexical_rank: None,
            semantic_rank: None,
            lexical_score: Some(score),
            semantic_score: None,
            rrf_score: None,
            chunk_id: row.get(0)?,
            doc_id: row.get(1)?,
            part: row.get::<_, u32>(2)?,
            year: row.get::<_, u32>(3)?,
            chunk_type: row.get(4)?,
            reference,
            heading,
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

    Ok(out)
}

fn query_fts_matches(
    connection: &Connection,
    query_text: &str,
    part_filter: Option<u32>,
    chunk_type_filter: Option<&str>,
    node_type_filter: Option<&str>,
    candidate_limit: usize,
) -> Result<Vec<QueryCandidate>> {
    let fts_query = to_fts_query(query_text);

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
          snippet(chunks_fts, 4, '[', ']', ' ... ', 18),
          c.origin_node_id,
          c.leaf_node_type,
          c.ancestor_path,
          c.anchor_type,
          c.anchor_label_raw,
          c.anchor_label_norm,
          c.anchor_order,
          c.citation_anchor_id
        FROM chunks_fts
        JOIN chunks c ON c.rowid = chunks_fts.rowid
        JOIN docs d ON d.doc_id = c.doc_id
        WHERE
          chunks_fts MATCH ?1
          AND (?2 IS NULL OR d.part = ?2)
          AND (?3 IS NULL OR c.type = ?3)
          AND (?4 IS NULL OR lower(COALESCE(c.leaf_node_type, c.type)) = lower(?4))
        ORDER BY bm25(chunks_fts) ASC
        LIMIT ?5
        ",
    )?;

    let mut rows = statement.query(params![
        fts_query,
        part_filter.map(i64::from),
        chunk_type_filter,
        node_type_filter,
        candidate_limit as i64,
    ])?;

    let mut out = Vec::new();
    let mut index = 0usize;

    while let Some(row) = rows.next()? {
        let score = 500.0 - (index as f64);
        out.push(QueryCandidate {
            score,
            match_kind: "fts".to_string(),
            source_tags: vec!["lexical_fts".to_string()],
            lexical_rank: None,
            semantic_rank: None,
            lexical_score: Some(score),
            semantic_score: None,
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
        index += 1;
    }

    Ok(out)
}

fn query_node_matches(
    connection: &Connection,
    query_text: &str,
    part_filter: Option<u32>,
    chunk_type_filter: Option<&str>,
    node_type_filter: Option<&str>,
    candidate_limit: usize,
) -> Result<Vec<QueryCandidate>> {
    let mut statement = connection.prepare(
        "
        SELECT
          n.node_id,
          n.doc_id,
          d.part,
          d.year,
          n.node_type,
          COALESCE(n.ref, ''),
          COALESCE(n.heading, ''),
          n.page_pdf_start,
          n.page_pdf_end,
          COALESCE(n.source_hash, ''),
          substr(COALESCE(n.text, ''), 1, 420),
          n.ancestor_path,
          n.anchor_type,
          n.anchor_label_raw,
          n.anchor_label_norm,
          n.anchor_order,
          n.citation_anchor_id
        FROM nodes n
        JOIN docs d ON d.doc_id = n.doc_id
        WHERE
          (?2 IS NULL OR d.part = ?2)
          AND (?3 IS NULL OR lower(n.node_type) = lower(?3))
          AND (?4 IS NULL OR lower(n.node_type) = lower(?4))
          AND (
            lower(n.ref) = lower(?1)
            OR lower(n.heading) = lower(?1)
            OR lower(n.ref) LIKE '%' || lower(?1) || '%'
            OR lower(n.heading) LIKE '%' || lower(?1) || '%'
            OR lower(n.text) LIKE '%' || lower(?1) || '%'
          )
        LIMIT ?5
        ",
    )?;

    let mut rows = statement.query(params![
        query_text,
        part_filter.map(i64::from),
        chunk_type_filter,
        node_type_filter,
        candidate_limit as i64,
    ])?;

    let query_lower = query_text.to_lowercase();
    let mut out = Vec::new();

    while let Some(row) = rows.next()? {
        let node_id: String = row.get(0)?;
        let reference: String = row.get(5)?;
        let heading: String = row.get(6)?;
        let node_type: String = row.get(4)?;
        let snippet: String = row.get(10)?;

        let ref_lower = reference.to_lowercase();
        let heading_lower = heading.to_lowercase();
        let snippet_lower = snippet.to_lowercase();

        let (score, match_kind) = if ref_lower == query_lower {
            (850.0, "node_exact_ref")
        } else if heading_lower == query_lower {
            (760.0, "node_exact_heading")
        } else if ref_lower.contains(&query_lower) {
            (650.0, "node_ref_contains")
        } else if heading_lower.contains(&query_lower) {
            (620.0, "node_heading_contains")
        } else if snippet_lower.contains(&query_lower) {
            (580.0, "node_text_contains")
        } else {
            (550.0, "node_match")
        };

        out.push(QueryCandidate {
            score,
            match_kind: match_kind.to_string(),
            source_tags: vec!["lexical_node".to_string()],
            lexical_rank: None,
            semantic_rank: None,
            lexical_score: Some(score),
            semantic_score: None,
            rrf_score: None,
            chunk_id: format!("node::{node_id}"),
            doc_id: row.get(1)?,
            part: row.get::<_, u32>(2)?,
            year: row.get::<_, u32>(3)?,
            chunk_type: node_type.clone(),
            reference,
            heading,
            page_pdf_start: row.get(7)?,
            page_pdf_end: row.get(8)?,
            source_hash: row.get(9)?,
            snippet,
            origin_node_id: Some(node_id),
            leaf_node_type: Some(node_type),
            ancestor_path: row.get(11)?,
            anchor_type: row.get(12)?,
            anchor_label_raw: row.get(13)?,
            anchor_label_norm: row.get(14)?,
            anchor_order: row.get(15)?,
            citation_anchor_id: row.get(16)?,
        });
    }

    Ok(out)
}
