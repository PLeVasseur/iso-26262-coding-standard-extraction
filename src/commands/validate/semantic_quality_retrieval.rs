use super::*;

pub fn semantic_eval_hybrid_hits(
    connection: &Connection,
    query_text: &str,
    part_filter: Option<u32>,
    chunk_type_filter: Option<&str>,
    model_id: &str,
    embedding_dim: usize,
    limit: usize,
    exact_intent_priority: bool,
) -> Result<Vec<SemanticRetrievedHit>> {
    let lexical_hits = semantic_eval_lexical_hits(
        connection,
        query_text,
        part_filter,
        chunk_type_filter,
        limit,
    )?;
    let semantic_hits = semantic_eval_semantic_hits(
        connection,
        query_text,
        part_filter,
        chunk_type_filter,
        model_id,
        embedding_dim,
        limit,
    )?;

    if lexical_hits.is_empty() {
        return Ok(semantic_hits.into_iter().take(limit).collect());
    }
    if semantic_hits.is_empty() {
        return Ok(lexical_hits.into_iter().take(limit).collect());
    }

    #[derive(Clone)]
    struct HybridRow {
        pub hit: SemanticRetrievedHit,
        pub score: f64,
    }

    let mut fused = HashMap::<String, HybridRow>::new();
    for (index, hit) in lexical_hits.iter().enumerate() {
        let contribution = 1.0 / (SEMANTIC_RRF_K + (index + 1) as f64);
        let entry = fused.entry(hit.chunk_id.clone()).or_insert(HybridRow {
            hit: hit.clone(),
            score: 0.0,
        });
        entry.score += contribution;
    }

    for (index, hit) in semantic_hits.iter().enumerate() {
        let contribution = 1.0 / (SEMANTIC_RRF_K + (index + 1) as f64);
        let entry = fused.entry(hit.chunk_id.clone()).or_insert(HybridRow {
            hit: hit.clone(),
            score: 0.0,
        });
        entry.score += contribution;
    }

    let mut out = fused
        .into_values()
        .map(|mut row| {
            row.hit.score = row.score;
            row.hit
        })
        .collect::<Vec<SemanticRetrievedHit>>();
    out.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then(
                left.page_pdf_start
                    .unwrap_or(i64::MAX)
                    .cmp(&right.page_pdf_start.unwrap_or(i64::MAX)),
            )
            .then(left.chunk_id.cmp(&right.chunk_id))
    });
    if exact_intent_priority
        && let Some(mut lexical_top1) = lexical_hits.first().cloned()
    {
        out.retain(|hit| hit.chunk_id != lexical_top1.chunk_id);
        lexical_top1.score = out.first().map_or(1.0, |row| row.score + 1.0);
        out.insert(0, lexical_top1);
    }

    if out.len() > limit {
        out.truncate(limit);
    }

    Ok(out)
}

pub fn semantic_eval_lexical_hits(
    connection: &Connection,
    query_text: &str,
    part_filter: Option<u32>,
    chunk_type_filter: Option<&str>,
    limit: usize,
) -> Result<Vec<SemanticRetrievedHit>> {
    let mut statement = connection.prepare(
        "
        SELECT
          c.chunk_id,
          COALESCE(c.ref, ''),
          c.page_pdf_start,
          c.page_pdf_end,
          c.citation_anchor_id,
          CASE
            WHEN lower(COALESCE(c.ref, '')) = lower(?1) THEN 1000.0
            WHEN lower(COALESCE(c.heading, '')) = lower(?1) THEN 900.0
            WHEN lower(COALESCE(c.ref, '')) LIKE '%' || lower(?1) || '%' THEN 700.0
            WHEN lower(COALESCE(c.heading, '')) LIKE '%' || lower(?1) || '%' THEN 600.0
            ELSE 500.0
          END AS lexical_score
        FROM chunks c
        JOIN docs d ON d.doc_id = c.doc_id
        WHERE
          (?2 IS NULL OR d.part = ?2)
          AND (?3 IS NULL OR lower(COALESCE(c.type, '')) = lower(?3))
          AND (
            lower(COALESCE(c.ref, '')) = lower(?1)
            OR lower(COALESCE(c.heading, '')) = lower(?1)
            OR lower(COALESCE(c.ref, '')) LIKE '%' || lower(?1) || '%'
            OR lower(COALESCE(c.heading, '')) LIKE '%' || lower(?1) || '%'
            OR lower(COALESCE(c.text, '')) LIKE '%' || lower(?1) || '%'
          )
        ORDER BY lexical_score DESC, c.page_pdf_start ASC, c.chunk_id ASC
        LIMIT ?4
        ",
    )?;

    let mut rows = statement.query(params![
        query_text,
        part_filter.map(i64::from),
        chunk_type_filter,
        limit as i64,
    ])?;

    let mut hits = Vec::<SemanticRetrievedHit>::new();
    while let Some(row) = rows.next()? {
        hits.push(SemanticRetrievedHit {
            chunk_id: row.get(0)?,
            reference: row.get(1)?,
            page_pdf_start: row.get(2)?,
            page_pdf_end: row.get(3)?,
            citation_anchor_id: row.get(4)?,
            score: row.get::<_, f64>(5)?,
        });
    }

    Ok(hits)
}

pub fn semantic_eval_semantic_hits(
    connection: &Connection,
    query_text: &str,
    part_filter: Option<u32>,
    chunk_type_filter: Option<&str>,
    model_id: &str,
    embedding_dim: usize,
    limit: usize,
) -> Result<Vec<SemanticRetrievedHit>> {
    let table_exists = connection
        .query_row(
            "
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table' AND name = 'chunk_embeddings'
            LIMIT 1
            ",
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .is_some();
    if !table_exists {
        return Ok(Vec::new());
    }

    let semantic_query_text = semantic_embedding_query_text(query_text);
    let query_embedding = crate::semantic::embed_text_local(&semantic_query_text, embedding_dim);
    let query_tokens = query_signal_tokens(&semantic_query_text);
    let mut statement = connection.prepare(
        "
        SELECT
          c.chunk_id,
          COALESCE(c.ref, ''),
          COALESCE(c.heading, ''),
          COALESCE(c.text, ''),
          c.page_pdf_start,
          c.page_pdf_end,
          c.citation_anchor_id,
          ce.embedding,
          ce.embedding_dim
        FROM chunk_embeddings ce
        JOIN chunks c ON c.chunk_id = ce.chunk_id
        JOIN docs d ON d.doc_id = c.doc_id
        WHERE
          ce.model_id = ?1
          AND (?2 IS NULL OR d.part = ?2)
          AND (?3 IS NULL OR lower(COALESCE(c.type, '')) = lower(?3))
        ",
    )?;

    let mut rows = statement.query(params![
        model_id,
        part_filter.map(i64::from),
        chunk_type_filter
    ])?;
    let mut hits = Vec::<SemanticRetrievedHit>::new();
    while let Some(row) = rows.next()? {
        let row_dim = row.get::<_, i64>(8)? as usize;
        if row_dim != embedding_dim {
            continue;
        }

        let blob = row.get::<_, Vec<u8>>(7)?;
        let Some(embedding) = crate::semantic::decode_embedding_blob(&blob, embedding_dim) else {
            continue;
        };
        let semantic_score = crate::semantic::cosine_similarity(&query_embedding, &embedding);
        let lexical_bonus = lexical_signal_bonus(
            &query_tokens,
            row.get::<_, String>(1)?.as_str(),
            row.get::<_, String>(2)?.as_str(),
            row.get::<_, String>(3)?.as_str(),
        );
        let score = semantic_score * 0.45 + lexical_bonus * 0.55;
        hits.push(SemanticRetrievedHit {
            chunk_id: row.get(0)?,
            reference: row.get(1)?,
            page_pdf_start: row.get(4)?,
            page_pdf_end: row.get(5)?,
            citation_anchor_id: row.get(6)?,
            score,
        });
    }

    hits.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then(
                left.page_pdf_start
                    .unwrap_or(i64::MAX)
                    .cmp(&right.page_pdf_start.unwrap_or(i64::MAX)),
            )
            .then(left.chunk_id.cmp(&right.chunk_id))
    });
    if hits.len() > limit {
        hits.truncate(limit);
    }

    Ok(hits)
}

pub fn semantic_hit_identity(hit: &SemanticRetrievedHit) -> String {
    format!(
        "{}|{}|{}|{}|{}",
        hit.chunk_id,
        hit.reference,
        hit.page_pdf_start.unwrap_or(-1),
        hit.page_pdf_end.unwrap_or(-1),
        hit.citation_anchor_id.clone().unwrap_or_default()
    )
}

pub fn ndcg_at_k(
    results: &[String],
    expected: &HashSet<String>,
    judged: &HashSet<String>,
    k: usize,
) -> Option<f64> {
    if (expected.is_empty() && judged.is_empty()) || k == 0 {
        return None;
    }

    let cutoff = results.len().min(k);
    let mut dcg = 0.0;
    for (index, chunk_id) in results.iter().take(cutoff).enumerate() {
        let relevance = if expected.contains(chunk_id) {
            2.0
        } else if judged.contains(chunk_id) {
            1.0
        } else {
            0.0
        };
        if relevance <= 0.0 {
            continue;
        }

        let rank = index + 1;
        let gain = 2f64.powf(relevance) - 1.0;
        dcg += gain / ((rank as f64 + 1.0).log2());
    }

    let judged_only = judged
        .iter()
        .filter(|chunk_id| !expected.contains(*chunk_id))
        .count();
    let mut ideal_relevances = vec![2.0; expected.len()];
    ideal_relevances.extend(std::iter::repeat(1.0).take(judged_only));
    if ideal_relevances.is_empty() {
        return None;
    }
    ideal_relevances.sort_by(|left, right| {
        right
            .partial_cmp(left)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut idcg = 0.0;
    for (index, relevance) in ideal_relevances.into_iter().take(k).enumerate() {
        let rank = index + 1;
        let gain = 2f64.powf(relevance) - 1.0;
        idcg += gain / ((rank as f64 + 1.0).log2());
    }
    if idcg <= 0.0 {
        return None;
    }

    Some(dcg / idcg)
}

pub fn top_k_jaccard_overlap(
    left: &[SemanticRetrievedHit],
    right: &[SemanticRetrievedHit],
    k: usize,
) -> Option<f64> {
    if k == 0 {
        return None;
    }

    let left_ids = left
        .iter()
        .take(k)
        .map(|hit| hit.chunk_id.clone())
        .collect::<HashSet<String>>();
    let right_ids = right
        .iter()
        .take(k)
        .map(|hit| hit.chunk_id.clone())
        .collect::<HashSet<String>>();

    let union_count = left_ids.union(&right_ids).count();
    if union_count == 0 {
        return None;
    }

    let intersection_count = left_ids.intersection(&right_ids).count();
    Some(intersection_count as f64 / union_count as f64)
}

pub fn query_signal_tokens(query_text: &str) -> Vec<String> {
    const STOPWORDS: &[&str] = &[
        "a",
        "an",
        "and",
        "around",
        "concept",
        "concerning",
        "for",
        "guidance",
        "in",
        "of",
        "on",
        "related",
        "requirement",
        "requirements",
        "the",
        "to",
        "with",
    ];

    let mut tokens = query_text
        .to_ascii_lowercase()
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|token| token.len() >= 3)
        .filter(|token| STOPWORDS.iter().all(|stopword| stopword != token))
        .map(str::to_string)
        .collect::<Vec<String>>();
    tokens.sort();
    tokens.dedup();
    tokens
}

pub fn semantic_embedding_query_text(query_text: &str) -> String {
    const NOISE_PREFIXES: &[&str] = &[
        "concept guidance for ",
        "requirements concerning ",
        "requirements regarding ",
        "requirements for ",
        "guidance for ",
    ];

    let normalized = query_text
        .split_whitespace()
        .collect::<Vec<&str>>()
        .join(" ")
        .trim()
        .to_string();
    if normalized.is_empty() {
        return normalized;
    }

    let lowered = normalized.to_ascii_lowercase();
    for prefix in NOISE_PREFIXES {
        if lowered.starts_with(prefix) {
            let stripped = normalized[prefix.len()..].trim();
            if !stripped.is_empty() {
                return stripped.to_string();
            }
        }
    }

    normalized
}

pub fn lexical_signal_bonus(query_tokens: &[String], reference: &str, heading: &str, text: &str) -> f64 {
    if query_tokens.is_empty() {
        return 0.0;
    }

    let haystack = format!(
        "{} {} {}",
        reference.to_ascii_lowercase(),
        heading.to_ascii_lowercase(),
        text.to_ascii_lowercase()
    );
    let overlap = query_tokens
        .iter()
        .filter(|token| haystack.contains(token.as_str()))
        .count();
    overlap as f64 / query_tokens.len() as f64
}

pub fn reciprocal_rank_at_k(results: &[String], expected: &HashSet<String>, k: usize) -> Option<f64> {
    if expected.is_empty() || k == 0 {
        return None;
    }

    for (index, chunk_id) in results.iter().take(k).enumerate() {
        if expected.contains(chunk_id) {
            return Some(1.0 / (index as f64 + 1.0));
        }
    }
    Some(0.0)
}

pub fn recall_at_k(results: &[String], expected: &HashSet<String>, k: usize) -> Option<f64> {
    if expected.is_empty() || k == 0 {
        return None;
    }

    let hit_count = results
        .iter()
        .take(k)
        .filter(|chunk_id| expected.contains(*chunk_id))
        .count();
    Some(hit_count as f64 / expected.len() as f64)
}

pub fn judged_at_k(results: &[String], judged: &HashSet<String>, k: usize) -> Option<f64> {
    if judged.is_empty() || k == 0 {
        return None;
    }

    let limit = results.len().min(k);
    if limit == 0 {
        return Some(0.0);
    }

    let judged_in_top_k = results
        .iter()
        .take(limit)
        .filter(|chunk_id| judged.contains(*chunk_id))
        .count();
    Some(judged_in_top_k as f64 / k as f64)
}
pub fn percentile(values: &[f64], quantile: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));

    let q = quantile.clamp(0.0, 1.0);
    let rank = ((sorted.len() as f64) * q).ceil() as usize;
    let index = rank.saturating_sub(1).min(sorted.len().saturating_sub(1));
    sorted.get(index).copied()
}

pub fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }

    Some(values.iter().sum::<f64>() / values.len() as f64)
}
