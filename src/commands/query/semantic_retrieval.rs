use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};

use crate::semantic::{cosine_similarity, decode_embedding_blob, embed_text_local};

use super::run::{enforce_timeout, sort_candidates, QueryCandidate, QueryTimeoutBudget};

pub(super) struct SemanticIndexStatus {
    pub(super) available: bool,
    pub(super) reason: Option<String>,
}

pub(super) fn semantic_index_status(
    connection: &Connection,
    model_id: &str,
) -> Result<SemanticIndexStatus> {
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
pub(super) fn collect_semantic_candidates(
    connection: &Connection,
    query_text: &str,
    part_filter: Option<u32>,
    chunk_type_filter: Option<&str>,
    node_type_filter: Option<&str>,
    model_id: &str,
    embedding_dim: usize,
    candidate_limit: usize,
    timeout_budget: Option<QueryTimeoutBudget>,
) -> Result<Vec<QueryCandidate>> {
    let semantic_query_text = semantic_embedding_query_text(query_text);
    let query_embedding = embed_text_local(&semantic_query_text, embedding_dim);
    let query_tokens = query_signal_tokens(&semantic_query_text);
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
    let mut scanned_rows = 0usize;
    while let Some(row) = rows.next()? {
        scanned_rows += 1;
        if scanned_rows % 64 == 0 {
            enforce_timeout(timeout_budget, "semantic candidate scan")?;
        }

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
        let reference = row.get::<_, String>(5)?;
        let heading = row.get::<_, String>(6)?;
        let snippet = row.get::<_, String>(10)?;
        let lexical_bonus = lexical_signal_bonus(&query_tokens, &reference, &heading, &snippet);
        let score = semantic_score * 0.45 + lexical_bonus * 0.55;
        out.push(QueryCandidate {
            score,
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
            reference,
            heading,
            page_pdf_start: row.get(7)?,
            page_pdf_end: row.get(8)?,
            source_hash: row.get(9)?,
            snippet,
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
    enforce_timeout(timeout_budget, "semantic candidate ranking")?;

    Ok(out)
}

fn query_signal_tokens(query_text: &str) -> Vec<String> {
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

fn lexical_signal_bonus(
    query_tokens: &[String],
    reference: &str,
    heading: &str,
    text: &str,
) -> f64 {
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

fn semantic_embedding_query_text(query_text: &str) -> String {
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
