use std::collections::HashMap;
use std::io::{self, Write};

use anyhow::{bail, Context, Result};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use tracing::{info, warn};

use crate::cli::{FusionMode, QueryArgs, RetrievalMode};
use crate::semantic::{
    cosine_similarity, decode_embedding_blob, embed_text_local, resolve_model_config,
};

const MAX_QUERY_CANDIDATES: usize = 256;

#[derive(Debug, Clone)]
struct QueryCandidate {
    score: f64,
    match_kind: String,
    source_tags: Vec<String>,
    lexical_rank: Option<usize>,
    semantic_rank: Option<usize>,
    lexical_score: Option<f64>,
    semantic_score: Option<f64>,
    rrf_score: Option<f64>,
    chunk_id: String,
    doc_id: String,
    part: u32,
    year: u32,
    chunk_type: String,
    reference: String,
    heading: String,
    page_pdf_start: Option<i64>,
    page_pdf_end: Option<i64>,
    source_hash: String,
    snippet: String,
    origin_node_id: Option<String>,
    leaf_node_type: Option<String>,
    ancestor_path: Option<String>,
    anchor_type: Option<String>,
    anchor_label_raw: Option<String>,
    anchor_label_norm: Option<String>,
    anchor_order: Option<i64>,
    citation_anchor_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct DescendantNode {
    node_id: String,
    parent_node_id: Option<String>,
    node_type: String,
    reference: Option<String>,
    heading: Option<String>,
    order_index: i64,
    page_pdf_start: Option<i64>,
    page_pdf_end: Option<i64>,
    text_preview: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct QueryRankTrace {
    lexical_rank: Option<usize>,
    semantic_rank: Option<usize>,
    lexical_score: Option<f64>,
    semantic_score: Option<f64>,
    rrf_score: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
struct QueryResult {
    rank: usize,
    score: f64,
    match_kind: String,
    source_tags: Vec<String>,
    rank_trace: QueryRankTrace,
    chunk_id: String,
    doc_id: String,
    part: u32,
    year: u32,
    chunk_type: String,
    reference: String,
    parent_ref: Option<String>,
    heading: String,
    page_pdf_start: Option<i64>,
    page_pdf_end: Option<i64>,
    source_hash: String,
    snippet: String,
    citation: String,
    origin_node_id: Option<String>,
    leaf_node_type: Option<String>,
    ancestor_path: Option<String>,
    anchor_type: Option<String>,
    anchor_label_raw: Option<String>,
    anchor_label_norm: Option<String>,
    anchor_order: Option<i64>,
    citation_anchor_id: Option<String>,
    ancestor_nodes: Option<Vec<String>>,
    descendants: Option<Vec<DescendantNode>>,
}

#[derive(Debug, Serialize)]
struct RetrievalMetadata {
    requested_mode: String,
    effective_mode: String,
    lexical_k: usize,
    semantic_k: usize,
    fusion: String,
    rrf_k: u32,
    semantic_model_id: Option<String>,
    exact_intent: bool,
    exact_intent_forced_lexical: bool,
    fallback_used: bool,
    fallback_reason: Option<String>,
}

#[derive(Debug, Serialize)]
struct QueryResponse {
    query: String,
    limit: usize,
    returned: usize,
    part_filter: Option<u32>,
    chunk_type_filter: Option<String>,
    node_type_filter: Option<String>,
    retrieval: RetrievalMetadata,
    results: Vec<QueryResult>,
}

pub fn run(args: QueryArgs) -> Result<()> {
    let query_text = args.query.trim();
    if query_text.is_empty() {
        bail!("query must not be empty");
    }

    let db_path = args
        .db_path
        .clone()
        .unwrap_or_else(|| args.cache_root.join("iso26262_index.sqlite"));

    let connection = Connection::open_with_flags(
        &db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("failed to open database read-only: {}", db_path.display()))?;

    let chunk_type_filter = args
        .chunk_type
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_lowercase);
    let node_type_filter = args
        .node_type
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_lowercase);

    let limit = args.limit.max(1);
    let lexical_k = clamp_candidates(args.lexical_k.max(limit));
    let semantic_k = clamp_candidates(args.semantic_k.max(limit));
    let exact_intent = is_exact_intent_query(query_text);

    let requested_mode = args.retrieval_mode;
    let exact_intent_forced_lexical = exact_intent
        && matches!(
            requested_mode,
            RetrievalMode::Hybrid | RetrievalMode::Semantic
        );
    let mut effective_mode = if exact_intent_forced_lexical {
        RetrievalMode::Lexical
    } else {
        requested_mode
    };

    let mut fallback_used = false;
    let mut fallback_reason = None::<String>;
    let semantic_model_id = args.semantic_model_id.as_ref().map(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    });
    let semantic_model_id = semantic_model_id.flatten();

    let mut lexical_candidates = Vec::<QueryCandidate>::new();
    if matches!(
        effective_mode,
        RetrievalMode::Lexical | RetrievalMode::Hybrid
    ) {
        lexical_candidates = collect_lexical_candidates(
            &connection,
            query_text,
            args.part,
            chunk_type_filter.as_deref(),
            node_type_filter.as_deref(),
            lexical_k,
        )?;
    }

    let mut semantic_candidates = Vec::<QueryCandidate>::new();
    if matches!(
        effective_mode,
        RetrievalMode::Semantic | RetrievalMode::Hybrid
    ) {
        let Some(model_id) = semantic_model_id.as_deref() else {
            bail!(
                "semantic retrieval requires --semantic-model-id (recommended: {})",
                crate::semantic::DEFAULT_MODEL_ID
            );
        };

        let semantic_status = semantic_index_status(&connection, model_id)?;
        if !semantic_status.available {
            let reason = semantic_status
                .reason
                .unwrap_or_else(|| "semantic index is unavailable".to_string());
            if args.allow_lexical_fallback {
                warn!(reason = %reason, "semantic retrieval unavailable; falling back to lexical");
                fallback_used = true;
                fallback_reason = Some(reason);
                effective_mode = RetrievalMode::Lexical;
                if lexical_candidates.is_empty() {
                    lexical_candidates = collect_lexical_candidates(
                        &connection,
                        query_text,
                        args.part,
                        chunk_type_filter.as_deref(),
                        node_type_filter.as_deref(),
                        lexical_k,
                    )?;
                }
            } else {
                bail!(
                    "{}; run `cargo run -- embed --cache-root .cache/iso26262 --model-id {}` or pass --allow-lexical-fallback",
                    reason,
                    model_id
                );
            }
        } else {
            let model = resolve_model_config(model_id);
            semantic_candidates = collect_semantic_candidates(
                &connection,
                query_text,
                args.part,
                chunk_type_filter.as_deref(),
                node_type_filter.as_deref(),
                model_id,
                model.dimensions,
                semantic_k,
            )?;
        }
    }

    let mut candidates = match effective_mode {
        RetrievalMode::Lexical => lexical_candidates,
        RetrievalMode::Semantic => semantic_candidates,
        RetrievalMode::Hybrid => fuse_rrf_candidates(
            &lexical_candidates,
            &semantic_candidates,
            args.rrf_k,
            args.fusion,
        )?,
    };

    sort_candidates(&mut candidates);
    if candidates.len() > limit {
        candidates.truncate(limit);
    }

    let results = to_results(
        &connection,
        candidates,
        args.with_ancestors,
        args.with_descendants,
    )?;

    let retrieval_metadata = RetrievalMetadata {
        requested_mode: retrieval_mode_label(requested_mode).to_string(),
        effective_mode: retrieval_mode_label(effective_mode).to_string(),
        lexical_k,
        semantic_k,
        fusion: fusion_mode_label(args.fusion).to_string(),
        rrf_k: args.rrf_k,
        semantic_model_id,
        exact_intent,
        exact_intent_forced_lexical,
        fallback_used,
        fallback_reason,
    };

    info!(
        query = %query_text,
        requested_mode = %retrieval_metadata.requested_mode,
        effective_mode = %retrieval_metadata.effective_mode,
        part_filter = ?args.part,
        chunk_type_filter = ?chunk_type_filter,
        node_type_filter = ?node_type_filter,
        result_count = results.len(),
        "query completed"
    );

    if args.json {
        write_json_response(
            query_text,
            limit,
            args.part,
            chunk_type_filter,
            node_type_filter,
            retrieval_metadata,
            results,
        )?;
    } else {
        write_text_response(query_text, &retrieval_metadata, &results)?;
    }

    Ok(())
}

fn retrieval_mode_label(value: RetrievalMode) -> &'static str {
    match value {
        RetrievalMode::Lexical => "lexical",
        RetrievalMode::Semantic => "semantic",
        RetrievalMode::Hybrid => "hybrid",
    }
}

fn fusion_mode_label(value: FusionMode) -> &'static str {
    match value {
        FusionMode::Rrf => "rrf",
    }
}

fn clamp_candidates(value: usize) -> usize {
    value.clamp(1, MAX_QUERY_CANDIDATES)
}

fn sort_candidates(candidates: &mut [QueryCandidate]) {
    candidates.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then(left.part.cmp(&right.part))
            .then(
                left.page_pdf_start
                    .unwrap_or(i64::MAX)
                    .cmp(&right.page_pdf_start.unwrap_or(i64::MAX)),
            )
            .then(left.chunk_id.cmp(&right.chunk_id))
    });
}
