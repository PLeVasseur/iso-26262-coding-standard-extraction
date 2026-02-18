use std::collections::HashMap;
use std::io::{self, Write};

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OpenFlags, params};
use serde::Serialize;
use tracing::info;

use crate::cli::QueryArgs;

const MAX_QUERY_CANDIDATES: i64 = 256;

#[derive(Debug, Clone)]
struct QueryCandidate {
    score: f64,
    match_kind: &'static str,
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
struct QueryResult {
    rank: usize,
    score: f64,
    match_kind: String,
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
struct QueryResponse {
    query: String,
    limit: usize,
    returned: usize,
    part_filter: Option<u32>,
    chunk_type_filter: Option<String>,
    node_type_filter: Option<String>,
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

    let mut dedup = HashMap::<String, QueryCandidate>::new();

    for candidate in query_exact_matches(
        &connection,
        query_text,
        args.part,
        chunk_type_filter.as_deref(),
        node_type_filter.as_deref(),
    )? {
        upsert_candidate(&mut dedup, candidate);
    }

    for candidate in query_fts_matches(
        &connection,
        query_text,
        args.part,
        chunk_type_filter.as_deref(),
        node_type_filter.as_deref(),
    )? {
        upsert_candidate(&mut dedup, candidate);
    }

    if node_type_filter.is_some() {
        for candidate in query_node_matches(
            &connection,
            query_text,
            args.part,
            chunk_type_filter.as_deref(),
            node_type_filter.as_deref(),
        )? {
            upsert_candidate(&mut dedup, candidate);
        }
    }

    let mut candidates: Vec<QueryCandidate> = dedup.into_values().collect();
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

    let limit = args.limit;
    if candidates.len() > limit {
        candidates.truncate(limit);
    }

    let results = to_results(
        &connection,
        candidates,
        args.with_ancestors,
        args.with_descendants,
    )?;

    info!(
        query = %query_text,
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
            results,
        )?;
    } else {
        write_text_response(query_text, &results)?;
    }

    Ok(())
}

