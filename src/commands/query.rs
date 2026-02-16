use std::collections::HashMap;
use std::io::{self, Write};

use anyhow::{bail, Context, Result};
use rusqlite::{params, Connection, OpenFlags};
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

fn query_exact_matches(
    connection: &Connection,
    query_text: &str,
    part_filter: Option<u32>,
    chunk_type_filter: Option<&str>,
    node_type_filter: Option<&str>,
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
        MAX_QUERY_CANDIDATES,
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
            match_kind,
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
          bm25(chunks_fts),
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
        MAX_QUERY_CANDIDATES,
    ])?;

    let mut out = Vec::new();
    let mut index = 0usize;

    while let Some(row) = rows.next()? {
        let snippet: String = row.get(10)?;
        out.push(QueryCandidate {
            score: 500.0 - (index as f64),
            match_kind: "fts",
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
            snippet,
            origin_node_id: row.get(12)?,
            leaf_node_type: row.get(13)?,
            ancestor_path: row.get(14)?,
            anchor_type: row.get(15)?,
            anchor_label_raw: row.get(16)?,
            anchor_label_norm: row.get(17)?,
            anchor_order: row.get(18)?,
            citation_anchor_id: row.get(19)?,
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
        MAX_QUERY_CANDIDATES,
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
            match_kind,
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

fn upsert_candidate(dedup: &mut HashMap<String, QueryCandidate>, candidate: QueryCandidate) {
    match dedup.get(&candidate.chunk_id) {
        Some(existing) if existing.score >= candidate.score => {}
        _ => {
            dedup.insert(candidate.chunk_id.clone(), candidate);
        }
    }
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
            match_kind: candidate.match_kind.to_string(),
            chunk_id: candidate.chunk_id,
            doc_id: candidate.doc_id,
            part: candidate.part,
            year: candidate.year,
            chunk_type: candidate.chunk_type,
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

fn write_json_response(
    query_text: &str,
    limit: usize,
    part_filter: Option<u32>,
    chunk_type_filter: Option<String>,
    node_type_filter: Option<String>,
    results: Vec<QueryResult>,
) -> Result<()> {
    let response = QueryResponse {
        query: query_text.to_string(),
        limit,
        returned: results.len(),
        part_filter,
        chunk_type_filter,
        node_type_filter,
        results,
    };

    let mut output = io::BufWriter::new(io::stdout().lock());
    serde_json::to_writer_pretty(&mut output, &response)
        .context("failed to serialize query json output")?;
    writeln!(output)?;
    output.flush()?;
    Ok(())
}

fn write_text_response(query_text: &str, results: &[QueryResult]) -> Result<()> {
    let mut output = io::BufWriter::new(io::stdout().lock());

    writeln!(output, "Query: {query_text}")?;
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
            "\tmatch={} score={:.3} chunk_id={}",
            result.match_kind, result.score, result.chunk_id
        )?;
        if let Some(origin_node_id) = &result.origin_node_id {
            writeln!(output, "\torigin_node_id: {origin_node_id}")?;
        }
        if let Some(leaf_node_type) = &result.leaf_node_type {
            writeln!(output, "\tleaf_node_type: {leaf_node_type}")?;
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

fn fetch_descendants(connection: &Connection, origin_node_id: &str) -> Result<Vec<DescendantNode>> {
    let mut statement = connection.prepare(
        "
        WITH RECURSIVE descendants(
          node_id, parent_node_id, node_type, ref, heading,
          order_index, page_pdf_start, page_pdf_end, text, depth
        ) AS (
          SELECT
            n.node_id,
            n.parent_node_id,
            n.node_type,
            n.ref,
            n.heading,
            n.order_index,
            n.page_pdf_start,
            n.page_pdf_end,
            n.text,
            1
          FROM nodes n
          WHERE n.parent_node_id = ?1

          UNION ALL

          SELECT
            n.node_id,
            n.parent_node_id,
            n.node_type,
            n.ref,
            n.heading,
            n.order_index,
            n.page_pdf_start,
            n.page_pdf_end,
            n.text,
            d.depth + 1
          FROM nodes n
          JOIN descendants d ON n.parent_node_id = d.node_id
          WHERE d.depth < 8
        )
        SELECT
          node_id,
          parent_node_id,
          node_type,
          ref,
          heading,
          order_index,
          page_pdf_start,
          page_pdf_end,
          substr(COALESCE(text, ''), 1, 180)
        FROM descendants
        ORDER BY depth, order_index, node_id
        LIMIT 256
        ",
    )?;

    let mut rows = statement.query(params![origin_node_id])?;
    let mut descendants = Vec::new();

    while let Some(row) = rows.next()? {
        descendants.push(DescendantNode {
            node_id: row.get(0)?,
            parent_node_id: row.get(1)?,
            node_type: row.get(2)?,
            reference: row.get(3)?,
            heading: row.get(4)?,
            order_index: row.get(5)?,
            page_pdf_start: row.get(6)?,
            page_pdf_end: row.get(7)?,
            text_preview: row
                .get::<_, Option<String>>(8)?
                .map(|value| condense_whitespace(&value)),
        });
    }

    Ok(descendants)
}

fn to_fts_query(query_text: &str) -> String {
    query_text
        .split_whitespace()
        .filter(|token| !token.trim().is_empty())
        .map(|token| format!("\"{}\"", token.replace('"', "")))
        .collect::<Vec<String>>()
        .join(" ")
}

fn format_page_range(start: Option<i64>, end: Option<i64>) -> String {
    match (start, end) {
        (Some(start), Some(end)) if start == end => start.to_string(),
        (Some(start), Some(end)) => format!("{start}-{end}"),
        (Some(start), None) => start.to_string(),
        (None, Some(end)) => end.to_string(),
        (None, None) => "unknown".to_string(),
    }
}

fn render_citation(candidate: &QueryCandidate) -> String {
    let reference = if candidate.reference.is_empty() {
        "(unreferenced chunk)".to_string()
    } else {
        candidate.reference.clone()
    };

    let reference_with_anchor = match (
        candidate.anchor_type.as_deref(),
        candidate.anchor_label_norm.as_deref(),
    ) {
        (Some("marker"), Some(label)) if !label.is_empty() => {
            let base = marker_base_reference(&reference);
            if label.starts_with("NOTE") {
                format!("{base}, {label}")
            } else {
                format!("{base}({label})")
            }
        }
        (Some("paragraph"), Some(label)) if !label.is_empty() => {
            let base = marker_base_reference(&reference);
            format!("{base}, para {label}")
        }
        _ => reference,
    };

    format!(
        "ISO 26262-{}:{}, {}, PDF pages {}",
        candidate.part,
        candidate.year,
        reference_with_anchor,
        format_page_range(candidate.page_pdf_start, candidate.page_pdf_end)
    )
}

fn marker_base_reference(reference: &str) -> String {
    if let Some((base, _)) = reference.split_once(" item ") {
        return base.to_string();
    }

    if let Some((base, _)) = reference.split_once(" para ") {
        return base.to_string();
    }

    if let Some((base, _)) = reference.split_once(" row ") {
        return base.to_string();
    }

    reference.to_string()
}

fn condense_whitespace(input: &str) -> String {
    input.split_whitespace().collect::<Vec<&str>>().join(" ")
}
