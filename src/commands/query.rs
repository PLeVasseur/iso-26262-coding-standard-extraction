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
}

#[derive(Debug, Serialize)]
struct QueryResponse {
    query: String,
    limit: usize,
    returned: usize,
    part_filter: Option<u32>,
    chunk_type_filter: Option<String>,
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

    let mut dedup = HashMap::<String, QueryCandidate>::new();

    for candidate in query_exact_matches(
        &connection,
        query_text,
        args.part,
        chunk_type_filter.as_deref(),
    )? {
        upsert_candidate(&mut dedup, candidate);
    }

    for candidate in query_fts_matches(
        &connection,
        query_text,
        args.part,
        chunk_type_filter.as_deref(),
    )? {
        upsert_candidate(&mut dedup, candidate);
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

    let results = to_results(candidates);

    info!(
        query = %query_text,
        part_filter = ?args.part,
        chunk_type_filter = ?chunk_type_filter,
        result_count = results.len(),
        "query completed"
    );

    if args.json {
        write_json_response(query_text, limit, args.part, chunk_type_filter, results)?;
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
          substr(COALESCE(c.text, ''), 1, 420)
        FROM chunks c
        JOIN docs d ON d.doc_id = c.doc_id
        WHERE
          (?2 IS NULL OR d.part = ?2)
          AND (?3 IS NULL OR c.type = ?3)
          AND (
            lower(c.ref) = lower(?1)
            OR lower(c.heading) = lower(?1)
            OR lower(c.ref) LIKE '%' || lower(?1) || '%'
            OR lower(c.heading) LIKE '%' || lower(?1) || '%'
          )
        LIMIT ?4
        ",
    )?;

    let mut rows = statement.query(params![
        query_text,
        part_filter.map(i64::from),
        chunk_type_filter,
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
        });
    }

    Ok(out)
}

fn query_fts_matches(
    connection: &Connection,
    query_text: &str,
    part_filter: Option<u32>,
    chunk_type_filter: Option<&str>,
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
          bm25(chunks_fts)
        FROM chunks_fts
        JOIN chunks c ON c.rowid = chunks_fts.rowid
        JOIN docs d ON d.doc_id = c.doc_id
        WHERE
          chunks_fts MATCH ?1
          AND (?2 IS NULL OR d.part = ?2)
          AND (?3 IS NULL OR c.type = ?3)
        ORDER BY bm25(chunks_fts) ASC
        LIMIT ?4
        ",
    )?;

    let mut rows = statement.query(params![
        fts_query,
        part_filter.map(i64::from),
        chunk_type_filter,
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
        });
        index += 1;
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

fn to_results(candidates: Vec<QueryCandidate>) -> Vec<QueryResult> {
    candidates
        .into_iter()
        .enumerate()
        .map(|(index, candidate)| {
            let citation = format!(
                "ISO 26262-{}:{}, {}, PDF pages {}",
                candidate.part,
                candidate.year,
                if candidate.reference.is_empty() {
                    "(unreferenced chunk)".to_string()
                } else {
                    candidate.reference.clone()
                },
                format_page_range(candidate.page_pdf_start, candidate.page_pdf_end)
            );

            QueryResult {
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
            }
        })
        .collect()
}

fn write_json_response(
    query_text: &str,
    limit: usize,
    part_filter: Option<u32>,
    chunk_type_filter: Option<String>,
    results: Vec<QueryResult>,
) -> Result<()> {
    let response = QueryResponse {
        query: query_text.to_string(),
        limit,
        returned: results.len(),
        part_filter,
        chunk_type_filter,
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
            "{}.	ISO 26262-{}:{}	{}	{}	pages {}",
            result.rank,
            result.part,
            result.year,
            result.chunk_type,
            reference,
            format_page_range(result.page_pdf_start, result.page_pdf_end)
        )?;
        writeln!(
            output,
            "	match={} score={:.3} chunk_id={}",
            result.match_kind, result.score, result.chunk_id
        )?;
        writeln!(output, "	citation: {}", result.citation)?;
        writeln!(output, "	snippet: {}", result.snippet)?;
    }

    output.flush()?;
    Ok(())
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

fn condense_whitespace(input: &str) -> String {
    input.split_whitespace().collect::<Vec<&str>>().join(" ")
}
