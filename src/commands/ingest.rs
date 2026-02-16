use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use regex::Regex;
use rusqlite::{Connection, params};
use tracing::{info, warn};

use crate::cli::IngestArgs;
use crate::commands::inventory;
use crate::model::{
    IngestCounts, IngestPaths, IngestRunManifest, PdfEntry, PdfInventoryManifest, ToolVersions,
};
use crate::util::{ensure_directory, now_utc_string, utc_compact_string, write_json_pretty};

const DB_SCHEMA_VERSION: &str = "0.2.0";

pub fn run(args: IngestArgs) -> Result<()> {
    let started_ts = Utc::now();
    let started_at = now_utc_string();
    let run_id = format!("run-{}", utc_compact_string(started_ts));

    let cache_root = args.cache_root.clone();
    let manifest_dir = cache_root.join("manifests");
    ensure_directory(&manifest_dir)?;

    let inventory_manifest_path = args
        .inventory_manifest_path
        .clone()
        .unwrap_or_else(|| manifest_dir.join("pdf_inventory.json"));
    let ingest_manifest_path = args.ingest_manifest_path.clone().unwrap_or_else(|| {
        manifest_dir.join(format!(
            "ingest_run_{}.json",
            utc_compact_string(started_ts)
        ))
    });
    let db_path = args
        .db_path
        .clone()
        .unwrap_or_else(|| cache_root.join("iso26262_index.sqlite"));

    info!(cache_root = %cache_root.display(), run_id = %run_id, "starting ingest");

    let inventory = load_or_refresh_inventory(
        &cache_root,
        &inventory_manifest_path,
        args.refresh_inventory,
    )?;

    let tool_versions = collect_tool_versions()?;

    let mut connection = Connection::open(&db_path)
        .with_context(|| format!("failed to open {}", db_path.display()))?;
    configure_connection(&connection)?;
    ensure_schema(&connection)?;

    let docs_upserted = upsert_docs(&mut connection, &inventory)?;

    let parser = StructuredChunkParser::new()?;
    let chunk_stats = insert_chunks(
        &mut connection,
        &cache_root,
        &inventory.pdfs,
        &parser,
        args.max_pages_per_doc,
        args.seed_page_chunks,
        &args.target_parts,
    )?;

    sync_fts_index(&connection)?;

    let docs_total = count_rows(&connection, "SELECT COUNT(*) FROM docs")?;
    let chunks_total = count_rows(&connection, "SELECT COUNT(*) FROM chunks")?;
    let updated_at = now_utc_string();

    let manifest = IngestRunManifest {
        manifest_version: 1,
        run_id: run_id.clone(),
        db_schema_version: DB_SCHEMA_VERSION.to_string(),
        status: "completed".to_string(),
        started_at,
        updated_at,
        completed_steps: vec!["R05-DB-INIT".to_string(), "R05-INGEST".to_string()],
        current_step: "R05-COMPLETE".to_string(),
        failed_step: None,
        failure_reason: None,
        command: render_ingest_command(&args),
        tool_versions,
        paths: IngestPaths {
            cache_root: cache_root.display().to_string(),
            manifest_dir: manifest_dir.display().to_string(),
            inventory_manifest_path: inventory_manifest_path.display().to_string(),
            db_path: db_path.display().to_string(),
        },
        counts: IngestCounts {
            pdf_count: inventory.pdf_count,
            processed_pdf_count: chunk_stats.processed_pdf_count,
            docs_upserted,
            docs_total,
            nodes_total: chunk_stats.nodes_total,
            chunks_total,
            structured_chunks_inserted: chunk_stats.structured_chunks_inserted,
            clause_chunks_inserted: chunk_stats.clause_chunks_inserted,
            table_chunks_inserted: chunk_stats.table_chunks_inserted,
            annex_chunks_inserted: chunk_stats.annex_chunks_inserted,
            page_chunks_inserted: chunk_stats.page_chunks_inserted,
            clause_nodes_inserted: chunk_stats.clause_nodes_inserted,
            subclause_nodes_inserted: chunk_stats.subclause_nodes_inserted,
            annex_nodes_inserted: chunk_stats.annex_nodes_inserted,
            table_nodes_inserted: chunk_stats.table_nodes_inserted,
            table_row_nodes_inserted: chunk_stats.table_row_nodes_inserted,
            table_cell_nodes_inserted: chunk_stats.table_cell_nodes_inserted,
            list_nodes_inserted: chunk_stats.list_nodes_inserted,
            list_item_nodes_inserted: chunk_stats.list_item_nodes_inserted,
            requirement_atom_nodes_inserted: chunk_stats.requirement_atom_nodes_inserted,
            table_raw_fallback_count: chunk_stats.table_raw_fallback_count,
            list_parse_fallback_count: chunk_stats.list_parse_fallback_count,
            ocr_page_count: 0,
        },
        source_hashes: inventory.pdfs,
        warnings: chunk_stats.warnings,
        notes: vec![
            "Ingest command completed using local manifests and sqlite store.".to_string(),
            "Structured chunk extraction uses clause/table/annex heading heuristics from pdftotext text layer."
                .to_string(),
        ],
    };

    write_json_pretty(&ingest_manifest_path, &manifest)?;

    info!(path = %ingest_manifest_path.display(), "wrote ingest run manifest");
    info!(docs = docs_total, chunks = chunks_total, "ingest completed");

    Ok(())
}

fn load_or_refresh_inventory(
    cache_root: &Path,
    inventory_manifest_path: &Path,
    refresh_inventory: bool,
) -> Result<PdfInventoryManifest> {
    if refresh_inventory || !inventory_manifest_path.exists() {
        let manifest = inventory::build_manifest(cache_root)?;
        write_json_pretty(inventory_manifest_path, &manifest)?;
        info!(
            path = %inventory_manifest_path.display(),
            pdf_count = manifest.pdf_count,
            "refreshed inventory manifest"
        );
        return Ok(manifest);
    }

    let raw = fs::read(inventory_manifest_path)
        .with_context(|| format!("failed to read {}", inventory_manifest_path.display()))?;
    let manifest: PdfInventoryManifest = serde_json::from_slice(&raw)
        .with_context(|| format!("failed to parse {}", inventory_manifest_path.display()))?;

    info!(
        path = %inventory_manifest_path.display(),
        pdf_count = manifest.pdf_count,
        "loaded existing inventory manifest"
    );

    Ok(manifest)
}

fn configure_connection(connection: &Connection) -> Result<()> {
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .context("failed to set journal_mode=WAL")?;
    connection
        .pragma_update(None, "synchronous", "NORMAL")
        .context("failed to set synchronous=NORMAL")?;
    Ok(())
}

fn ensure_schema(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS metadata (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS docs (
          doc_id TEXT PRIMARY KEY,
          filename TEXT NOT NULL,
          sha256 TEXT NOT NULL,
          part INTEGER,
          year INTEGER,
          title TEXT
        );

        CREATE TABLE IF NOT EXISTS nodes (
          node_id TEXT PRIMARY KEY,
          parent_node_id TEXT,
          doc_id TEXT NOT NULL,
          node_type TEXT NOT NULL,
          ref TEXT,
          ref_path TEXT,
          heading TEXT,
          order_index INTEGER DEFAULT 0,
          page_pdf_start INTEGER,
          page_pdf_end INTEGER,
          text TEXT,
          source_hash TEXT,
          ancestor_path TEXT,
          FOREIGN KEY(doc_id) REFERENCES docs(doc_id),
          FOREIGN KEY(parent_node_id) REFERENCES nodes(node_id)
        );

        CREATE TABLE IF NOT EXISTS chunks (
          chunk_id TEXT PRIMARY KEY,
          doc_id TEXT NOT NULL,
          type TEXT NOT NULL,
          ref TEXT,
          ref_path TEXT,
          heading TEXT,
          chunk_seq INTEGER DEFAULT 0,
          page_pdf_start INTEGER,
          page_pdf_end INTEGER,
          page_printed_start TEXT,
          page_printed_end TEXT,
          text TEXT,
          table_md TEXT,
          table_csv TEXT,
          source_hash TEXT,
          origin_node_id TEXT,
          leaf_node_type TEXT,
          ancestor_path TEXT,
          FOREIGN KEY(doc_id) REFERENCES docs(doc_id)
        );
        ",
    )?;

    ensure_column_exists(connection, "nodes", "ancestor_path TEXT")?;
    ensure_column_exists(connection, "chunks", "origin_node_id TEXT")?;
    ensure_column_exists(connection, "chunks", "leaf_node_type TEXT")?;
    ensure_column_exists(connection, "chunks", "ancestor_path TEXT")?;

    connection
        .execute(
            "
            CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts
            USING fts5(chunk_id, doc_id, ref, heading, text, content='chunks', content_rowid='rowid')
            ",
            [],
        )
        .context("failed to initialize FTS5 table chunks_fts")?;

    connection.execute_batch(
        "
        CREATE INDEX IF NOT EXISTS idx_nodes_parent ON nodes(parent_node_id);
        CREATE INDEX IF NOT EXISTS idx_nodes_doc_type ON nodes(doc_id, node_type);
        CREATE INDEX IF NOT EXISTS idx_chunks_origin_node ON chunks(origin_node_id);
        ",
    )?;

    let now = now_utc_string();
    connection.execute(
        "INSERT INTO metadata(key, value) VALUES('db_schema_version', ?1)
         ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        [DB_SCHEMA_VERSION],
    )?;
    connection.execute(
        "INSERT INTO metadata(key, value) VALUES('db_updated_at', ?1)
         ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        [now],
    )?;

    Ok(())
}

fn ensure_column_exists(
    connection: &Connection,
    table_name: &str,
    column_definition: &str,
) -> Result<()> {
    let Some(column_name) = column_definition.split_whitespace().next() else {
        bail!("invalid column definition: {column_definition}");
    };

    let pragma_sql = format!("PRAGMA table_info({table_name})");
    let mut statement = connection
        .prepare(&pragma_sql)
        .with_context(|| format!("failed to inspect schema for table {table_name}"))?;

    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let existing_name: String = row.get(1)?;
        if existing_name == column_name {
            return Ok(());
        }
    }

    let alter_sql = format!("ALTER TABLE {table_name} ADD COLUMN {column_definition}");
    connection
        .execute(&alter_sql, [])
        .with_context(|| format!("failed to add column {column_name} on {table_name}"))?;

    Ok(())
}

fn upsert_docs(connection: &mut Connection, inventory: &PdfInventoryManifest) -> Result<usize> {
    let tx = connection.transaction()?;

    {
        let mut statement = tx.prepare(
            "
            INSERT INTO docs(doc_id, filename, sha256, part, year, title)
            VALUES(?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(doc_id) DO UPDATE SET
              filename=excluded.filename,
              sha256=excluded.sha256,
              part=excluded.part,
              year=excluded.year,
              title=excluded.title
            ",
        )?;

        for pdf in &inventory.pdfs {
            let doc_id = doc_id_for(pdf);
            let title = format!("ISO 26262-{}:{}", pdf.part, pdf.year);

            statement.execute(params![
                doc_id,
                &pdf.filename,
                &pdf.sha256,
                pdf.part,
                pdf.year,
                title
            ])?;
        }
    }

    tx.commit()?;
    Ok(inventory.pdfs.len())
}

#[derive(Debug, Default)]
struct ChunkInsertStats {
    processed_pdf_count: usize,
    structured_chunks_inserted: usize,
    clause_chunks_inserted: usize,
    table_chunks_inserted: usize,
    annex_chunks_inserted: usize,
    page_chunks_inserted: usize,
    nodes_total: i64,
    clause_nodes_inserted: usize,
    subclause_nodes_inserted: usize,
    annex_nodes_inserted: usize,
    table_nodes_inserted: usize,
    table_row_nodes_inserted: usize,
    table_cell_nodes_inserted: usize,
    list_nodes_inserted: usize,
    list_item_nodes_inserted: usize,
    requirement_atom_nodes_inserted: usize,
    table_raw_fallback_count: usize,
    list_parse_fallback_count: usize,
    warnings: Vec<String>,
}

#[derive(Debug, Clone)]
struct StructuredChunkDraft {
    chunk_type: ChunkType,
    reference: String,
    ref_path: String,
    heading: String,
    text: String,
    page_start: i64,
    page_end: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChunkType {
    Clause,
    Table,
    Annex,
}

impl ChunkType {
    fn as_str(self) -> &'static str {
        match self {
            ChunkType::Clause => "clause",
            ChunkType::Table => "table",
            ChunkType::Annex => "annex",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeType {
    Document,
    Clause,
    Subclause,
    Annex,
    Table,
    TableRow,
    TableCell,
    List,
    ListItem,
    RequirementAtom,
    Page,
}

impl NodeType {
    fn as_str(self) -> &'static str {
        match self {
            NodeType::Document => "document",
            NodeType::Clause => "clause",
            NodeType::Subclause => "subclause",
            NodeType::Annex => "annex",
            NodeType::Table => "table",
            NodeType::TableRow => "table_row",
            NodeType::TableCell => "table_cell",
            NodeType::List => "list",
            NodeType::ListItem => "list_item",
            NodeType::RequirementAtom => "requirement_atom",
            NodeType::Page => "page",
        }
    }
}

#[derive(Debug)]
struct ParsedTableRows {
    rows: Vec<Vec<String>>,
    markdown: Option<String>,
    csv: Option<String>,
    used_fallback: bool,
}

#[derive(Debug)]
struct ListItemDraft {
    marker: String,
    text: String,
    depth: i64,
}

#[derive(Debug)]
struct StructuredChunkParser {
    clause_heading: Regex,
    table_heading: Regex,
    annex_heading: Regex,
    toc_line: Regex,
}

impl StructuredChunkParser {
    fn new() -> Result<Self> {
        Ok(Self {
            clause_heading: Regex::new(r"^\s*(\d+(?:\.\d+)+)\s+(.+)$")
                .context("failed to compile clause heading regex")?,
            table_heading: Regex::new(r"^\s*(Table\s+\d+)\s*[-:–—]?\s*(.*)$")
                .context("failed to compile table heading regex")?,
            annex_heading: Regex::new(r"^\s*(Annex\s+[A-Z])(?:\s*\([^)]*\))?\s*[-:–—]?\s*(.*)$")
                .context("failed to compile annex heading regex")?,
            toc_line: Regex::new(r"\.{3,}\s*\d+\s*$")
                .context("failed to compile table-of-contents line regex")?,
        })
    }

    fn parse_pages(&self, pages: &[String]) -> Vec<StructuredChunkDraft> {
        #[derive(Debug)]
        struct ActiveChunk {
            chunk_type: ChunkType,
            reference: String,
            heading: String,
            page_start: i64,
            page_end: i64,
            body_lines: Vec<String>,
        }

        fn finalize(active: ActiveChunk) -> StructuredChunkDraft {
            let body = active.body_lines.join("\n").trim().to_string();
            let text = if body.is_empty() {
                active.heading.clone()
            } else {
                format!("{}\n\n{}", active.heading, body)
            };
            let ref_path = derive_ref_path(&active.reference, active.chunk_type);

            StructuredChunkDraft {
                chunk_type: active.chunk_type,
                reference: active.reference,
                ref_path,
                heading: active.heading,
                text,
                page_start: active.page_start,
                page_end: active.page_end,
            }
        }

        let mut chunks = Vec::new();
        let mut current: Option<ActiveChunk> = None;

        for (page_index, page_text) in pages.iter().enumerate() {
            let page_number = (page_index + 1) as i64;
            for raw_line in page_text.lines() {
                let line = normalize_line(raw_line);
                if line.is_empty() {
                    continue;
                }

                if let Some((chunk_type, reference, heading)) = self.detect_heading(line) {
                    if let Some(active) = current.take() {
                        chunks.push(finalize(active));
                    }

                    current = Some(ActiveChunk {
                        chunk_type,
                        reference,
                        heading,
                        page_start: page_number,
                        page_end: page_number,
                        body_lines: Vec::new(),
                    });
                    continue;
                }

                if let Some(active) = current.as_mut() {
                    active.page_end = page_number;
                    active.body_lines.push(line.to_string());
                }
            }
        }

        if let Some(active) = current.take() {
            chunks.push(finalize(active));
        }

        chunks
    }

    fn detect_heading(&self, line: &str) -> Option<(ChunkType, String, String)> {
        if self.toc_line.is_match(line) {
            return None;
        }

        if let Some(captures) = self.table_heading.captures(line) {
            let reference = captures.get(1).map(|m| m.as_str().trim().to_string())?;
            return Some((ChunkType::Table, reference, line.to_string()));
        }

        if let Some(captures) = self.annex_heading.captures(line) {
            let reference = captures.get(1).map(|m| m.as_str().trim().to_string())?;
            return Some((ChunkType::Annex, reference, line.to_string()));
        }

        if let Some(captures) = self.clause_heading.captures(line) {
            let reference = captures.get(1).map(|m| m.as_str().trim().to_string())?;
            let title = captures.get(2).map(|m| m.as_str().trim()).unwrap_or("");
            if title.is_empty() || title.len() > 140 {
                return None;
            }

            return Some((ChunkType::Clause, reference, line.to_string()));
        }

        None
    }
}

fn insert_chunks(
    connection: &mut Connection,
    cache_root: &Path,
    pdfs: &[PdfEntry],
    parser: &StructuredChunkParser,
    max_pages_per_doc: Option<usize>,
    seed_page_chunks: bool,
    target_parts: &[u32],
) -> Result<ChunkInsertStats> {
    let target_set: HashSet<u32> = target_parts.iter().copied().collect();
    let tx = connection.transaction()?;
    let mut stats = ChunkInsertStats::default();
    let list_item_regex = Regex::new(r"^(?P<marker>(?:\d+|[A-Za-z])[\.)]|[-*•])\s+(?P<body>.+)$")
        .context("failed to compile list item regex")?;
    let table_cell_split_regex =
        Regex::new(r"\t+|\s{2,}").context("failed to compile table cell split regex")?;
    let requirement_split_regex =
        Regex::new(r"[.;]\s+").context("failed to compile requirement split regex")?;
    let requirement_keyword_regex = Regex::new(r"(?i)\bshall(?:\s+not)?\b|\bshould\b")
        .context("failed to compile requirement keyword regex")?;

    {
        let mut chunk_statement = tx.prepare(
            "
            INSERT INTO chunks(
              chunk_id, doc_id, type, ref, ref_path, heading, chunk_seq,
              page_pdf_start, page_pdf_end, text, table_md, table_csv, source_hash,
              origin_node_id, leaf_node_type, ancestor_path
            )
            VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
            ON CONFLICT(chunk_id) DO UPDATE SET
              doc_id=excluded.doc_id,
              type=excluded.type,
              ref=excluded.ref,
              ref_path=excluded.ref_path,
              heading=excluded.heading,
              chunk_seq=excluded.chunk_seq,
              page_pdf_start=excluded.page_pdf_start,
              page_pdf_end=excluded.page_pdf_end,
              text=excluded.text,
              table_md=excluded.table_md,
              table_csv=excluded.table_csv,
              source_hash=excluded.source_hash,
              origin_node_id=excluded.origin_node_id,
              leaf_node_type=excluded.leaf_node_type,
              ancestor_path=excluded.ancestor_path
            ",
        )?;

        let mut node_statement = tx.prepare(
            "
            INSERT INTO nodes(
              node_id, parent_node_id, doc_id, node_type, ref, ref_path, heading,
              order_index, page_pdf_start, page_pdf_end, text, source_hash, ancestor_path
            )
            VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            ON CONFLICT(node_id) DO UPDATE SET
              parent_node_id=excluded.parent_node_id,
              doc_id=excluded.doc_id,
              node_type=excluded.node_type,
              ref=excluded.ref,
              ref_path=excluded.ref_path,
              heading=excluded.heading,
              order_index=excluded.order_index,
              page_pdf_start=excluded.page_pdf_start,
              page_pdf_end=excluded.page_pdf_end,
              text=excluded.text,
              source_hash=excluded.source_hash,
              ancestor_path=excluded.ancestor_path
            ",
        )?;

        for pdf in pdfs {
            if !target_set.is_empty() && !target_set.contains(&pdf.part) {
                continue;
            }

            stats.processed_pdf_count += 1;

            let doc_id = doc_id_for(pdf);
            tx.execute("DELETE FROM chunks WHERE doc_id = ?1", [&doc_id])?;
            tx.execute("DELETE FROM nodes WHERE doc_id = ?1", [&doc_id])?;

            let pdf_path = cache_root.join(&pdf.filename);
            if !pdf_path.exists() {
                stats
                    .warnings
                    .push(format!("missing source PDF: {}", pdf_path.display()));
                continue;
            }

            let pages = match extract_pages_with_pdftotext(&pdf_path, max_pages_per_doc) {
                Ok(pages) => pages,
                Err(err) => {
                    let warning =
                        format!("failed to extract text for {}: {err}", pdf_path.display());
                    warn!(warning = %warning, "pdf extraction warning");
                    stats.warnings.push(warning);
                    continue;
                }
            };

            let document_node_id = format!("{}:node:document", doc_id);
            let document_path = format!("document:{}", doc_id);
            let page_count = pages.len() as i64;
            insert_node(
                &mut node_statement,
                &document_node_id,
                None,
                &doc_id,
                NodeType::Document,
                None,
                None,
                Some(&format!("ISO 26262 Part {}", pdf.part)),
                0,
                Some(1),
                Some(page_count),
                None,
                &pdf.sha256,
                &document_path,
            )?;

            stats.nodes_total += 1;

            let mut node_paths = HashMap::<String, String>::new();
            node_paths.insert(document_node_id.clone(), document_path);

            let mut node_key_counts = HashMap::<String, i64>::new();
            let mut chunk_key_counts = HashMap::<String, i64>::new();
            let mut clause_ref_to_node_id = HashMap::<String, String>::new();
            let mut last_clause_node_id: Option<String> = None;
            let mut node_order_index: i64 = 1;

            let structured_chunks = parser.parse_pages(&pages);
            let mut structured_seq: i64 = 1;

            for chunk in structured_chunks {
                let origin_node_type = chunk_origin_node_type(chunk.chunk_type, &chunk.reference);
                let parent_node_id = match chunk.chunk_type {
                    ChunkType::Table => last_clause_node_id
                        .clone()
                        .unwrap_or_else(|| document_node_id.clone()),
                    ChunkType::Clause => {
                        find_parent_clause_node_id(&chunk.reference, &clause_ref_to_node_id)
                            .unwrap_or_else(|| document_node_id.clone())
                    }
                    ChunkType::Annex => document_node_id.clone(),
                };

                let ref_key = sanitize_ref_for_id(&chunk.reference);
                let node_count = node_key_counts
                    .entry(format!("{}:{}", origin_node_type.as_str(), ref_key))
                    .and_modify(|value| *value += 1)
                    .or_insert(1);

                let origin_node_id = format!(
                    "{}:node:{}:{}:{:03}",
                    doc_id,
                    origin_node_type.as_str(),
                    ref_key,
                    node_count
                );

                let ancestor_path = build_ancestor_path(
                    Some(&parent_node_id),
                    &node_paths,
                    origin_node_type,
                    &chunk.reference,
                    &chunk.heading,
                );

                insert_node(
                    &mut node_statement,
                    &origin_node_id,
                    Some(&parent_node_id),
                    &doc_id,
                    origin_node_type,
                    Some(&chunk.reference),
                    Some(&chunk.ref_path),
                    Some(&chunk.heading),
                    node_order_index,
                    Some(chunk.page_start),
                    Some(chunk.page_end),
                    Some(&chunk.text),
                    &pdf.sha256,
                    &ancestor_path,
                )?;

                node_paths.insert(origin_node_id.clone(), ancestor_path.clone());
                node_order_index += 1;
                stats.nodes_total += 1;
                increment_node_type_stat(&mut stats, origin_node_type);

                if matches!(origin_node_type, NodeType::Clause | NodeType::Subclause) {
                    clause_ref_to_node_id.insert(chunk.reference.clone(), origin_node_id.clone());
                    last_clause_node_id = Some(origin_node_id.clone());
                }

                let (table_md, table_csv, parsed_table_rows) =
                    if chunk.chunk_type == ChunkType::Table {
                        let parsed =
                            parse_table_rows(&chunk.text, &chunk.heading, &table_cell_split_regex);
                        (parsed.markdown.clone(), parsed.csv.clone(), Some(parsed))
                    } else {
                        (None::<String>, None::<String>, None::<ParsedTableRows>)
                    };

                if parsed_table_rows
                    .as_ref()
                    .is_some_and(|parsed| parsed.used_fallback)
                {
                    stats.table_raw_fallback_count += 1;
                }

                let chunk_count = chunk_key_counts
                    .entry(format!("{}:{}", chunk.chunk_type.as_str(), ref_key))
                    .and_modify(|value| *value += 1)
                    .or_insert(1);

                let chunk_id = format!(
                    "{}:{}:{}:{:03}",
                    doc_id,
                    chunk.chunk_type.as_str(),
                    ref_key,
                    chunk_count
                );

                chunk_statement.execute(params![
                    chunk_id,
                    &doc_id,
                    chunk.chunk_type.as_str(),
                    &chunk.reference,
                    &chunk.ref_path,
                    &chunk.heading,
                    structured_seq,
                    chunk.page_start,
                    chunk.page_end,
                    &chunk.text,
                    &table_md,
                    &table_csv,
                    &pdf.sha256,
                    &origin_node_id,
                    origin_node_type.as_str(),
                    &ancestor_path
                ])?;

                stats.structured_chunks_inserted += 1;
                match chunk.chunk_type {
                    ChunkType::Clause => stats.clause_chunks_inserted += 1,
                    ChunkType::Table => stats.table_chunks_inserted += 1,
                    ChunkType::Annex => stats.annex_chunks_inserted += 1,
                }

                if let Some(parsed) = parsed_table_rows {
                    insert_table_child_nodes(
                        &mut node_statement,
                        &doc_id,
                        &origin_node_id,
                        &ancestor_path,
                        &chunk.reference,
                        &parsed,
                        chunk.page_start,
                        chunk.page_end,
                        &pdf.sha256,
                        &mut node_order_index,
                        &mut stats,
                    )?;
                }

                if matches!(
                    origin_node_type,
                    NodeType::Clause | NodeType::Subclause | NodeType::Annex
                ) {
                    let (list_items, list_fallback) =
                        parse_list_items(&chunk.text, &chunk.heading, &list_item_regex);
                    if !list_items.is_empty() {
                        insert_list_nodes(
                            &mut node_statement,
                            &doc_id,
                            &origin_node_id,
                            &ancestor_path,
                            &chunk.reference,
                            &list_items,
                            chunk.page_start,
                            chunk.page_end,
                            &pdf.sha256,
                            &mut node_order_index,
                            &mut stats,
                        )?;
                    } else if list_fallback {
                        stats.list_parse_fallback_count += 1;
                    }

                    let requirement_atoms = parse_requirement_atoms(
                        &chunk.text,
                        &chunk.heading,
                        &requirement_split_regex,
                        &requirement_keyword_regex,
                    );
                    if !requirement_atoms.is_empty() {
                        insert_requirement_atom_nodes(
                            &mut node_statement,
                            &doc_id,
                            &origin_node_id,
                            &ancestor_path,
                            &chunk.reference,
                            &requirement_atoms,
                            chunk.page_start,
                            chunk.page_end,
                            &pdf.sha256,
                            &mut node_order_index,
                            &mut stats,
                        )?;
                    }
                }

                structured_seq += 1;
            }

            if seed_page_chunks {
                for (index, page_text) in pages.into_iter().enumerate() {
                    let text = page_text.trim();
                    if text.is_empty() {
                        continue;
                    }

                    let page_number = (index + 1) as i64;
                    let chunk_id = format!("{}:page:{:04}", doc_id, page_number);
                    let page_ref = format!("PDF page {}", page_number);
                    let heading = format!("Page {}", page_number);
                    let page_node_id = format!("{}:node:page:{:04}", doc_id, page_number);
                    let page_ancestor_path = build_ancestor_path(
                        Some(&document_node_id),
                        &node_paths,
                        NodeType::Page,
                        &page_ref,
                        &heading,
                    );

                    insert_node(
                        &mut node_statement,
                        &page_node_id,
                        Some(&document_node_id),
                        &doc_id,
                        NodeType::Page,
                        Some(&page_ref),
                        Some(&page_ref),
                        Some(&heading),
                        node_order_index,
                        Some(page_number),
                        Some(page_number),
                        Some(text),
                        &pdf.sha256,
                        &page_ancestor_path,
                    )?;
                    node_order_index += 1;
                    stats.nodes_total += 1;

                    chunk_statement.execute(params![
                        chunk_id,
                        &doc_id,
                        "page",
                        &page_ref,
                        &page_ref,
                        &heading,
                        page_number,
                        page_number,
                        page_number,
                        text,
                        Option::<String>::None,
                        Option::<String>::None,
                        &pdf.sha256,
                        &page_node_id,
                        NodeType::Page.as_str(),
                        &page_ancestor_path
                    ])?;
                    stats.page_chunks_inserted += 1;
                }
            }
        }
    }

    tx.commit()?;
    Ok(stats)
}

fn increment_node_type_stat(stats: &mut ChunkInsertStats, node_type: NodeType) {
    match node_type {
        NodeType::Clause => stats.clause_nodes_inserted += 1,
        NodeType::Subclause => stats.subclause_nodes_inserted += 1,
        NodeType::Annex => stats.annex_nodes_inserted += 1,
        NodeType::Table => stats.table_nodes_inserted += 1,
        NodeType::TableRow => stats.table_row_nodes_inserted += 1,
        NodeType::TableCell => stats.table_cell_nodes_inserted += 1,
        NodeType::List => stats.list_nodes_inserted += 1,
        NodeType::ListItem => stats.list_item_nodes_inserted += 1,
        NodeType::RequirementAtom => stats.requirement_atom_nodes_inserted += 1,
        NodeType::Document | NodeType::Page => {}
    }
}

fn chunk_origin_node_type(chunk_type: ChunkType, reference: &str) -> NodeType {
    match chunk_type {
        ChunkType::Clause => {
            let depth = reference.split('.').count();
            if depth > 2 {
                NodeType::Subclause
            } else {
                NodeType::Clause
            }
        }
        ChunkType::Table => NodeType::Table,
        ChunkType::Annex => NodeType::Annex,
    }
}

fn find_parent_clause_node_id(
    reference: &str,
    clause_ref_to_node_id: &HashMap<String, String>,
) -> Option<String> {
    let mut parts = reference.split('.').collect::<Vec<&str>>();
    while parts.len() > 1 {
        parts.pop();
        let parent_ref = parts.join(".");
        if let Some(parent) = clause_ref_to_node_id.get(&parent_ref) {
            return Some(parent.clone());
        }
    }

    None
}

fn build_ancestor_path(
    parent_node_id: Option<&str>,
    node_paths: &HashMap<String, String>,
    node_type: NodeType,
    reference: &str,
    heading: &str,
) -> String {
    let node_label = if !reference.is_empty() {
        format!("{}:{}", node_type.as_str(), reference)
    } else if !heading.is_empty() {
        format!("{}:{}", node_type.as_str(), heading)
    } else {
        format!("{}:unlabeled", node_type.as_str())
    };

    if let Some(parent) = parent_node_id.and_then(|node_id| node_paths.get(node_id)) {
        format!("{} > {}", parent, node_label)
    } else {
        node_label
    }
}

#[allow(clippy::too_many_arguments)]
fn insert_node(
    statement: &mut rusqlite::Statement<'_>,
    node_id: &str,
    parent_node_id: Option<&str>,
    doc_id: &str,
    node_type: NodeType,
    ref_value: Option<&str>,
    ref_path: Option<&str>,
    heading: Option<&str>,
    order_index: i64,
    page_start: Option<i64>,
    page_end: Option<i64>,
    text: Option<&str>,
    source_hash: &str,
    ancestor_path: &str,
) -> Result<()> {
    statement.execute(params![
        node_id,
        parent_node_id,
        doc_id,
        node_type.as_str(),
        ref_value,
        ref_path,
        heading,
        order_index,
        page_start,
        page_end,
        text,
        source_hash,
        ancestor_path
    ])?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_table_child_nodes(
    node_statement: &mut rusqlite::Statement<'_>,
    doc_id: &str,
    table_node_id: &str,
    table_ancestor_path: &str,
    table_reference: &str,
    parsed_table: &ParsedTableRows,
    page_start: i64,
    page_end: i64,
    source_hash: &str,
    node_order_index: &mut i64,
    stats: &mut ChunkInsertStats,
) -> Result<()> {
    for (row_idx, row_cells) in parsed_table.rows.iter().enumerate() {
        let row_node_id = format!("{}:row:{:03}", table_node_id, row_idx + 1);
        let row_ref = format!("{} row {}", table_reference, row_idx + 1);
        let row_heading = format!("{} row {}", table_reference, row_idx + 1);
        let row_text = row_cells.join(" | ");
        let row_path = format!("{} > table_row:{}", table_ancestor_path, row_idx + 1);

        insert_node(
            node_statement,
            &row_node_id,
            Some(table_node_id),
            doc_id,
            NodeType::TableRow,
            Some(&row_ref),
            Some(&row_ref),
            Some(&row_heading),
            *node_order_index,
            Some(page_start),
            Some(page_end),
            Some(&row_text),
            source_hash,
            &row_path,
        )?;

        *node_order_index += 1;
        stats.nodes_total += 1;
        increment_node_type_stat(stats, NodeType::TableRow);

        for (col_idx, cell_text) in row_cells.iter().enumerate() {
            let cell_node_id = format!(
                "{}:cell:{:03}:{:03}",
                table_node_id,
                row_idx + 1,
                col_idx + 1
            );
            let cell_ref = format!("{} r{}c{}", table_reference, row_idx + 1, col_idx + 1);
            let cell_heading = format!("{} r{}c{}", table_reference, row_idx + 1, col_idx + 1);
            let cell_path = format!("{} > table_cell:r{}c{}", row_path, row_idx + 1, col_idx + 1);

            insert_node(
                node_statement,
                &cell_node_id,
                Some(&row_node_id),
                doc_id,
                NodeType::TableCell,
                Some(&cell_ref),
                Some(&cell_ref),
                Some(&cell_heading),
                *node_order_index,
                Some(page_start),
                Some(page_end),
                Some(cell_text),
                source_hash,
                &cell_path,
            )?;

            *node_order_index += 1;
            stats.nodes_total += 1;
            increment_node_type_stat(stats, NodeType::TableCell);
        }
    }

    Ok(())
}

fn parse_table_rows(text: &str, heading: &str, cell_split_regex: &Regex) -> ParsedTableRows {
    let body_lines = extract_body_lines(text, heading);
    let mut rows = Vec::<Vec<String>>::new();

    for line in body_lines {
        if line_is_noise(line) {
            continue;
        }

        let cells = split_table_cells(line, cell_split_regex);
        if !cells.is_empty() {
            rows.push(cells);
        }
    }

    let structured = rows.len() >= 2 && rows.iter().any(|cells| cells.len() > 1);
    let markdown = if rows.is_empty() {
        None
    } else {
        Some(table_to_markdown(&rows))
    };
    let csv = if rows.is_empty() {
        None
    } else {
        Some(table_to_csv(&rows))
    };

    ParsedTableRows {
        rows,
        markdown,
        csv,
        used_fallback: !structured,
    }
}

fn extract_body_lines<'a>(text: &'a str, heading: &str) -> Vec<&'a str> {
    let mut lines = text.lines().collect::<Vec<&str>>();
    if let Some(first) = lines.first() {
        if first.trim() == heading.trim() {
            lines.remove(0);
        }
    }
    lines
        .into_iter()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect()
}

fn line_is_noise(line: &str) -> bool {
    let lower = line.to_lowercase();
    lower.contains("license")
        || lower.contains("downloaded:")
        || lower.contains("single user")
        || lower.contains("networking prohibited")
}

fn split_table_cells(line: &str, cell_split_regex: &Regex) -> Vec<String> {
    let mut cells = cell_split_regex
        .split(line)
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<String>>();

    if cells.len() <= 1 && line.contains('|') {
        cells = line
            .split('|')
            .map(str::trim)
            .filter(|segment| !segment.is_empty())
            .map(ToOwned::to_owned)
            .collect();
    }

    if cells.is_empty() {
        vec![line.trim().to_string()]
    } else {
        cells
    }
}

fn table_to_markdown(rows: &[Vec<String>]) -> String {
    let col_count = rows.iter().map(|row| row.len()).max().unwrap_or(1).max(1);
    let mut padded_rows = rows
        .iter()
        .map(|row| {
            let mut current = row.clone();
            while current.len() < col_count {
                current.push(String::new());
            }
            current
        })
        .collect::<Vec<Vec<String>>>();

    if padded_rows.is_empty() {
        padded_rows.push(vec![String::new(); col_count]);
    }

    let header = padded_rows.first().cloned().unwrap_or_default();
    let mut lines = Vec::<String>::new();
    lines.push(format!("| {} |", header.join(" | ")));
    lines.push(format!(
        "| {} |",
        (0..col_count)
            .map(|_| "---")
            .collect::<Vec<&str>>()
            .join(" | ")
    ));

    for row in padded_rows.iter().skip(1) {
        lines.push(format!("| {} |", row.join(" | ")));
    }

    lines.join("\n")
}

fn table_to_csv(rows: &[Vec<String>]) -> String {
    rows.iter()
        .map(|row| {
            row.iter()
                .map(|cell| escape_csv_cell(cell))
                .collect::<Vec<String>>()
                .join(",")
        })
        .collect::<Vec<String>>()
        .join("\n")
}

fn escape_csv_cell(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn parse_list_items(
    text: &str,
    heading: &str,
    list_item_regex: &Regex,
) -> (Vec<ListItemDraft>, bool) {
    let body_lines = extract_body_lines(text, heading);
    let mut items = Vec::<ListItemDraft>::new();
    let mut list_like_lines = 0usize;

    for line in body_lines {
        if list_item_regex.is_match(line) {
            list_like_lines += 1;
        }

        if let Some(captures) = list_item_regex.captures(line) {
            let marker = captures
                .name("marker")
                .map(|value| value.as_str().to_string())
                .unwrap_or_else(|| "-".to_string());
            let body = captures
                .name("body")
                .map(|value| value.as_str().trim().to_string())
                .unwrap_or_default();

            if !body.is_empty() {
                items.push(ListItemDraft {
                    marker,
                    text: body,
                    depth: 1,
                });
            }
        }
    }

    let used_fallback = list_like_lines > 0 && items.is_empty();
    (items, used_fallback)
}

#[allow(clippy::too_many_arguments)]
fn insert_list_nodes(
    node_statement: &mut rusqlite::Statement<'_>,
    doc_id: &str,
    parent_node_id: &str,
    parent_path: &str,
    reference: &str,
    list_items: &[ListItemDraft],
    page_start: i64,
    page_end: i64,
    source_hash: &str,
    node_order_index: &mut i64,
    stats: &mut ChunkInsertStats,
) -> Result<()> {
    let list_node_id = format!("{}:list:001", parent_node_id);
    let list_ref = format!("{} list", reference);
    let list_heading = format!("{} list", reference);
    let list_path = format!("{} > list:{}", parent_path, reference);

    insert_node(
        node_statement,
        &list_node_id,
        Some(parent_node_id),
        doc_id,
        NodeType::List,
        Some(&list_ref),
        Some(&list_ref),
        Some(&list_heading),
        *node_order_index,
        Some(page_start),
        Some(page_end),
        None,
        source_hash,
        &list_path,
    )?;

    *node_order_index += 1;
    stats.nodes_total += 1;
    increment_node_type_stat(stats, NodeType::List);

    for (item_idx, item) in list_items.iter().enumerate() {
        let list_item_node_id = format!("{}:item:{:03}", list_node_id, item_idx + 1);
        let list_item_ref = format!("{} item {}", reference, item_idx + 1);
        let list_item_heading = format!("{} {}", item.marker, item.text);
        let list_item_path = format!("{} > list_item:{}", list_path, item_idx + 1);

        insert_node(
            node_statement,
            &list_item_node_id,
            Some(&list_node_id),
            doc_id,
            NodeType::ListItem,
            Some(&list_item_ref),
            Some(&list_item_ref),
            Some(&list_item_heading),
            *node_order_index,
            Some(page_start),
            Some(page_end),
            Some(&item.text),
            source_hash,
            &list_item_path,
        )?;

        *node_order_index += 1;
        stats.nodes_total += 1;
        increment_node_type_stat(stats, NodeType::ListItem);
        let _ = item.depth;
    }

    Ok(())
}

fn parse_requirement_atoms(
    text: &str,
    heading: &str,
    split_regex: &Regex,
    keyword_regex: &Regex,
) -> Vec<String> {
    let body = extract_body_lines(text, heading).join(" ");

    split_regex
        .split(&body)
        .map(str::trim)
        .filter(|sentence| !sentence.is_empty())
        .filter(|sentence| keyword_regex.is_match(sentence))
        .map(ToOwned::to_owned)
        .collect::<Vec<String>>()
}

#[allow(clippy::too_many_arguments)]
fn insert_requirement_atom_nodes(
    node_statement: &mut rusqlite::Statement<'_>,
    doc_id: &str,
    parent_node_id: &str,
    parent_path: &str,
    reference: &str,
    atoms: &[String],
    page_start: i64,
    page_end: i64,
    source_hash: &str,
    node_order_index: &mut i64,
    stats: &mut ChunkInsertStats,
) -> Result<()> {
    for (index, atom) in atoms.iter().enumerate() {
        let node_id = format!("{}:req:{:03}", parent_node_id, index + 1);
        let atom_ref = format!("{} req {}", reference, index + 1);
        let atom_path = format!("{} > requirement_atom:{}", parent_path, index + 1);
        let atom_heading = format!("Requirement atom {}", index + 1);

        insert_node(
            node_statement,
            &node_id,
            Some(parent_node_id),
            doc_id,
            NodeType::RequirementAtom,
            Some(&atom_ref),
            Some(&atom_ref),
            Some(&atom_heading),
            *node_order_index,
            Some(page_start),
            Some(page_end),
            Some(atom),
            source_hash,
            &atom_path,
        )?;

        *node_order_index += 1;
        stats.nodes_total += 1;
        increment_node_type_stat(stats, NodeType::RequirementAtom);
    }

    Ok(())
}

fn derive_ref_path(reference: &str, chunk_type: ChunkType) -> String {
    match chunk_type {
        ChunkType::Clause => reference.split('.').collect::<Vec<&str>>().join(" > "),
        ChunkType::Table | ChunkType::Annex => reference.to_string(),
    }
}

fn normalize_line(input: &str) -> &str {
    input.trim()
}

fn sanitize_ref_for_id(reference: &str) -> String {
    let mut out = String::with_capacity(reference.len());
    for ch in reference.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }

    while out.contains("__") {
        out = out.replace("__", "_");
    }

    out.trim_matches('_').to_string()
}

fn extract_pages_with_pdftotext(
    pdf_path: &Path,
    max_pages_per_doc: Option<usize>,
) -> Result<Vec<String>> {
    let mut command = Command::new("pdftotext");
    command.arg("-enc").arg("UTF-8").arg("-f").arg("1");
    if let Some(max_pages) = max_pages_per_doc {
        command.arg("-l").arg(max_pages.to_string());
    }
    command.arg(pdf_path).arg("-");

    let output = command
        .output()
        .with_context(|| format!("failed to execute pdftotext for {}", pdf_path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "pdftotext returned non-zero exit status for {}: {}",
            pdf_path.display(),
            stderr.trim()
        );
    }

    let raw = String::from_utf8_lossy(&output.stdout);
    let mut pages: Vec<String> = raw
        .split('\u{000C}')
        .map(|chunk| chunk.replace('\u{0000}', ""))
        .collect();

    while let Some(last_page) = pages.last() {
        if last_page.trim().is_empty() {
            pages.pop();
            continue;
        }
        break;
    }

    Ok(pages)
}

fn sync_fts_index(connection: &Connection) -> Result<()> {
    connection
        .execute("INSERT INTO chunks_fts(chunks_fts) VALUES('rebuild')", [])
        .context("failed to rebuild FTS index")?;
    Ok(())
}

fn count_rows(connection: &Connection, sql: &str) -> Result<i64> {
    let count = connection.query_row(sql, [], |row| row.get(0))?;
    Ok(count)
}

fn collect_tool_versions() -> Result<ToolVersions> {
    Ok(ToolVersions {
        rustc: command_version("rustc", &["--version"])?,
        cargo: command_version("cargo", &["--version"])?,
        pdftotext: command_version("pdftotext", &["-v"])?,
    })
}

fn command_version(program: &str, args: &[&str]) -> Result<String> {
    let output = Command::new(program)
        .args(args)
        .output()
        .with_context(|| format!("failed to run {} {}", program, args.join(" ")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("{} {} failed: {}", program, args.join(" "), stderr.trim());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let source = if stdout.trim().is_empty() {
        stderr.trim()
    } else {
        stdout.trim()
    };

    let version_line = source
        .lines()
        .next()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .unwrap_or("unknown");

    Ok(version_line.to_string())
}

fn doc_id_for(pdf: &PdfEntry) -> String {
    format!("ISO26262-{}-{}", pdf.part, pdf.year)
}

fn render_ingest_command(args: &IngestArgs) -> String {
    let mut command = vec![
        "iso26262".to_string(),
        "ingest".to_string(),
        "--cache-root".to_string(),
        args.cache_root.display().to_string(),
    ];

    if let Some(path) = &args.inventory_manifest_path {
        command.push("--inventory-manifest-path".to_string());
        command.push(path.display().to_string());
    }
    if let Some(path) = &args.ingest_manifest_path {
        command.push("--ingest-manifest-path".to_string());
        command.push(path.display().to_string());
    }
    if let Some(path) = &args.db_path {
        command.push("--db-path".to_string());
        command.push(path.display().to_string());
    }
    if args.refresh_inventory {
        command.push("--refresh-inventory".to_string());
    }
    if args.seed_page_chunks {
        command.push("--seed-page-chunks".to_string());
    }
    for part in &args.target_parts {
        command.push("--target-part".to_string());
        command.push(part.to_string());
    }
    if let Some(max_pages) = args.max_pages_per_doc {
        command.push("--max-pages-per-doc".to_string());
        command.push(max_pages.to_string());
    }

    command.join(" ")
}
