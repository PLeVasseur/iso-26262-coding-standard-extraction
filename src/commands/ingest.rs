use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use regex::Regex;
use rusqlite::{Connection, params};
use serde::Serialize;
use tracing::{info, warn};

use crate::cli::{IngestArgs, OcrMode};
use crate::commands::inventory;
use crate::model::{
    IngestCounts, IngestPaths, IngestRunManifest, PdfEntry, PdfInventoryManifest, ToolVersions,
};
use crate::util::{ensure_directory, now_utc_string, utc_compact_string, write_json_pretty};

const DB_SCHEMA_VERSION: &str = "0.3.0";

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
    let page_provenance_path = manifest_dir.join(format!(
        "ingest_page_provenance_{}.json",
        utc_compact_string(started_ts)
    ));
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
        args.ocr_mode,
        &args.ocr_lang,
        args.ocr_min_text_chars,
    )?;

    sync_fts_index(&connection)?;

    let docs_total = count_rows(&connection, "SELECT COUNT(*) FROM docs")?;
    let chunks_total = count_rows(&connection, "SELECT COUNT(*) FROM chunks")?;
    let updated_at = now_utc_string();

    let page_provenance_manifest = PageProvenanceManifest {
        manifest_version: 1,
        run_id: run_id.clone(),
        generated_at: updated_at.clone(),
        entries: chunk_stats.page_provenance.clone(),
    };
    write_json_pretty(&page_provenance_path, &page_provenance_manifest)?;

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
            page_provenance_path: page_provenance_path.display().to_string(),
        },
        processed_parts: chunk_stats.processed_parts.clone(),
        counts: IngestCounts {
            pdf_count: inventory.pdf_count,
            processed_pdf_count: chunk_stats.processed_pdf_count,
            text_layer_page_count: chunk_stats.text_layer_page_count,
            ocr_fallback_page_count: chunk_stats.ocr_fallback_page_count,
            empty_page_count: chunk_stats.empty_page_count,
            header_lines_removed: chunk_stats.header_lines_removed,
            footer_lines_removed: chunk_stats.footer_lines_removed,
            dehyphenation_merges: chunk_stats.dehyphenation_merges,
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
            note_nodes_inserted: chunk_stats.note_nodes_inserted,
            note_item_nodes_inserted: chunk_stats.note_item_nodes_inserted,
            paragraph_nodes_inserted: chunk_stats.paragraph_nodes_inserted,
            requirement_atom_nodes_inserted: chunk_stats.requirement_atom_nodes_inserted,
            table_raw_fallback_count: chunk_stats.table_raw_fallback_count,
            list_parse_fallback_count: chunk_stats.list_parse_fallback_count,
            table_sparse_rows_count: chunk_stats.table_sparse_rows_count,
            table_overloaded_rows_count: chunk_stats.table_overloaded_rows_count,
            table_rows_with_markers_count: chunk_stats.table_rows_with_markers_count,
            table_rows_with_descriptions_count: chunk_stats.table_rows_with_descriptions_count,
            table_marker_expected_count: chunk_stats.table_marker_expected_count,
            table_marker_observed_count: chunk_stats.table_marker_observed_count,
            ocr_page_count: chunk_stats.ocr_page_count,
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
          anchor_type TEXT,
          anchor_label_raw TEXT,
          anchor_label_norm TEXT,
          anchor_order INTEGER,
          citation_anchor_id TEXT,
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
          anchor_type TEXT,
          anchor_label_raw TEXT,
          anchor_label_norm TEXT,
          anchor_order INTEGER,
          citation_anchor_id TEXT,
          FOREIGN KEY(doc_id) REFERENCES docs(doc_id)
        );
        ",
    )?;

    ensure_column_exists(connection, "nodes", "ancestor_path TEXT")?;
    ensure_column_exists(connection, "nodes", "anchor_type TEXT")?;
    ensure_column_exists(connection, "nodes", "anchor_label_raw TEXT")?;
    ensure_column_exists(connection, "nodes", "anchor_label_norm TEXT")?;
    ensure_column_exists(connection, "nodes", "anchor_order INTEGER")?;
    ensure_column_exists(connection, "nodes", "citation_anchor_id TEXT")?;
    ensure_column_exists(connection, "chunks", "origin_node_id TEXT")?;
    ensure_column_exists(connection, "chunks", "leaf_node_type TEXT")?;
    ensure_column_exists(connection, "chunks", "ancestor_path TEXT")?;
    ensure_column_exists(connection, "chunks", "anchor_type TEXT")?;
    ensure_column_exists(connection, "chunks", "anchor_label_raw TEXT")?;
    ensure_column_exists(connection, "chunks", "anchor_label_norm TEXT")?;
    ensure_column_exists(connection, "chunks", "anchor_order INTEGER")?;
    ensure_column_exists(connection, "chunks", "citation_anchor_id TEXT")?;

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
        CREATE INDEX IF NOT EXISTS idx_nodes_doc_parent_order ON nodes(doc_id, parent_node_id, order_index);
        CREATE INDEX IF NOT EXISTS idx_nodes_doc_citation_anchor ON nodes(doc_id, citation_anchor_id);
        CREATE INDEX IF NOT EXISTS idx_chunks_origin_node ON chunks(origin_node_id);
        CREATE INDEX IF NOT EXISTS idx_chunks_doc_citation_anchor ON chunks(doc_id, citation_anchor_id);
        CREATE INDEX IF NOT EXISTS idx_chunks_doc_ref_anchor_label ON chunks(doc_id, ref, anchor_label_norm);
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
    processed_parts: Vec<u32>,
    ocr_page_count: usize,
    text_layer_page_count: usize,
    ocr_fallback_page_count: usize,
    empty_page_count: usize,
    header_lines_removed: usize,
    footer_lines_removed: usize,
    dehyphenation_merges: usize,
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
    note_nodes_inserted: usize,
    note_item_nodes_inserted: usize,
    paragraph_nodes_inserted: usize,
    requirement_atom_nodes_inserted: usize,
    table_raw_fallback_count: usize,
    list_parse_fallback_count: usize,
    table_sparse_rows_count: usize,
    table_overloaded_rows_count: usize,
    table_rows_with_markers_count: usize,
    table_rows_with_descriptions_count: usize,
    table_marker_expected_count: usize,
    table_marker_observed_count: usize,
    page_provenance: Vec<PageExtractionProvenance>,
    warnings: Vec<String>,
}

#[derive(Debug, Default)]
struct ExtractedPages {
    pages: Vec<String>,
    page_printed_labels: Vec<Option<String>>,
    ocr_page_count: usize,
    text_layer_page_count: usize,
    ocr_fallback_page_count: usize,
    empty_page_count: usize,
    header_lines_removed: usize,
    footer_lines_removed: usize,
    dehyphenation_merges: usize,
    page_provenance: Vec<PageExtractionProvenance>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct PageExtractionProvenance {
    doc_id: String,
    page_pdf: i64,
    backend: String,
    reason: String,
    text_char_count: usize,
    ocr_char_count: Option<usize>,
    printed_page_label: Option<String>,
    printed_page_status: String,
}

#[derive(Debug, Serialize)]
struct PageProvenanceManifest {
    manifest_version: u32,
    run_id: String,
    generated_at: String,
    entries: Vec<PageExtractionProvenance>,
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
    SectionHeading,
    Clause,
    Subclause,
    Annex,
    Paragraph,
    Table,
    TableRow,
    TableCell,
    List,
    ListItem,
    Note,
    NoteItem,
    RequirementAtom,
    Page,
}

impl NodeType {
    fn as_str(self) -> &'static str {
        match self {
            NodeType::Document => "document",
            NodeType::SectionHeading => "section_heading",
            NodeType::Clause => "clause",
            NodeType::Subclause => "subclause",
            NodeType::Annex => "annex",
            NodeType::Paragraph => "paragraph",
            NodeType::Table => "table",
            NodeType::TableRow => "table_row",
            NodeType::TableCell => "table_cell",
            NodeType::List => "list",
            NodeType::ListItem => "list_item",
            NodeType::Note => "note",
            NodeType::NoteItem => "note_item",
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
    quality: TableQualityCounters,
}

#[derive(Debug, Default)]
struct TableQualityCounters {
    sparse_rows_count: usize,
    overloaded_rows_count: usize,
    rows_with_markers_count: usize,
    rows_with_descriptions_count: usize,
    marker_expected_count: usize,
    marker_observed_count: usize,
}

#[derive(Debug)]
struct ListItemDraft {
    marker: String,
    marker_norm: String,
    text: String,
    depth: i64,
}

#[derive(Debug)]
struct NoteItemDraft {
    marker: String,
    marker_norm: String,
    text: String,
}

#[derive(Debug, Clone)]
struct SectionHeadingDraft {
    reference: String,
    heading: String,
    page_pdf: i64,
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
    ocr_mode: OcrMode,
    ocr_lang: &str,
    ocr_min_text_chars: usize,
) -> Result<ChunkInsertStats> {
    let target_set: HashSet<u32> = target_parts.iter().copied().collect();
    let tx = connection.transaction()?;
    let mut stats = ChunkInsertStats::default();
    let list_item_regex = Regex::new(
        r"^(?P<marker>(?:(?:\d+[A-Za-z]?|[A-Za-z])(?:[\.)])?|[-*•—–]))(?:\s+(?P<body>.+))?$",
    )
    .context("failed to compile list item regex")?;
    let note_item_regex = Regex::new(r"^(?i)(?P<marker>NOTE(?:\s+\d+)?)(?:\s+(?P<body>.+))?$")
        .context("failed to compile note item regex")?;
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
              page_pdf_start, page_pdf_end, page_printed_start, page_printed_end,
              text, table_md, table_csv, source_hash,
              origin_node_id, leaf_node_type, ancestor_path,
              anchor_type, anchor_label_raw, anchor_label_norm, anchor_order, citation_anchor_id
            )
            VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23)
            ON CONFLICT(chunk_id) DO UPDATE SET
              doc_id=excluded.doc_id,
              type=excluded.type,
              ref=excluded.ref,
              ref_path=excluded.ref_path,
              heading=excluded.heading,
              chunk_seq=excluded.chunk_seq,
              page_pdf_start=excluded.page_pdf_start,
              page_pdf_end=excluded.page_pdf_end,
              page_printed_start=excluded.page_printed_start,
              page_printed_end=excluded.page_printed_end,
              text=excluded.text,
              table_md=excluded.table_md,
              table_csv=excluded.table_csv,
              source_hash=excluded.source_hash,
              origin_node_id=excluded.origin_node_id,
              leaf_node_type=excluded.leaf_node_type,
              ancestor_path=excluded.ancestor_path,
              anchor_type=excluded.anchor_type,
              anchor_label_raw=excluded.anchor_label_raw,
              anchor_label_norm=excluded.anchor_label_norm,
              anchor_order=excluded.anchor_order,
              citation_anchor_id=excluded.citation_anchor_id
            ",
        )?;

        let mut node_statement = tx.prepare(
            "
            INSERT INTO nodes(
              node_id, parent_node_id, doc_id, node_type, ref, ref_path, heading,
              order_index, page_pdf_start, page_pdf_end, text, source_hash, ancestor_path,
              anchor_type, anchor_label_raw, anchor_label_norm, anchor_order, citation_anchor_id
            )
            VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
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
              ancestor_path=excluded.ancestor_path,
              anchor_type=excluded.anchor_type,
              anchor_label_raw=excluded.anchor_label_raw,
              anchor_label_norm=excluded.anchor_label_norm,
              anchor_order=excluded.anchor_order,
              citation_anchor_id=excluded.citation_anchor_id
            ",
        )?;

        for pdf in pdfs {
            if !target_set.is_empty() && !target_set.contains(&pdf.part) {
                continue;
            }

            stats.processed_pdf_count += 1;
            if !stats.processed_parts.contains(&pdf.part) {
                stats.processed_parts.push(pdf.part);
                stats.processed_parts.sort_unstable();
            }

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

            let page_extraction = match extract_pages_with_backend(
                &pdf_path,
                &doc_id,
                max_pages_per_doc,
                ocr_mode,
                ocr_lang,
                ocr_min_text_chars,
            ) {
                Ok(extraction) => extraction,
                Err(err) => {
                    if matches!(ocr_mode, OcrMode::Force) {
                        return Err(err).with_context(|| {
                            format!("failed to extract text for {}", pdf_path.display())
                        });
                    }

                    let warning =
                        format!("failed to extract text for {}: {err}", pdf_path.display());
                    warn!(warning = %warning, "pdf extraction warning");
                    stats.warnings.push(warning);
                    continue;
                }
            };
            let page_printed_labels = page_extraction.page_printed_labels.clone();
            let pages = page_extraction.pages;
            stats.ocr_page_count += page_extraction.ocr_page_count;
            stats.text_layer_page_count += page_extraction.text_layer_page_count;
            stats.ocr_fallback_page_count += page_extraction.ocr_fallback_page_count;
            stats.empty_page_count += page_extraction.empty_page_count;
            stats.header_lines_removed += page_extraction.header_lines_removed;
            stats.footer_lines_removed += page_extraction.footer_lines_removed;
            stats.dehyphenation_merges += page_extraction.dehyphenation_merges;
            stats
                .page_provenance
                .extend(page_extraction.page_provenance);
            stats.warnings.extend(page_extraction.warnings);

            let section_headings = match extract_section_headings_with_pdftohtml(&pdf_path) {
                Ok(headings) => headings,
                Err(err) => {
                    let warning = format!(
                        "failed to extract outline headings for {}: {err}",
                        pdf_path.display()
                    );
                    warn!(warning = %warning, "pdf outline extraction warning");
                    stats.warnings.push(warning);
                    Vec::new()
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
                None,
                None,
                None,
                None,
                None,
            )?;

            stats.nodes_total += 1;

            let mut node_paths = HashMap::<String, String>::new();
            node_paths.insert(document_node_id.clone(), document_path);

            let mut node_key_counts = HashMap::<String, i64>::new();
            let mut chunk_key_counts = HashMap::<String, i64>::new();
            let mut section_ref_to_node_id = HashMap::<String, String>::new();
            let mut clause_ref_to_node_id = HashMap::<String, String>::new();
            let mut last_clause_node_id: Option<String> = None;
            let mut node_order_index: i64 = 1;

            for section in section_headings {
                let section_node_id = format!(
                    "{}:node:section_heading:{}",
                    doc_id,
                    sanitize_ref_for_id(&section.reference)
                );

                let section_path = build_ancestor_path(
                    Some(&document_node_id),
                    &node_paths,
                    NodeType::SectionHeading,
                    &section.reference,
                    &section.heading,
                );
                let section_anchor_order = section.reference.parse::<i64>().ok();
                let section_anchor_id = build_citation_anchor_id(
                    &doc_id,
                    &section.reference,
                    "clause",
                    Some(&section.reference),
                    section_anchor_order,
                );

                insert_node(
                    &mut node_statement,
                    &section_node_id,
                    Some(&document_node_id),
                    &doc_id,
                    NodeType::SectionHeading,
                    Some(&section.reference),
                    Some(&section.reference),
                    Some(&section.heading),
                    node_order_index,
                    Some(section.page_pdf),
                    Some(section.page_pdf),
                    Some(&section.heading),
                    &pdf.sha256,
                    &section_path,
                    Some("clause"),
                    Some(&section.reference),
                    Some(&section.reference),
                    section_anchor_order,
                    Some(&section_anchor_id),
                )?;

                node_paths.insert(section_node_id.clone(), section_path);
                section_ref_to_node_id.insert(section.reference, section_node_id);
                node_order_index += 1;
                stats.nodes_total += 1;
                increment_node_type_stat(&mut stats, NodeType::SectionHeading);
            }

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
                            .or_else(|| {
                                find_section_node_id(&chunk.reference, &section_ref_to_node_id)
                            })
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
                let node_anchor_type = match origin_node_type {
                    NodeType::Clause | NodeType::Subclause | NodeType::Annex | NodeType::Table => {
                        Some("clause")
                    }
                    _ => None,
                };
                let node_anchor_order = node_anchor_type.map(|_| structured_seq);
                let node_anchor_id = node_anchor_type.map(|anchor_type| {
                    build_citation_anchor_id(
                        &doc_id,
                        &chunk.reference,
                        anchor_type,
                        Some(&chunk.reference),
                        node_anchor_order,
                    )
                });

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
                    node_anchor_type,
                    node_anchor_type.map(|_| chunk.reference.as_str()),
                    node_anchor_type.map(|_| chunk.reference.as_str()),
                    node_anchor_order,
                    node_anchor_id.as_deref(),
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

                if let Some(parsed) = parsed_table_rows.as_ref() {
                    stats.table_sparse_rows_count += parsed.quality.sparse_rows_count;
                    stats.table_overloaded_rows_count += parsed.quality.overloaded_rows_count;
                    stats.table_rows_with_markers_count += parsed.quality.rows_with_markers_count;
                    stats.table_rows_with_descriptions_count +=
                        parsed.quality.rows_with_descriptions_count;
                    stats.table_marker_expected_count += parsed.quality.marker_expected_count;
                    stats.table_marker_observed_count += parsed.quality.marker_observed_count;
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
                let chunk_anchor_type = Some("clause");
                let chunk_anchor_order = Some(structured_seq);
                let chunk_anchor_id = Some(build_citation_anchor_id(
                    &doc_id,
                    &chunk.reference,
                    "clause",
                    Some(&chunk.reference),
                    chunk_anchor_order,
                ));
                let chunk_page_printed_start =
                    printed_page_label_for(&page_printed_labels, chunk.page_start);
                let chunk_page_printed_end =
                    printed_page_label_for(&page_printed_labels, chunk.page_end);

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
                    &chunk_page_printed_start,
                    &chunk_page_printed_end,
                    &chunk.text,
                    &table_md,
                    &table_csv,
                    &pdf.sha256,
                    &origin_node_id,
                    origin_node_type.as_str(),
                    &ancestor_path,
                    chunk_anchor_type,
                    chunk_anchor_type.map(|_| chunk.reference.as_str()),
                    chunk_anchor_type.map(|_| chunk.reference.as_str()),
                    chunk_anchor_order,
                    chunk_anchor_id.as_deref()
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
                    let paragraphs = parse_paragraphs(
                        &chunk.text,
                        &chunk.heading,
                        &list_item_regex,
                        &note_item_regex,
                    );
                    if !paragraphs.is_empty() {
                        insert_paragraph_nodes(
                            &mut node_statement,
                            &doc_id,
                            &origin_node_id,
                            &ancestor_path,
                            &chunk.reference,
                            &paragraphs,
                            chunk.page_start,
                            chunk.page_end,
                            &pdf.sha256,
                            &mut node_order_index,
                            &mut stats,
                        )?;
                    }

                    let note_items = parse_note_items(
                        &chunk.text,
                        &chunk.heading,
                        &note_item_regex,
                        &list_item_regex,
                    );
                    if !note_items.is_empty() {
                        insert_note_nodes(
                            &mut node_statement,
                            &doc_id,
                            &origin_node_id,
                            &ancestor_path,
                            &chunk.reference,
                            &note_items,
                            chunk.page_start,
                            chunk.page_end,
                            &pdf.sha256,
                            &mut node_order_index,
                            &mut stats,
                        )?;
                    }

                    let (list_items, list_fallback) = parse_list_items(
                        &chunk.text,
                        &chunk.heading,
                        &list_item_regex,
                        &note_item_regex,
                    );
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
                    let page_printed_label =
                        printed_page_label_for(&page_printed_labels, page_number);
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
                        None,
                        None,
                        None,
                        None,
                        None,
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
                        &page_printed_label,
                        &page_printed_label,
                        text,
                        Option::<String>::None,
                        Option::<String>::None,
                        &pdf.sha256,
                        &page_node_id,
                        NodeType::Page.as_str(),
                        &page_ancestor_path,
                        Option::<String>::None,
                        Option::<String>::None,
                        Option::<String>::None,
                        Option::<i64>::None,
                        Option::<String>::None
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
        NodeType::SectionHeading => {}
        NodeType::Clause => stats.clause_nodes_inserted += 1,
        NodeType::Subclause => stats.subclause_nodes_inserted += 1,
        NodeType::Annex => stats.annex_nodes_inserted += 1,
        NodeType::Paragraph => stats.paragraph_nodes_inserted += 1,
        NodeType::Table => stats.table_nodes_inserted += 1,
        NodeType::TableRow => stats.table_row_nodes_inserted += 1,
        NodeType::TableCell => stats.table_cell_nodes_inserted += 1,
        NodeType::List => stats.list_nodes_inserted += 1,
        NodeType::ListItem => stats.list_item_nodes_inserted += 1,
        NodeType::Note => stats.note_nodes_inserted += 1,
        NodeType::NoteItem => stats.note_item_nodes_inserted += 1,
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

fn find_section_node_id(
    reference: &str,
    section_ref_to_node_id: &HashMap<String, String>,
) -> Option<String> {
    let section_ref = reference.split('.').next()?.trim();
    if section_ref.is_empty() {
        return None;
    }

    section_ref_to_node_id.get(section_ref).cloned()
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
    anchor_type: Option<&str>,
    anchor_label_raw: Option<&str>,
    anchor_label_norm: Option<&str>,
    anchor_order: Option<i64>,
    citation_anchor_id: Option<&str>,
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
        ancestor_path,
        anchor_type,
        anchor_label_raw,
        anchor_label_norm,
        anchor_order,
        citation_anchor_id
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
        let row_order = (row_idx + 1) as i64;
        let row_label = (row_idx + 1).to_string();
        let row_anchor_id = build_citation_anchor_id(
            doc_id,
            table_reference,
            "table_row",
            Some(&row_label),
            Some(row_order),
        );

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
            Some("table_row"),
            None,
            Some(&row_label),
            Some(row_order),
            Some(&row_anchor_id),
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
            let cell_order = ((row_idx * 1000) + col_idx + 1) as i64;
            let cell_label = format!("r{}c{}", row_idx + 1, col_idx + 1);
            let cell_anchor_id = build_citation_anchor_id(
                doc_id,
                table_reference,
                "table_cell",
                Some(&cell_label),
                Some(cell_order),
            );

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
                Some("table_cell"),
                None,
                Some(&cell_label),
                Some(cell_order),
                Some(&cell_anchor_id),
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

    for line in &body_lines {
        if line_is_noise(line) {
            continue;
        }

        let cells = split_table_cells(line, cell_split_regex);
        if !cells.is_empty() {
            rows.push(cells);
        }
    }

    let mut structured = rows.len() >= 2 && rows.iter().any(|cells| cells.len() > 1);
    let reconstructed = reconstruct_table_rows_from_markers(&body_lines);
    if !reconstructed.is_empty() {
        let original_quality = analyze_table_rows(&rows);
        let reconstructed_quality = analyze_table_rows(&reconstructed);

        if prefer_reconstructed_rows(
            rows.len(),
            &original_quality,
            reconstructed.len(),
            &reconstructed_quality,
        ) {
            rows = reconstructed;
            structured = rows.len() >= 2 && rows.iter().any(|cells| cells.len() > 1);
        } else if !structured
            && reconstructed.len() >= 2
            && reconstructed.iter().any(|cells| cells.len() > 1)
        {
            rows = reconstructed;
            structured = true;
        }
    }

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
    let quality = analyze_table_rows(&rows);

    ParsedTableRows {
        rows,
        markdown,
        csv,
        used_fallback: !structured,
        quality,
    }
}

fn analyze_table_rows(rows: &[Vec<String>]) -> TableQualityCounters {
    let mut counters = TableQualityCounters::default();
    let mut observed_markers = HashSet::<(i64, Option<char>)>::new();

    for row in rows {
        let first_cell = row.first().map(|value| value.as_str()).unwrap_or_default();
        let row_marker = parse_table_marker_token(first_cell);
        let row_marker_count = count_row_marker_tokens(row);

        if let Some(marker) = row_marker {
            counters.rows_with_markers_count += 1;
            observed_markers.insert(marker);

            if has_row_description(row) {
                counters.rows_with_descriptions_count += 1;
            } else {
                counters.sparse_rows_count += 1;
            }
        }

        if row_marker_count > 1 {
            counters.overloaded_rows_count += 1;
        }
    }

    counters.marker_observed_count = observed_markers.len();
    counters.marker_expected_count = estimate_expected_marker_count(&observed_markers);
    counters
}

fn parse_table_marker_token(value: &str) -> Option<(i64, Option<char>)> {
    let marker = normalize_marker_label(value);
    parse_numeric_alpha_marker(&marker)
}

fn count_row_marker_tokens(row: &[String]) -> usize {
    let mut marker_tokens = HashSet::<(i64, Option<char>)>::new();

    for cell in row {
        for token in cell.split_whitespace() {
            let trimmed = token.trim_matches(['(', ')', '.', ':', ';', ',']);
            if let Some(marker) = parse_table_marker_token(trimmed) {
                marker_tokens.insert(marker);
            }
        }
    }

    marker_tokens.len()
}

fn has_row_description(row: &[String]) -> bool {
    if row.len() < 2 {
        return false;
    }

    let description = row[1].trim();
    !description.is_empty() && description.chars().any(|value| value.is_ascii_alphabetic())
}

fn estimate_expected_marker_count(observed_markers: &HashSet<(i64, Option<char>)>) -> usize {
    let mut grouped = HashMap::<i64, Vec<Option<char>>>::new();

    for (number, suffix) in observed_markers {
        grouped.entry(*number).or_default().push(*suffix);
    }

    let mut expected = 0usize;
    for suffixes in grouped.values() {
        let with_suffix = suffixes
            .iter()
            .filter_map(|suffix| *suffix)
            .collect::<Vec<char>>();

        if with_suffix.is_empty() {
            expected += suffixes.len().max(1);
            continue;
        }

        let min_index = with_suffix
            .iter()
            .map(|suffix| (*suffix as u8).saturating_sub(b'a') as usize)
            .min()
            .unwrap_or(0);
        let max_index = with_suffix
            .iter()
            .map(|suffix| (*suffix as u8).saturating_sub(b'a') as usize)
            .max()
            .unwrap_or(min_index);

        expected += (max_index.saturating_sub(min_index) + 1).max(with_suffix.len());
    }

    expected
}

fn reconstruct_table_rows_from_markers(lines: &[&str]) -> Vec<Vec<String>> {
    let marker_with_body_regex = Regex::new(r"^(?P<marker>\d+[A-Za-z]?)[\.)]?\s+(?P<body>.+)$")
        .expect("valid marker with body regex");
    let marker_only_regex =
        Regex::new(r"^(?P<marker>\d+[A-Za-z]?)[\.)]?$").expect("valid marker only regex");
    let marker_list_regex = Regex::new(r"^(?P<list>(?:\d+[A-Za-z]?\s+){1,}\d+[A-Za-z]?)$")
        .expect("valid marker list regex");
    let plus_regex = Regex::new(r"^\+{1,2}$").expect("valid plus regex");

    let mut rows = Vec::<Vec<String>>::new();
    let mut current_row: Option<Vec<String>> = None;
    let mut pending_markers = Vec::<String>::new();

    let flush_current = |rows: &mut Vec<Vec<String>>, current_row: &mut Option<Vec<String>>| {
        if let Some(row) = current_row.take() {
            if row.len() > 1 {
                rows.push(row);
            }
        }
    };

    for raw_line in lines {
        let line = raw_line.trim();
        if line.is_empty() || line_is_noise(line) {
            continue;
        }

        if let Some(captures) = marker_list_regex.captures(line) {
            flush_current(&mut rows, &mut current_row);

            let marker_list = captures
                .name("list")
                .map(|value| value.as_str())
                .unwrap_or("");
            pending_markers = marker_list
                .split_whitespace()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect();
            continue;
        }

        if let Some(captures) = marker_with_body_regex.captures(line) {
            flush_current(&mut rows, &mut current_row);

            let marker = captures
                .name("marker")
                .map(|value| value.as_str().trim().to_string())
                .unwrap_or_default();
            let body = captures
                .name("body")
                .map(|value| value.as_str().trim().to_string())
                .unwrap_or_default();

            let mut row = vec![marker];
            row.push(body);
            current_row = Some(row);
            continue;
        }

        if let Some(captures) = marker_only_regex.captures(line) {
            flush_current(&mut rows, &mut current_row);
            let marker = captures
                .name("marker")
                .map(|value| value.as_str().trim().to_string())
                .unwrap_or_default();
            if !marker.is_empty() {
                pending_markers.push(marker);
            }
            continue;
        }

        if plus_regex.is_match(line) {
            if let Some(row) = current_row.as_mut() {
                row.push(line.to_string());
            }
            continue;
        }

        if !pending_markers.is_empty() {
            flush_current(&mut rows, &mut current_row);
            let marker = pending_markers.remove(0);
            current_row = Some(vec![marker, line.to_string()]);
            continue;
        }

        let Some(row) = current_row.as_mut() else {
            continue;
        };

        if row.len() <= 1 {
            row.push(line.to_string());
            continue;
        }

        let description = row.get_mut(1).expect("description slot exists");
        if !description.is_empty() {
            description.push(' ');
        }
        description.push_str(line);
    }

    if let Some(row) = current_row.take() {
        if row.len() > 1 {
            rows.push(row);
        }
    }

    for marker in pending_markers {
        rows.push(vec![marker, String::new()]);
    }

    rows
}

fn prefer_reconstructed_rows(
    original_rows_count: usize,
    original_quality: &TableQualityCounters,
    reconstructed_rows_count: usize,
    reconstructed_quality: &TableQualityCounters,
) -> bool {
    if reconstructed_rows_count < 2 {
        return false;
    }

    if reconstructed_quality.rows_with_markers_count == 0 {
        return false;
    }

    let original_sparse_ratio = if original_rows_count == 0 {
        1.0
    } else {
        original_quality.sparse_rows_count as f64 / original_rows_count as f64
    };
    let reconstructed_sparse_ratio =
        reconstructed_quality.sparse_rows_count as f64 / reconstructed_rows_count as f64;

    let original_description_coverage = ratio_usize(
        original_quality.rows_with_descriptions_count,
        original_quality.rows_with_markers_count,
    )
    .unwrap_or(0.0);
    let reconstructed_description_coverage = ratio_usize(
        reconstructed_quality.rows_with_descriptions_count,
        reconstructed_quality.rows_with_markers_count,
    )
    .unwrap_or(0.0);

    (reconstructed_sparse_ratio + 0.05) < original_sparse_ratio
        || (reconstructed_description_coverage > original_description_coverage + 0.10)
        || (reconstructed_quality.sparse_rows_count < original_quality.sparse_rows_count
            && reconstructed_quality.rows_with_descriptions_count
                >= original_quality.rows_with_descriptions_count)
}

fn ratio_usize(numerator: usize, denominator: usize) -> Option<f64> {
    if denominator == 0 {
        None
    } else {
        Some(numerator as f64 / denominator as f64)
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

fn extract_body_lines_preserve_blanks<'a>(text: &'a str, heading: &str) -> Vec<&'a str> {
    let mut lines = text.lines().collect::<Vec<&str>>();
    if let Some(first) = lines.first() {
        if first.trim() == heading.trim() {
            lines.remove(0);
        }
    }

    lines
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

fn parse_paragraphs(
    text: &str,
    heading: &str,
    list_item_regex: &Regex,
    note_item_regex: &Regex,
) -> Vec<String> {
    let body_lines = extract_body_lines_preserve_blanks(text, heading);
    let mut paragraphs = Vec::<String>::new();
    let mut current = String::new();

    for raw_line in body_lines {
        if line_is_noise(raw_line) {
            continue;
        }

        let line = raw_line.trim();
        if line.is_empty() {
            if !current.is_empty() {
                paragraphs.push(current.trim().to_string());
                current.clear();
            }
            continue;
        }

        if current.is_empty() {
            current.push_str(line);
            continue;
        }

        let starts_new_marker = list_item_regex.is_match(line) || note_item_regex.is_match(line);
        let previous_ends_sentence = current.ends_with('.') || current.ends_with(';');
        let starts_with_lowercase = line
            .chars()
            .next()
            .map(|value| value.is_lowercase())
            .unwrap_or(false);

        if starts_new_marker || (previous_ends_sentence && !starts_with_lowercase) {
            paragraphs.push(current.trim().to_string());
            current.clear();
            current.push_str(line);
            continue;
        }

        current.push(' ');
        current.push_str(line);
    }

    if !current.is_empty() {
        paragraphs.push(current.trim().to_string());
    }

    paragraphs
}

#[allow(clippy::too_many_arguments)]
fn insert_paragraph_nodes(
    node_statement: &mut rusqlite::Statement<'_>,
    doc_id: &str,
    parent_node_id: &str,
    parent_path: &str,
    reference: &str,
    paragraphs: &[String],
    page_start: i64,
    page_end: i64,
    source_hash: &str,
    node_order_index: &mut i64,
    stats: &mut ChunkInsertStats,
) -> Result<()> {
    for (index, paragraph) in paragraphs.iter().enumerate() {
        let paragraph_node_id = format!("{}:paragraph:{:03}", parent_node_id, index + 1);
        let paragraph_ref = format!("{} para {}", reference, index + 1);
        let paragraph_heading = format!("{} paragraph {}", reference, index + 1);
        let paragraph_path = format!("{} > paragraph:{}", parent_path, index + 1);
        let paragraph_order = (index + 1) as i64;
        let paragraph_label = paragraph_order.to_string();
        let paragraph_anchor_id = build_citation_anchor_id(
            doc_id,
            reference,
            "paragraph",
            Some(&paragraph_label),
            Some(paragraph_order),
        );

        insert_node(
            node_statement,
            &paragraph_node_id,
            Some(parent_node_id),
            doc_id,
            NodeType::Paragraph,
            Some(&paragraph_ref),
            Some(&paragraph_ref),
            Some(&paragraph_heading),
            *node_order_index,
            Some(page_start),
            Some(page_end),
            Some(paragraph),
            source_hash,
            &paragraph_path,
            Some("paragraph"),
            None,
            Some(&paragraph_label),
            Some(paragraph_order),
            Some(&paragraph_anchor_id),
        )?;

        *node_order_index += 1;
        stats.nodes_total += 1;
        increment_node_type_stat(stats, NodeType::Paragraph);
    }

    Ok(())
}

fn parse_list_items(
    text: &str,
    heading: &str,
    list_item_regex: &Regex,
    note_item_regex: &Regex,
) -> (Vec<ListItemDraft>, bool) {
    let body_lines = extract_body_lines(text, heading);
    let mut items = Vec::<ListItemDraft>::new();
    let mut list_like_lines = 0usize;
    let mut active_item: Option<ListItemDraft> = None;

    for raw_line in body_lines {
        if line_is_noise(raw_line) {
            continue;
        }

        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        let list_capture = list_item_regex.captures(line);
        let note_capture = note_item_regex.captures(line);

        if list_capture.is_some() || note_capture.is_some() {
            list_like_lines += 1;
        }

        if let Some(captures) = list_capture {
            if let Some(item) = active_item.take() {
                if !item.text.trim().is_empty() {
                    items.push(item);
                }
            }

            let marker = captures
                .name("marker")
                .map(|value| value.as_str().to_string())
                .unwrap_or_else(|| "-".to_string());
            let marker_norm = normalize_marker_label(&marker);
            let body = captures
                .name("body")
                .map(|value| value.as_str().trim().to_string())
                .unwrap_or_default();

            active_item = Some(ListItemDraft {
                marker,
                marker_norm,
                text: body,
                depth: 1,
            });
            continue;
        }

        if note_capture.is_some() {
            if let Some(item) = active_item.take() {
                if !item.text.trim().is_empty() {
                    items.push(item);
                }
            }
            continue;
        }

        if let Some(item) = active_item.as_mut() {
            if !item.text.is_empty() {
                item.text.push(' ');
            }
            item.text.push_str(line);
        }
    }

    if let Some(item) = active_item.take() {
        if !item.text.trim().is_empty() {
            items.push(item);
        }
    }

    reorder_list_items_for_marker_sequence(&mut items);

    let used_fallback = list_like_lines > 0 && items.is_empty();
    (items, used_fallback)
}

fn parse_note_items(
    text: &str,
    heading: &str,
    note_item_regex: &Regex,
    list_item_regex: &Regex,
) -> Vec<NoteItemDraft> {
    let body_lines = extract_body_lines(text, heading);
    let mut items = Vec::<NoteItemDraft>::new();
    let mut active_item: Option<NoteItemDraft> = None;

    for raw_line in body_lines {
        if line_is_noise(raw_line) {
            continue;
        }

        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        if let Some(captures) = note_item_regex.captures(line) {
            if let Some(item) = active_item.take() {
                if !item.text.trim().is_empty() {
                    items.push(item);
                }
            }

            let marker = captures
                .name("marker")
                .map(|value| value.as_str().to_string())
                .unwrap_or_else(|| "NOTE".to_string());
            let marker_norm = normalize_marker_label(&marker);
            let body = captures
                .name("body")
                .map(|value| value.as_str().trim().to_string())
                .unwrap_or_default();

            active_item = Some(NoteItemDraft {
                marker,
                marker_norm,
                text: body,
            });
            continue;
        }

        if list_item_regex.is_match(line) {
            if let Some(item) = active_item.take() {
                if !item.text.trim().is_empty() {
                    items.push(item);
                }
            }
            continue;
        }

        if let Some(item) = active_item.as_mut() {
            if !item.text.is_empty() {
                item.text.push(' ');
            }
            item.text.push_str(line);
        }
    }

    if let Some(item) = active_item.take() {
        if !item.text.trim().is_empty() {
            items.push(item);
        }
    }

    items
}

fn reorder_list_items_for_marker_sequence(items: &mut Vec<ListItemDraft>) {
    if items.len() < 3 {
        return;
    }

    if items.iter().all(|item| {
        item.marker_norm.len() == 1 && item.marker_norm.chars().all(|ch| ch.is_ascii_lowercase())
    }) {
        items.sort_by(|left, right| left.marker_norm.cmp(&right.marker_norm));
        return;
    }

    if items
        .iter()
        .all(|item| parse_numeric_alpha_marker(&item.marker_norm).is_some())
    {
        items.sort_by(|left, right| {
            let (left_num, left_suffix) =
                parse_numeric_alpha_marker(&left.marker_norm).unwrap_or((i64::MAX, None));
            let (right_num, right_suffix) =
                parse_numeric_alpha_marker(&right.marker_norm).unwrap_or((i64::MAX, None));

            left_num
                .cmp(&right_num)
                .then(left_suffix.unwrap_or('~').cmp(&right_suffix.unwrap_or('~')))
        });
    }
}

fn parse_numeric_alpha_marker(value: &str) -> Option<(i64, Option<char>)> {
    let mut digits = String::new();
    let mut suffix: Option<char> = None;

    for ch in value.chars() {
        if ch.is_ascii_digit() {
            if suffix.is_some() {
                return None;
            }
            digits.push(ch);
            continue;
        }

        if ch.is_ascii_lowercase() {
            if suffix.is_some() {
                return None;
            }
            suffix = Some(ch);
            continue;
        }

        return None;
    }

    if digits.is_empty() {
        return None;
    }

    let number = digits.parse::<i64>().ok()?;
    Some((number, suffix))
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
        None,
        None,
        None,
        None,
        None,
    )?;

    *node_order_index += 1;
    stats.nodes_total += 1;
    increment_node_type_stat(stats, NodeType::List);

    for (item_idx, item) in list_items.iter().enumerate() {
        let list_item_node_id = format!("{}:item:{:03}", list_node_id, item_idx + 1);
        let list_item_ref = format!("{} item {}", reference, item_idx + 1);
        let list_item_heading = format!("{} {}", item.marker, item.text);
        let list_item_path = format!("{} > list_item:{}", list_path, item_idx + 1);
        let marker_order = (item_idx + 1) as i64;
        let marker_anchor_id = build_citation_anchor_id(
            doc_id,
            reference,
            "marker",
            Some(&item.marker_norm),
            Some(marker_order),
        );

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
            Some("marker"),
            Some(&item.marker),
            Some(&item.marker_norm),
            Some(marker_order),
            Some(&marker_anchor_id),
        )?;

        *node_order_index += 1;
        stats.nodes_total += 1;
        increment_node_type_stat(stats, NodeType::ListItem);
        let _ = item.depth;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_note_nodes(
    node_statement: &mut rusqlite::Statement<'_>,
    doc_id: &str,
    parent_node_id: &str,
    parent_path: &str,
    reference: &str,
    note_items: &[NoteItemDraft],
    page_start: i64,
    page_end: i64,
    source_hash: &str,
    node_order_index: &mut i64,
    stats: &mut ChunkInsertStats,
) -> Result<()> {
    let note_node_id = format!("{}:note:001", parent_node_id);
    let note_ref = format!("{} note", reference);
    let note_heading = format!("{} note", reference);
    let note_path = format!("{} > note:{}", parent_path, reference);

    insert_node(
        node_statement,
        &note_node_id,
        Some(parent_node_id),
        doc_id,
        NodeType::Note,
        Some(&note_ref),
        Some(&note_ref),
        Some(&note_heading),
        *node_order_index,
        Some(page_start),
        Some(page_end),
        None,
        source_hash,
        &note_path,
        None,
        None,
        None,
        None,
        None,
    )?;

    *node_order_index += 1;
    stats.nodes_total += 1;
    increment_node_type_stat(stats, NodeType::Note);

    for (item_idx, item) in note_items.iter().enumerate() {
        let note_item_node_id = format!("{}:item:{:03}", note_node_id, item_idx + 1);
        let note_item_ref = format!("{} note {}", reference, item_idx + 1);
        let note_item_heading = format!("{} {}", item.marker, item.text);
        let note_item_path = format!("{} > note_item:{}", note_path, item_idx + 1);
        let marker_order = (item_idx + 1) as i64;
        let marker_anchor_id = build_citation_anchor_id(
            doc_id,
            reference,
            "marker",
            Some(&item.marker_norm),
            Some(marker_order),
        );

        insert_node(
            node_statement,
            &note_item_node_id,
            Some(&note_node_id),
            doc_id,
            NodeType::NoteItem,
            Some(&note_item_ref),
            Some(&note_item_ref),
            Some(&note_item_heading),
            *node_order_index,
            Some(page_start),
            Some(page_end),
            Some(&item.text),
            source_hash,
            &note_item_path,
            Some("marker"),
            Some(&item.marker),
            Some(&item.marker_norm),
            Some(marker_order),
            Some(&marker_anchor_id),
        )?;

        *node_order_index += 1;
        stats.nodes_total += 1;
        increment_node_type_stat(stats, NodeType::NoteItem);
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
            None,
            None,
            None,
            None,
            None,
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

fn normalize_marker_label(marker: &str) -> String {
    let trimmed = marker.trim();
    if trimmed.is_empty() {
        return "-".to_string();
    }

    let without_suffix = trimmed.trim_end_matches([')', '.', ':', ';']);
    let canonical_bullet = without_suffix.replace('–', "-").replace('—', "-");
    if canonical_bullet == "-" || canonical_bullet == "*" || canonical_bullet == "•" {
        return "-".to_string();
    }

    let upper = canonical_bullet.to_ascii_uppercase();
    if upper == "NOTE" {
        return "NOTE".to_string();
    }

    if let Some(rest) = upper.strip_prefix("NOTE ") {
        let normalized_rest = rest.trim();
        if !normalized_rest.is_empty() && normalized_rest.chars().all(|ch| ch.is_ascii_digit()) {
            return format!("NOTE {}", normalized_rest);
        }
    }

    canonical_bullet.to_ascii_lowercase()
}

fn build_citation_anchor_id(
    doc_id: &str,
    parent_ref: &str,
    anchor_type: &str,
    anchor_label_norm: Option<&str>,
    anchor_order: Option<i64>,
) -> String {
    let parent_key = sanitize_ref_for_id(parent_ref);
    let label_key = anchor_label_norm
        .map(sanitize_ref_for_id)
        .filter(|value| !value.is_empty())
        .or_else(|| anchor_order.map(|value| value.to_string()))
        .unwrap_or_else(|| "root".to_string());

    format!(
        "{}:{}:{}:{}",
        doc_id,
        parent_key,
        sanitize_ref_for_id(anchor_type),
        label_key
    )
}

fn extract_section_headings_with_pdftohtml(pdf_path: &Path) -> Result<Vec<SectionHeadingDraft>> {
    let output = Command::new("pdftohtml")
        .arg("-xml")
        .arg("-f")
        .arg("1")
        .arg("-l")
        .arg("1")
        .arg(pdf_path)
        .arg("-stdout")
        .output()
        .with_context(|| format!("failed to execute pdftohtml for {}", pdf_path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "pdftohtml returned non-zero exit status for {}: {}",
            pdf_path.display(),
            stderr.trim()
        );
    }

    let xml = String::from_utf8_lossy(&output.stdout);
    let item_regex = Regex::new(r#"<item page="(\d+)">(.*?)</item>"#)
        .context("failed to compile outline item regex")?;
    let section_heading_regex =
        Regex::new(r"^\s*(\d+)\s+(.+)$").context("failed to compile section heading regex")?;

    let mut section_headings = Vec::<SectionHeadingDraft>::new();
    let mut seen_refs = HashSet::<String>::new();

    for captures in item_regex.captures_iter(&xml) {
        let page_pdf = captures
            .get(1)
            .and_then(|value| value.as_str().parse::<i64>().ok())
            .unwrap_or(1);

        let raw_label = captures.get(2).map(|value| value.as_str()).unwrap_or("");
        let normalized_label = normalize_outline_label(raw_label);

        let Some(section_captures) = section_heading_regex.captures(&normalized_label) else {
            continue;
        };

        let reference = section_captures
            .get(1)
            .map(|value| value.as_str().trim())
            .unwrap_or_default();
        let title = section_captures
            .get(2)
            .map(|value| value.as_str().trim())
            .unwrap_or_default();

        if reference.is_empty() || title.is_empty() {
            continue;
        }
        if !seen_refs.insert(reference.to_string()) {
            continue;
        }

        section_headings.push(SectionHeadingDraft {
            reference: reference.to_string(),
            heading: normalized_label,
            page_pdf,
        });
    }

    Ok(section_headings)
}

fn normalize_outline_label(raw_label: &str) -> String {
    raw_label
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace('\u{00a0}', " ")
        .split_whitespace()
        .collect::<Vec<&str>>()
        .join(" ")
}

fn extract_pages_with_backend(
    pdf_path: &Path,
    doc_id: &str,
    max_pages_per_doc: Option<usize>,
    ocr_mode: OcrMode,
    ocr_lang: &str,
    ocr_min_text_chars: usize,
) -> Result<ExtractedPages> {
    let pages = extract_pages_with_pdftotext(pdf_path, max_pages_per_doc)?;
    let mut extraction = ExtractedPages {
        page_printed_labels: vec![None; pages.len()],
        text_layer_page_count: pages.len(),
        empty_page_count: pages
            .iter()
            .filter(|page| non_whitespace_char_count(page) == 0)
            .count(),
        page_provenance: pages
            .iter()
            .enumerate()
            .map(|(index, page)| {
                let chars = non_whitespace_char_count(page);
                PageExtractionProvenance {
                    doc_id: doc_id.to_string(),
                    page_pdf: (index + 1) as i64,
                    backend: "text_layer".to_string(),
                    reason: if chars == 0 {
                        "text_layer_empty".to_string()
                    } else {
                        "text_layer_default".to_string()
                    },
                    text_char_count: chars,
                    ocr_char_count: None,
                    printed_page_label: None,
                    printed_page_status: "unknown".to_string(),
                }
            })
            .collect(),
        pages,
        ..ExtractedPages::default()
    };

    let candidate_pages = collect_ocr_candidates(&extraction.pages, ocr_mode, ocr_min_text_chars);
    if candidate_pages.is_empty() {
        refresh_printed_page_labels(&mut extraction);
        apply_page_normalization(&mut extraction);
        return Ok(extraction);
    }

    if !command_available("pdftoppm") || !command_available("tesseract") {
        let message = format!(
            "OCR mode '{}' requested for {} pages but pdftoppm/tesseract are unavailable",
            ocr_mode.as_str(),
            candidate_pages.len()
        );
        if matches!(ocr_mode, OcrMode::Force) {
            bail!(message);
        }

        for page_number in &candidate_pages {
            let page_index = page_number.saturating_sub(1);
            if let Some(entry) = extraction.page_provenance.get_mut(page_index) {
                entry.reason = "ocr_unavailable_text_layer_fallback".to_string();
            }
        }
        extraction.warnings.push(message);
        refresh_printed_page_labels(&mut extraction);
        apply_page_normalization(&mut extraction);
        return Ok(extraction);
    }

    for page_number in candidate_pages {
        let page_index = page_number.saturating_sub(1);
        let current_text = extraction
            .pages
            .get(page_index)
            .cloned()
            .unwrap_or_default();

        match extract_page_with_ocr(pdf_path, page_number, ocr_lang) {
            Ok(ocr_text) => {
                let ocr_char_count = non_whitespace_char_count(&ocr_text);
                if ocr_char_count == 0 && matches!(ocr_mode, OcrMode::Auto) {
                    extraction.warnings.push(format!(
                        "OCR text was empty for {} page {} in auto mode",
                        pdf_path.display(),
                        page_number
                    ));
                    if let Some(entry) = extraction.page_provenance.get_mut(page_index) {
                        entry.reason = "ocr_empty_text_layer_fallback".to_string();
                        entry.ocr_char_count = Some(0);
                    }
                    continue;
                }

                if let Some(page) = extraction.pages.get_mut(page_index) {
                    *page = ocr_text;
                }
                extraction.ocr_page_count += 1;
                extraction.text_layer_page_count =
                    extraction.text_layer_page_count.saturating_sub(1);
                if matches!(ocr_mode, OcrMode::Auto) {
                    extraction.ocr_fallback_page_count += 1;
                }

                let previous_chars = non_whitespace_char_count(&current_text);
                if previous_chars == 0 && ocr_char_count > 0 {
                    extraction.empty_page_count = extraction.empty_page_count.saturating_sub(1);
                } else if previous_chars > 0 && ocr_char_count == 0 {
                    extraction.empty_page_count += 1;
                }

                if let Some(entry) = extraction.page_provenance.get_mut(page_index) {
                    entry.backend = "ocr".to_string();
                    entry.reason = if matches!(ocr_mode, OcrMode::Force) {
                        "ocr_force_mode".to_string()
                    } else {
                        "ocr_auto_low_text".to_string()
                    };
                    entry.text_char_count = ocr_char_count;
                    entry.ocr_char_count = Some(ocr_char_count);
                }
            }
            Err(error) => {
                if matches!(ocr_mode, OcrMode::Force) {
                    return Err(error).with_context(|| {
                        format!(
                            "failed OCR extraction for {} page {}",
                            pdf_path.display(),
                            page_number
                        )
                    });
                }

                extraction.warnings.push(format!(
                    "OCR fallback failed for {} page {}: {}",
                    pdf_path.display(),
                    page_number,
                    error
                ));
                if let Some(page) = extraction.pages.get_mut(page_index) {
                    *page = current_text;
                }
                if let Some(entry) = extraction.page_provenance.get_mut(page_index) {
                    entry.reason = "ocr_failed_text_layer_fallback".to_string();
                }
            }
        }
    }

    refresh_printed_page_labels(&mut extraction);
    apply_page_normalization(&mut extraction);
    Ok(extraction)
}

fn apply_page_normalization(extraction: &mut ExtractedPages) {
    let header_candidates = detect_repeated_edge_lines(&extraction.pages, true);
    let footer_candidates = detect_repeated_edge_lines(&extraction.pages, false);

    let mut header_removed = 0usize;
    let mut footer_removed = 0usize;
    let mut dehyphen_merges = 0usize;

    for page in &mut extraction.pages {
        let mut lines = page
            .lines()
            .map(|line| line.to_string())
            .collect::<Vec<String>>();

        if let Some(index) = first_nonempty_line_index(&lines) {
            let candidate = normalize_edge_line(&lines[index]);
            if !candidate.is_empty() && header_candidates.contains(&candidate) {
                lines.remove(index);
                header_removed += 1;
            }
        }

        if let Some(index) = last_nonempty_line_index(&lines) {
            let candidate = normalize_edge_line(&lines[index]);
            if !candidate.is_empty() && footer_candidates.contains(&candidate) {
                lines.remove(index);
                footer_removed += 1;
            }
        }

        let (normalized_lines, merges) = merge_hyphenated_lines(lines);
        dehyphen_merges += merges;
        *page = normalized_lines.join("\n");
    }

    extraction.header_lines_removed += header_removed;
    extraction.footer_lines_removed += footer_removed;
    extraction.dehyphenation_merges += dehyphen_merges;
    extraction.empty_page_count = extraction
        .pages
        .iter()
        .filter(|page| non_whitespace_char_count(page) == 0)
        .count();
}

fn refresh_printed_page_labels(extraction: &mut ExtractedPages) {
    extraction.page_printed_labels = extraction
        .pages
        .iter()
        .map(|page| detect_printed_page_label(page))
        .collect();

    for (index, label) in extraction.page_printed_labels.iter().enumerate() {
        if let Some(entry) = extraction.page_provenance.get_mut(index) {
            entry.printed_page_label = label.clone();
            if label.is_some() {
                entry.printed_page_status = "detected".to_string();
            } else {
                entry.printed_page_status = "missing".to_string();
            }
        }
    }
}

fn detect_printed_page_label(page_text: &str) -> Option<String> {
    let lines = page_text.lines().collect::<Vec<&str>>();

    let page_regex = Regex::new(r"(?i)^page\s+([0-9ivxlcdm]+)$").ok()?;
    let number_regex = Regex::new(r"^([0-9]{1,4})$").ok()?;
    let roman_regex = Regex::new(r"(?i)^([ivxlcdm]{1,8})$").ok()?;

    for line in lines.iter().rev().take(5).chain(lines.iter().take(2)) {
        let normalized = line
            .chars()
            .filter(|character| {
                character.is_ascii_alphanumeric() || character.is_ascii_whitespace()
            })
            .collect::<String>();
        let normalized = normalized.trim();
        if normalized.is_empty() {
            continue;
        }

        if let Some(captures) = page_regex.captures(normalized) {
            if let Some(value) = captures.get(1) {
                return Some(value.as_str().to_ascii_lowercase());
            }
        }

        if let Some(captures) = number_regex.captures(normalized) {
            if let Some(value) = captures.get(1) {
                return Some(value.as_str().to_string());
            }
        }

        if let Some(captures) = roman_regex.captures(normalized) {
            if let Some(value) = captures.get(1) {
                return Some(value.as_str().to_ascii_lowercase());
            }
        }
    }

    None
}

fn printed_page_label_for(labels: &[Option<String>], page_pdf: i64) -> Option<String> {
    if page_pdf <= 0 {
        return None;
    }

    labels
        .get((page_pdf - 1) as usize)
        .and_then(|label| label.clone())
}

fn detect_repeated_edge_lines(pages: &[String], header: bool) -> HashSet<String> {
    let mut counts = HashMap::<String, usize>::new();
    for page in pages {
        let lines = page.lines().map(str::trim).collect::<Vec<&str>>();
        let candidate = if header {
            lines.iter().copied().find(|line| !line.is_empty())
        } else {
            lines.iter().rev().copied().find(|line| !line.is_empty())
        };

        let Some(candidate) = candidate else {
            continue;
        };

        let normalized = normalize_edge_line(candidate);
        if normalized.is_empty() || normalized.len() > 120 {
            continue;
        }
        *counts.entry(normalized).or_insert(0) += 1;
    }

    counts
        .into_iter()
        .filter_map(|(candidate, count)| if count >= 3 { Some(candidate) } else { None })
        .collect()
}

fn normalize_edge_line(input: &str) -> String {
    input
        .split_whitespace()
        .collect::<Vec<&str>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn first_nonempty_line_index(lines: &[String]) -> Option<usize> {
    lines.iter().position(|line| !line.trim().is_empty())
}

fn last_nonempty_line_index(lines: &[String]) -> Option<usize> {
    lines.iter().rposition(|line| !line.trim().is_empty())
}

fn merge_hyphenated_lines(lines: Vec<String>) -> (Vec<String>, usize) {
    let mut merged = Vec::<String>::new();
    let mut merges = 0usize;
    let mut index = 0usize;

    while index < lines.len() {
        let current = lines[index].clone();
        if index + 1 < lines.len() {
            let next = lines[index + 1].clone();
            if should_merge_hyphenated_pair(&current, &next) {
                let joined = format!(
                    "{}{}",
                    current.trim_end().trim_end_matches('-'),
                    next.trim_start()
                );
                merged.push(joined);
                merges += 1;
                index += 2;
                continue;
            }
        }

        merged.push(current);
        index += 1;
    }

    (merged, merges)
}

fn should_merge_hyphenated_pair(current: &str, next: &str) -> bool {
    let left = current.trim_end();
    if !left.ends_with('-') {
        return false;
    }

    let right = next.trim_start();
    let starts_with_lowercase = right
        .chars()
        .next()
        .map(|character| character.is_ascii_lowercase())
        .unwrap_or(false);
    if !starts_with_lowercase {
        return false;
    }

    left.trim_end_matches('-')
        .chars()
        .last()
        .map(|character| character.is_ascii_alphabetic())
        .unwrap_or(false)
}

fn collect_ocr_candidates(
    pages: &[String],
    ocr_mode: OcrMode,
    min_text_chars: usize,
) -> Vec<usize> {
    match ocr_mode {
        OcrMode::Off => Vec::new(),
        OcrMode::Force => (1..=pages.len()).collect(),
        OcrMode::Auto => pages
            .iter()
            .enumerate()
            .filter_map(|(index, page)| {
                if non_whitespace_char_count(page) < min_text_chars {
                    Some(index + 1)
                } else {
                    None
                }
            })
            .collect(),
    }
}

fn non_whitespace_char_count(text: &str) -> usize {
    text.chars()
        .filter(|character| !character.is_whitespace())
        .count()
}

fn extract_page_with_ocr(pdf_path: &Path, page_number: usize, ocr_lang: &str) -> Result<String> {
    let pdf_stem = pdf_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("pdf");
    let safe_stem = pdf_stem
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();

    let stamp = Utc::now().timestamp_nanos_opt().unwrap_or_default();
    let output_root = std::env::temp_dir().join(format!(
        "iso26262_ocr_{}_{}_{}_{}",
        safe_stem,
        std::process::id(),
        page_number,
        stamp
    ));
    let png_path = PathBuf::from(format!("{}.png", output_root.display()));

    let pdftoppm_output = Command::new("pdftoppm")
        .arg("-f")
        .arg(page_number.to_string())
        .arg("-l")
        .arg(page_number.to_string())
        .arg("-singlefile")
        .arg("-png")
        .arg(pdf_path)
        .arg(&output_root)
        .output()
        .with_context(|| format!("failed to execute pdftoppm for {}", pdf_path.display()))?;

    if !pdftoppm_output.status.success() {
        let stderr = String::from_utf8_lossy(&pdftoppm_output.stderr);
        bail!(
            "pdftoppm returned non-zero exit status for {} page {}: {}",
            pdf_path.display(),
            page_number,
            stderr.trim()
        );
    }

    if !png_path.exists() {
        bail!(
            "pdftoppm did not produce expected image for {} page {}",
            pdf_path.display(),
            page_number
        );
    }

    let tesseract_output = Command::new("tesseract")
        .arg(&png_path)
        .arg("stdout")
        .arg("-l")
        .arg(ocr_lang)
        .output()
        .with_context(|| format!("failed to execute tesseract for {}", png_path.display()))?;

    let _ = fs::remove_file(&png_path);

    if !tesseract_output.status.success() {
        let stderr = String::from_utf8_lossy(&tesseract_output.stderr);
        bail!(
            "tesseract returned non-zero exit status for {} page {}: {}",
            pdf_path.display(),
            page_number,
            stderr.trim()
        );
    }

    Ok(String::from_utf8_lossy(&tesseract_output.stdout)
        .replace('\u{0000}', "")
        .trim()
        .to_string())
}

fn command_available(program: &str) -> bool {
    Command::new(program).arg("--version").output().is_ok()
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
        pdftohtml: command_version("pdftohtml", &["-v"])?,
        pdftoppm: command_version_optional("pdftoppm", &["-v"]),
        tesseract: command_version_optional("tesseract", &["--version"]),
    })
}

fn command_version_optional(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let source = if stdout.trim().is_empty() {
        stderr.trim()
    } else {
        stdout.trim()
    };

    source
        .lines()
        .next()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| line.to_string())
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
    if args.ocr_mode != OcrMode::Off {
        command.push("--ocr-mode".to_string());
        command.push(args.ocr_mode.as_str().to_string());
        command.push("--ocr-lang".to_string());
        command.push(args.ocr_lang.clone());
        command.push("--ocr-min-text-chars".to_string());
        command.push(args.ocr_min_text_chars.to_string());
    }

    command.join(" ")
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn normalize_marker_label_handles_common_forms() {
        assert_eq!(normalize_marker_label("b)"), "b");
        assert_eq!(normalize_marker_label("NOTE 2"), "NOTE 2");
        assert_eq!(normalize_marker_label("—"), "-");
    }

    #[test]
    fn reconstruct_table_rows_assigns_marker_list_to_following_lines() {
        let lines = vec![
            "1a 1b 1c",
            "First requirement description",
            "++",
            "Second requirement description",
            "+",
            "Third requirement description",
            "++",
        ];

        let rows = reconstruct_table_rows_from_markers(&lines);
        assert_eq!(rows.len(), 3);

        assert_eq!(rows[0][0], "1a");
        assert!(rows[0][1].contains("First requirement"));
        assert!(rows[0].iter().any(|cell| cell == "++"));

        assert_eq!(rows[1][0], "1b");
        assert!(rows[1][1].contains("Second requirement"));

        assert_eq!(rows[2][0], "1c");
        assert!(rows[2][1].contains("Third requirement"));
    }

    #[test]
    fn analyze_table_rows_tracks_sparse_and_overloaded_patterns() {
        let rows = vec![
            vec![
                "1a".to_string(),
                "Valid description".to_string(),
                "++".to_string(),
            ],
            vec!["1b".to_string(), String::new(), "+".to_string()],
            vec![
                "1c".to_string(),
                "Contains merged marker 1d text".to_string(),
                "++".to_string(),
            ],
        ];

        let counters = analyze_table_rows(&rows);
        assert_eq!(counters.rows_with_markers_count, 3);
        assert_eq!(counters.rows_with_descriptions_count, 2);
        assert_eq!(counters.sparse_rows_count, 1);
        assert_eq!(counters.overloaded_rows_count, 1);
        assert_eq!(counters.marker_observed_count, 3);
        assert_eq!(counters.marker_expected_count, 3);
    }

    #[test]
    fn prefer_reconstructed_rows_when_quality_improves() {
        let original = TableQualityCounters {
            sparse_rows_count: 4,
            overloaded_rows_count: 1,
            rows_with_markers_count: 5,
            rows_with_descriptions_count: 1,
            marker_expected_count: 5,
            marker_observed_count: 5,
        };
        let reconstructed = TableQualityCounters {
            sparse_rows_count: 1,
            overloaded_rows_count: 1,
            rows_with_markers_count: 5,
            rows_with_descriptions_count: 4,
            marker_expected_count: 5,
            marker_observed_count: 5,
        };

        let preferred = prefer_reconstructed_rows(5, &original, 5, &reconstructed);
        assert!(preferred);
    }

    #[test]
    fn parse_list_items_excludes_note_markers() {
        let list_item_regex = Regex::new(
            r"^(?P<marker>(?:(?:\d+[A-Za-z]?|[A-Za-z])(?:[\.)])?|[-*•—–]))(?:\s+(?P<body>.+))?$",
        )
        .expect("list regex compiles");
        let note_item_regex = Regex::new(r"^(?i)(?P<marker>NOTE(?:\s+\d+)?)(?:\s+(?P<body>.+))?$")
            .expect("note regex compiles");

        let text = "9.4.2 Heading\nNOTE 1 software safety requirements include implementation constraints\na) retain traceability\nb) verify assumptions";
        let (list_items, fallback) =
            parse_list_items(text, "9.4.2 Heading", &list_item_regex, &note_item_regex);

        assert!(!fallback);
        assert_eq!(list_items.len(), 2);
        assert_eq!(list_items[0].marker_norm, "a");
        assert_eq!(list_items[1].marker_norm, "b");
    }

    #[test]
    fn parse_note_items_extracts_note_markers() {
        let list_item_regex = Regex::new(
            r"^(?P<marker>(?:(?:\d+[A-Za-z]?|[A-Za-z])(?:[\.)])?|[-*•—–]))(?:\s+(?P<body>.+))?$",
        )
        .expect("list regex compiles");
        let note_item_regex = Regex::new(r"^(?i)(?P<marker>NOTE(?:\s+\d+)?)(?:\s+(?P<body>.+))?$")
            .expect("note regex compiles");

        let text = "5.2 Heading\nNOTE 1 Development approaches can be suitable\nNOTE 2 Safety activities remain required\na) This line belongs to list";
        let note_items = parse_note_items(text, "5.2 Heading", &note_item_regex, &list_item_regex);

        assert_eq!(note_items.len(), 2);
        assert_eq!(note_items[0].marker_norm, "NOTE 1");
        assert_eq!(note_items[1].marker_norm, "NOTE 2");
        assert!(note_items[0].text.contains("Development approaches"));
    }

    #[test]
    fn parse_paragraphs_splits_on_blank_lines() {
        let list_item_regex = Regex::new(
            r"^(?P<marker>(?:(?:\d+[A-Za-z]?|[A-Za-z])(?:[\.)])?|[-*•—–]))(?:\s+(?P<body>.+))?$",
        )
        .expect("list regex compiles");
        let note_item_regex = Regex::new(r"^(?i)(?P<marker>NOTE(?:\s+\d+)?)(?:\s+(?P<body>.+))?$")
            .expect("note regex compiles");

        let text = "9.3 Heading\nThe software unit should satisfy the first property.\n\nThis second paragraph starts after a blank line.";
        let paragraphs = parse_paragraphs(text, "9.3 Heading", &list_item_regex, &note_item_regex);

        assert_eq!(paragraphs.len(), 2);
        assert!(paragraphs[0].contains("first property"));
        assert!(paragraphs[1].contains("second paragraph"));
    }

    #[test]
    fn parse_paragraphs_splits_before_marker_transitions() {
        let list_item_regex = Regex::new(
            r"^(?P<marker>(?:(?:\d+[A-Za-z]?|[A-Za-z])(?:[\.)])?|[-*•—–]))(?:\s+(?P<body>.+))?$",
        )
        .expect("list regex compiles");
        let note_item_regex = Regex::new(r"^(?i)(?P<marker>NOTE(?:\s+\d+)?)(?:\s+(?P<body>.+))?$")
            .expect("note regex compiles");

        let text = "8.4.5 Heading\nThe following principles apply to software units.\nNOTE 1 This is informative guidance.\na) first normative bullet";
        let paragraphs =
            parse_paragraphs(text, "8.4.5 Heading", &list_item_regex, &note_item_regex);

        assert_eq!(paragraphs.len(), 3);
        assert_eq!(paragraphs[1], "NOTE 1 This is informative guidance.");
        assert_eq!(paragraphs[2], "a) first normative bullet");
    }

    #[test]
    fn collect_ocr_candidates_auto_uses_text_threshold() {
        let pages = vec![
            "minimal".to_string(),
            "this page has substantially more extractable text content".to_string(),
            "tiny".to_string(),
        ];

        let candidates = collect_ocr_candidates(&pages, OcrMode::Auto, 10);
        assert_eq!(candidates, vec![1, 3]);
    }

    #[test]
    fn render_ingest_command_includes_ocr_flags_when_enabled() {
        let args = IngestArgs {
            cache_root: PathBuf::from(".cache/iso26262"),
            inventory_manifest_path: None,
            ingest_manifest_path: None,
            db_path: None,
            refresh_inventory: false,
            seed_page_chunks: false,
            target_parts: vec![6],
            max_pages_per_doc: Some(5),
            ocr_mode: OcrMode::Auto,
            ocr_lang: "eng".to_string(),
            ocr_min_text_chars: 200,
        };

        let command = render_ingest_command(&args);
        assert!(command.contains("--ocr-mode auto"));
        assert!(command.contains("--ocr-lang eng"));
        assert!(command.contains("--ocr-min-text-chars 200"));
    }

    #[test]
    fn apply_page_normalization_strips_repeated_headers_and_footers() {
        let mut extraction = ExtractedPages {
            pages: vec![
                "ISO 26262 Part 6\nRequirement intro\nLicensed copy".to_string(),
                "ISO 26262 Part 6\nAnother requirement\nLicensed copy".to_string(),
                "ISO 26262 Part 6\nFinal requirement\nLicensed copy".to_string(),
            ],
            ..ExtractedPages::default()
        };

        apply_page_normalization(&mut extraction);

        assert_eq!(extraction.header_lines_removed, 3);
        assert_eq!(extraction.footer_lines_removed, 3);
        assert!(
            extraction
                .pages
                .iter()
                .all(|page| !page.contains("ISO 26262 Part 6"))
        );
        assert!(
            extraction
                .pages
                .iter()
                .all(|page| !page.contains("Licensed copy"))
        );
    }

    #[test]
    fn apply_page_normalization_merges_hyphenated_line_wraps() {
        let mut extraction = ExtractedPages {
            pages: vec!["soft-\nware unit".to_string()],
            ..ExtractedPages::default()
        };

        apply_page_normalization(&mut extraction);

        assert_eq!(extraction.dehyphenation_merges, 1);
        assert_eq!(extraction.pages[0], "software unit");
    }

    #[test]
    fn detect_printed_page_label_supports_numeric_and_roman_labels() {
        let numeric_page = "Some line\nPage 12\n";
        let roman_page = "Some line\nii\n";

        assert_eq!(
            detect_printed_page_label(numeric_page),
            Some("12".to_string())
        );
        assert_eq!(
            detect_printed_page_label(roman_page),
            Some("ii".to_string())
        );
    }
}
