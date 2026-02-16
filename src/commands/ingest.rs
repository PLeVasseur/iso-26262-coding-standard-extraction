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

const DB_SCHEMA_VERSION: &str = "0.1.0";

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
            chunks_total,
            structured_chunks_inserted: chunk_stats.structured_chunks_inserted,
            clause_chunks_inserted: chunk_stats.clause_chunks_inserted,
            table_chunks_inserted: chunk_stats.table_chunks_inserted,
            annex_chunks_inserted: chunk_stats.annex_chunks_inserted,
            page_chunks_inserted: chunk_stats.page_chunks_inserted,
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
          FOREIGN KEY(doc_id) REFERENCES docs(doc_id)
        );
        ",
    )?;

    connection
        .execute(
            "
            CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts
            USING fts5(chunk_id, doc_id, ref, heading, text, content='chunks', content_rowid='rowid')
            ",
            [],
        )
        .context("failed to initialize FTS5 table chunks_fts")?;

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
    warnings: Vec<String>,
}

#[derive(Debug, Clone)]
struct StructuredChunkDraft {
    chunk_type: ChunkType,
    reference: String,
    heading: String,
    text: String,
    page_start: i64,
    page_end: i64,
}

#[derive(Debug, Clone, Copy)]
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

            StructuredChunkDraft {
                chunk_type: active.chunk_type,
                reference: active.reference,
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

    {
        let mut statement = tx.prepare(
            "
            INSERT INTO chunks(
              chunk_id, doc_id, type, ref, ref_path, heading, chunk_seq,
              page_pdf_start, page_pdf_end, text, source_hash
            )
            VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
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
              source_hash=excluded.source_hash
            ",
        )?;

        for pdf in pdfs {
            if !target_set.is_empty() && !target_set.contains(&pdf.part) {
                continue;
            }

            stats.processed_pdf_count += 1;

            let doc_id = doc_id_for(pdf);
            tx.execute("DELETE FROM chunks WHERE doc_id = ?1", [&doc_id])?;

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

            let structured_chunks = parser.parse_pages(&pages);
            let mut seen_refs: HashMap<String, i64> = HashMap::new();
            let mut structured_seq: i64 = 1;

            for chunk in structured_chunks {
                let ref_key = sanitize_ref_for_id(&chunk.reference);
                let count = seen_refs
                    .entry(format!("{}:{}", chunk.chunk_type.as_str(), ref_key))
                    .and_modify(|value| *value += 1)
                    .or_insert(1);

                let chunk_id = format!(
                    "{}:{}:{}:{:03}",
                    doc_id,
                    chunk.chunk_type.as_str(),
                    ref_key,
                    count
                );

                statement.execute(params![
                    chunk_id,
                    &doc_id,
                    chunk.chunk_type.as_str(),
                    &chunk.reference,
                    &chunk.reference,
                    &chunk.heading,
                    structured_seq,
                    chunk.page_start,
                    chunk.page_end,
                    &chunk.text,
                    &pdf.sha256
                ])?;

                stats.structured_chunks_inserted += 1;
                match chunk.chunk_type {
                    ChunkType::Clause => stats.clause_chunks_inserted += 1,
                    ChunkType::Table => stats.table_chunks_inserted += 1,
                    ChunkType::Annex => stats.annex_chunks_inserted += 1,
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

                    statement.execute(params![
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
                        &pdf.sha256
                    ])?;
                    stats.page_chunks_inserted += 1;
                }
            }
        }
    }

    tx.commit()?;
    Ok(stats)
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
