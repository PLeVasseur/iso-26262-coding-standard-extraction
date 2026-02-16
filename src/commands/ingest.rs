use std::fs;
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result, bail};
use chrono::Utc;
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

    let mut warnings = Vec::new();
    let page_chunks_inserted = if args.seed_page_chunks {
        let (inserted, extract_warnings) = seed_page_chunks(
            &mut connection,
            &cache_root,
            &inventory.pdfs,
            args.max_pages_per_doc,
        )?;
        warnings.extend(extract_warnings);
        inserted
    } else {
        0
    };

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
            docs_upserted,
            docs_total,
            chunks_total,
            page_chunks_inserted,
            ocr_page_count: 0,
        },
        source_hashes: inventory.pdfs,
        warnings,
        notes: vec![
            "Ingest command completed using local manifests and sqlite store.".to_string(),
            "Page chunk seeding uses pdftotext extraction when enabled.".to_string(),
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

fn seed_page_chunks(
    connection: &mut Connection,
    cache_root: &Path,
    pdfs: &[PdfEntry],
    max_pages_per_doc: Option<usize>,
) -> Result<(usize, Vec<String>)> {
    let tx = connection.transaction()?;
    let mut inserted = 0usize;
    let mut warnings = Vec::new();

    {
        let mut statement = tx.prepare(
            "
            INSERT INTO chunks(
              chunk_id, doc_id, type, ref, ref_path, heading, chunk_seq,
              page_pdf_start, page_pdf_end, text, source_hash
            )
            VALUES(?1, ?2, 'page', ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
            ON CONFLICT(chunk_id) DO UPDATE SET
              doc_id=excluded.doc_id,
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
            let pdf_path = cache_root.join(&pdf.filename);
            if !pdf_path.exists() {
                warnings.push(format!("missing source PDF: {}", pdf_path.display()));
                continue;
            }

            let pages = match extract_pages_with_pdftotext(&pdf_path, max_pages_per_doc) {
                Ok(pages) => pages,
                Err(err) => {
                    let warning =
                        format!("failed to extract text for {}: {err}", pdf_path.display());
                    warn!(warning = %warning, "pdf extraction warning");
                    warnings.push(warning);
                    continue;
                }
            };

            let doc_id = doc_id_for(pdf);
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
                    &page_ref,
                    &page_ref,
                    &heading,
                    page_number,
                    page_number,
                    page_number,
                    text,
                    &pdf.sha256
                ])?;
                inserted += 1;
            }
        }
    }

    tx.commit()?;
    Ok((inserted, warnings))
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
    if let Some(max_pages) = args.max_pages_per_doc {
        command.push("--max-pages-per-doc".to_string());
        command.push(max_pages.to_string());
    }

    command.join(" ")
}
