use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};
use chrono::Utc;
use regex::Regex;
use rusqlite::{params, Connection};
use serde::Serialize;
use tracing::{info, warn};

use crate::cli::{IngestArgs, OcrMode};
use crate::commands::inventory;
use crate::model::{
    IngestCounts, IngestPaths, IngestRunManifest, PdfEntry, PdfInventoryManifest, ToolVersions,
};
use crate::util::{ensure_directory, now_utc_string, utc_compact_string, write_json_pretty};

const DB_SCHEMA_VERSION: &str = "0.4.0";

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
            list_parse_candidate_count: chunk_stats.list_parse_candidate_count,
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
