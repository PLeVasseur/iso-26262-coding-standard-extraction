use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PdfEntry {
    pub filename: String,
    pub part: u32,
    pub year: u32,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PdfInventoryManifest {
    pub manifest_version: u32,
    pub generated_at: String,
    pub source_directory: String,
    pub pdf_count: usize,
    pub pdfs: Vec<PdfEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RunStateManifest {
    pub active_run_id: Option<String>,
    pub current_phase: Option<String>,
    pub phase_id: Option<String>,
    pub current_step: Option<String>,
    pub status: Option<String>,
    pub base_branch: Option<String>,
    pub active_branch: Option<String>,
    pub commit_mode: Option<String>,
    pub last_commit: Option<String>,
    pub failed_step: Option<String>,
    pub failure_reason: Option<String>,
    pub resume_from_step: Option<String>,
    pub next_planned_command: Option<String>,
    pub started_at: Option<String>,
    pub updated_at: Option<String>,
    pub last_successful_command: Option<String>,
    pub last_successful_artifact: Option<String>,
    pub compatibility: Option<RunStateCompatibility>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RunStateCompatibility {
    pub runbook_version: Option<String>,
    pub engine_version: Option<String>,
    pub db_schema_version: Option<String>,
    pub status: Option<String>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ToolVersions {
    pub rustc: String,
    pub cargo: String,
    pub pdftotext: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct IngestPaths {
    pub cache_root: String,
    pub manifest_dir: String,
    pub inventory_manifest_path: String,
    pub db_path: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct IngestCounts {
    pub pdf_count: usize,
    pub processed_pdf_count: usize,
    pub docs_upserted: usize,
    pub docs_total: i64,
    pub nodes_total: i64,
    pub chunks_total: i64,
    pub structured_chunks_inserted: usize,
    pub clause_chunks_inserted: usize,
    pub table_chunks_inserted: usize,
    pub annex_chunks_inserted: usize,
    pub page_chunks_inserted: usize,
    pub clause_nodes_inserted: usize,
    pub subclause_nodes_inserted: usize,
    pub annex_nodes_inserted: usize,
    pub table_nodes_inserted: usize,
    pub table_row_nodes_inserted: usize,
    pub table_cell_nodes_inserted: usize,
    pub list_nodes_inserted: usize,
    pub list_item_nodes_inserted: usize,
    pub note_nodes_inserted: usize,
    pub note_item_nodes_inserted: usize,
    pub paragraph_nodes_inserted: usize,
    pub requirement_atom_nodes_inserted: usize,
    pub table_raw_fallback_count: usize,
    pub list_parse_fallback_count: usize,
    pub table_sparse_rows_count: usize,
    pub table_overloaded_rows_count: usize,
    pub table_rows_with_markers_count: usize,
    pub table_rows_with_descriptions_count: usize,
    pub table_marker_expected_count: usize,
    pub table_marker_observed_count: usize,
    pub ocr_page_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct IngestRunManifest {
    pub manifest_version: u32,
    pub run_id: String,
    pub db_schema_version: String,
    pub status: String,
    pub started_at: String,
    pub updated_at: String,
    pub completed_steps: Vec<String>,
    pub current_step: String,
    pub failed_step: Option<String>,
    pub failure_reason: Option<String>,
    pub command: String,
    pub tool_versions: ToolVersions,
    pub paths: IngestPaths,
    pub counts: IngestCounts,
    pub source_hashes: Vec<PdfEntry>,
    pub warnings: Vec<String>,
    pub notes: Vec<String>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CitationAnchorFields {
    pub anchor_type: Option<String>,
    pub anchor_label_raw: Option<String>,
    pub anchor_label_norm: Option<String>,
    pub anchor_order: Option<i64>,
    pub citation_anchor_id: Option<String>,
}
