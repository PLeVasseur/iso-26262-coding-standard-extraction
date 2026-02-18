use serde::Serialize;

pub(super) const EMBEDDING_DB_SCHEMA_VERSION: &str = "0.4.0";
pub(super) const SEMANTIC_MODEL_CONFIG_LOCK_PATH: &str =
    "manifests/semantic_model_config.lock.json";

#[derive(Debug, Clone)]
pub(super) struct EmbedChunkRow {
    pub(super) chunk_id: String,
    pub(super) chunk_type: String,
    pub(super) reference: String,
    pub(super) heading: String,
    pub(super) text: Option<String>,
    pub(super) table_md: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct ExistingEmbeddingRow {
    pub(super) text_hash: String,
    pub(super) embedding_dim: usize,
}

#[derive(Debug, Serialize)]
pub(super) struct EmbeddingRunManifest {
    pub(super) manifest_version: u32,
    pub(super) run_id: String,
    pub(super) generated_at: String,
    pub(super) model_id: String,
    pub(super) model_name: String,
    pub(super) embedding_dim: usize,
    pub(super) normalization: String,
    pub(super) backend: String,
    pub(super) db_schema_version: String,
    pub(super) refresh_mode: String,
    pub(super) chunk_type_filter: Vec<String>,
    pub(super) eligible_chunks: usize,
    pub(super) embedded_chunks: usize,
    pub(super) updated_chunks: usize,
    pub(super) skipped_empty_chunks: usize,
    pub(super) stale_rows_before: usize,
    pub(super) stale_rows_after: usize,
    pub(super) batch_size: usize,
    pub(super) duration_ms: u128,
    pub(super) status: String,
    pub(super) warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(super) struct SemanticModelConfigLock {
    pub(super) manifest_version: u32,
    pub(super) model_id: String,
    pub(super) model_name: String,
    pub(super) embedding_dim: usize,
    pub(super) normalization: String,
    pub(super) runtime_backend: String,
    pub(super) created_at: String,
    pub(super) checksum: String,
}
