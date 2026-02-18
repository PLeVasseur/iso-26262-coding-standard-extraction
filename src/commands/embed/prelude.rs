use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, Connection, OpenFlags};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tracing::info;

use crate::cli::{EmbedArgs, EmbedRefreshMode};
use crate::commands::ingest::ensure_embedding_schema;
use crate::semantic::{
    chunk_payload_for_embedding, embed_text_local, embedding_text_hash, encode_embedding_blob,
    resolve_model_config, SemanticModelConfig,
};
use crate::util::{ensure_directory, now_utc_string, utc_compact_string, write_json_pretty};

const EMBEDDING_DB_SCHEMA_VERSION: &str = "0.4.0";
const SEMANTIC_MODEL_CONFIG_LOCK_PATH: &str = "manifests/semantic_model_config.lock.json";

#[derive(Debug, Clone)]
struct EmbedChunkRow {
    chunk_id: String,
    chunk_type: String,
    reference: String,
    heading: String,
    text: Option<String>,
    table_md: Option<String>,
}

#[derive(Debug, Clone)]
struct ExistingEmbeddingRow {
    text_hash: String,
    embedding_dim: usize,
}

#[derive(Debug, Serialize)]
struct EmbeddingRunManifest {
    manifest_version: u32,
    run_id: String,
    generated_at: String,
    model_id: String,
    model_name: String,
    embedding_dim: usize,
    normalization: String,
    backend: String,
    db_schema_version: String,
    refresh_mode: String,
    chunk_type_filter: Vec<String>,
    eligible_chunks: usize,
    embedded_chunks: usize,
    updated_chunks: usize,
    skipped_empty_chunks: usize,
    stale_rows_before: usize,
    stale_rows_after: usize,
    batch_size: usize,
    duration_ms: u128,
    status: String,
    warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
struct SemanticModelConfigLock {
    manifest_version: u32,
    model_id: String,
    model_name: String,
    embedding_dim: usize,
    normalization: String,
    runtime_backend: String,
    created_at: String,
    checksum: String,
}
