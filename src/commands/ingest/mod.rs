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

const DB_SCHEMA_VERSION: &str = "0.4.0";

mod db_setup;
mod reference_outline;
mod structured_insertions;
mod node_table_insert;
mod ocr_manifest;
mod page_processing;
mod block_parsing;
mod pipeline;
mod pipeline_page_chunks;
mod pipeline_section_nodes;
mod pipeline_structured_chunks;
mod run;
mod table_parsing;
mod table_parsing_quality;
#[cfg(test)]
mod tests;
mod structured_types;

pub use run::run;
pub use db_setup::ensure_embedding_schema;

use db_setup::*;
use reference_outline::*;
use structured_insertions::*;
use node_table_insert::*;
use ocr_manifest::*;
use page_processing::*;
use block_parsing::*;
use pipeline::*;
use pipeline_page_chunks::*;
use pipeline_section_nodes::*;
use pipeline_structured_chunks::*;
use table_parsing::*;
use table_parsing_quality::*;
use structured_types::*;
