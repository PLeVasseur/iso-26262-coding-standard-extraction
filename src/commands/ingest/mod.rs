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
mod ids_and_outline;
mod list_note_requirement_insert;
mod node_and_table_insert;
mod ocr_tools_and_manifest;
mod page_extract_and_normalize;
mod paragraphs_and_list_parse;
mod pipeline;
mod pipeline_page_chunks;
mod pipeline_section_nodes;
mod pipeline_structured_chunks;
mod run;
mod table_parse_quality_part1;
mod table_parse_quality_part2;
#[cfg(test)]
mod tests;
mod types_and_structured;

pub use run::run;
pub use db_setup::ensure_embedding_schema;

use db_setup::*;
use ids_and_outline::*;
use list_note_requirement_insert::*;
use node_and_table_insert::*;
use ocr_tools_and_manifest::*;
use page_extract_and_normalize::*;
use paragraphs_and_list_parse::*;
use pipeline::*;
use pipeline_page_chunks::*;
use pipeline_section_nodes::*;
use pipeline_structured_chunks::*;
use table_parse_quality_part1::*;
use table_parse_quality_part2::*;
use types_and_structured::*;
