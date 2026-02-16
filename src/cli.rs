use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "iso26262",
    version,
    about = "Local ISO 26262 extraction and query tooling"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Inventory(InventoryArgs),
    Ingest(IngestArgs),
    Status(StatusArgs),
}

#[derive(Args, Debug, Clone)]
pub struct InventoryArgs {
    #[arg(long, default_value = ".cache/iso26262")]
    pub cache_root: PathBuf,

    #[arg(long)]
    pub manifest_path: Option<PathBuf>,

    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
}

#[derive(Args, Debug, Clone)]
pub struct IngestArgs {
    #[arg(long, default_value = ".cache/iso26262")]
    pub cache_root: PathBuf,

    #[arg(long)]
    pub inventory_manifest_path: Option<PathBuf>,

    #[arg(long)]
    pub ingest_manifest_path: Option<PathBuf>,

    #[arg(long)]
    pub db_path: Option<PathBuf>,

    #[arg(long, default_value_t = false)]
    pub refresh_inventory: bool,

    #[arg(long, default_value_t = false)]
    pub seed_page_chunks: bool,

    #[arg(long = "target-part")]
    pub target_parts: Vec<u32>,

    #[arg(long)]
    pub max_pages_per_doc: Option<usize>,
}

#[derive(Args, Debug, Clone)]
pub struct StatusArgs {
    #[arg(long, default_value = ".cache/iso26262")]
    pub cache_root: PathBuf,
}
