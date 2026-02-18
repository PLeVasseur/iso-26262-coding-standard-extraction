use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

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
    Embed(EmbedArgs),
    Query(QueryArgs),
    Status(StatusArgs),
    Validate(ValidateArgs),
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

    #[arg(long, value_enum, default_value_t = OcrMode::Off)]
    pub ocr_mode: OcrMode,

    #[arg(long, default_value = "eng")]
    pub ocr_lang: String,

    #[arg(long, default_value_t = 120)]
    pub ocr_min_text_chars: usize,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum OcrMode {
    Off,
    Auto,
    Force,
}

impl OcrMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Auto => "auto",
            Self::Force => "force",
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum EmbedRefreshMode {
    Full,
    MissingOrStale,
}

#[derive(Args, Debug, Clone)]
pub struct EmbedArgs {
    #[arg(long, default_value = ".cache/iso26262")]
    pub cache_root: PathBuf,

    #[arg(long)]
    pub db_path: Option<PathBuf>,

    #[arg(long, default_value = "miniLM-L6-v2-local-v1")]
    pub model_id: String,

    #[arg(long, value_enum, default_value_t = EmbedRefreshMode::MissingOrStale)]
    pub refresh_mode: EmbedRefreshMode,

    #[arg(long, default_value_t = 64)]
    pub batch_size: usize,

    #[arg(long = "chunk-type")]
    pub chunk_types: Vec<String>,

    #[arg(long)]
    pub semantic_model_lock_path: Option<PathBuf>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum RetrievalMode {
    Lexical,
    Semantic,
    Hybrid,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum FusionMode {
    Rrf,
}

#[derive(Args, Debug, Clone)]
pub struct StatusArgs {
    #[arg(long, default_value = ".cache/iso26262")]
    pub cache_root: PathBuf,
}

#[derive(Args, Debug, Clone)]
pub struct QueryArgs {
    #[arg(long, default_value = ".cache/iso26262")]
    pub cache_root: PathBuf,

    #[arg(long)]
    pub db_path: Option<PathBuf>,

    #[arg(long)]
    pub query: String,

    #[arg(long, value_enum, default_value_t = RetrievalMode::Lexical)]
    pub retrieval_mode: RetrievalMode,

    #[arg(long, default_value_t = 96)]
    pub lexical_k: usize,

    #[arg(long, default_value_t = 96)]
    pub semantic_k: usize,

    #[arg(long, value_enum, default_value_t = FusionMode::Rrf)]
    pub fusion: FusionMode,

    #[arg(long, default_value_t = 60)]
    pub rrf_k: u32,

    #[arg(long)]
    pub semantic_model_id: Option<String>,

    #[arg(long, default_value_t = false)]
    pub allow_lexical_fallback: bool,

    #[arg(long, default_value_t = 2000)]
    pub timeout_ms: u64,

    #[arg(long, default_value_t = 10)]
    pub limit: usize,

    #[arg(long)]
    pub part: Option<u32>,

    #[arg(long = "type")]
    pub chunk_type: Option<String>,

    #[arg(long)]
    pub node_type: Option<String>,

    #[arg(long, default_value_t = false)]
    pub with_ancestors: bool,

    #[arg(long, default_value_t = false)]
    pub with_descendants: bool,

    #[arg(long, default_value_t = false)]
    pub with_pinpoint: bool,

    #[arg(long, default_value_t = 3)]
    pub pinpoint_max_units: usize,

    #[arg(long, default_value_t = false)]
    pub json: bool,
}

#[derive(Args, Debug, Clone)]
pub struct ValidateArgs {
    #[arg(long, default_value = ".cache/iso26262")]
    pub cache_root: PathBuf,

    #[arg(long)]
    pub db_path: Option<PathBuf>,

    #[arg(long)]
    pub gold_manifest_path: Option<PathBuf>,

    #[arg(long)]
    pub quality_report_path: Option<PathBuf>,
}
