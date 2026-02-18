use std::collections::{HashMap, HashSet};
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::cli::ValidateArgs;
use crate::semantic::{chunk_payload_for_embedding, embedding_text_hash, DEFAULT_MODEL_ID};
use crate::util::{now_utc_string, write_json_pretty};

mod core_types;
mod semantic_types;
mod run;
mod wp2_gate;
mod extraction_metrics;
mod semantics_metrics;
mod semantic_quality;
mod citation_parity;
mod freshness_inputs;
mod reference_evaluation;
mod quality_checks;
mod structural_invariants;
mod formatting;
#[cfg(test)]
mod tests;

use self::quality_checks::*;
use self::citation_parity::*;
use self::freshness_inputs::*;
use self::reference_evaluation::*;
use self::extraction_metrics::*;
use self::semantics_metrics::*;
use self::structural_invariants::*;
use self::core_types::*;
use self::semantic_types::*;
use self::semantic_quality::*;
use self::wp2_gate::*;
use self::formatting::*;

pub use self::run::run;
