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

mod prelude_types_part1;
mod prelude_types_part2;
mod run;
mod stage_and_wp2_core;
mod fidelity_metrics_part1;
mod fidelity_metrics_part2;
mod semantic_quality;
mod citation_parity_part1;
mod coverage_freshness_part1;
mod coverage_freshness_part2;
mod checks_and_summary;
mod invariants_and_hierarchy;
mod tail_and_tests;

use self::checks_and_summary::*;
use self::citation_parity_part1::*;
use self::coverage_freshness_part1::*;
use self::coverage_freshness_part2::*;
use self::fidelity_metrics_part1::*;
use self::fidelity_metrics_part2::*;
use self::invariants_and_hierarchy::*;
use self::prelude_types_part1::*;
use self::prelude_types_part2::*;
use self::semantic_quality::*;
use self::stage_and_wp2_core::*;
use self::tail_and_tests::*;

pub use self::run::run;
