use std::time::Instant;

use anyhow::{bail, Context, Result};
use rusqlite::{Connection, OpenFlags};
use serde::Serialize;
use tracing::{info, warn};

use crate::cli::{FusionMode, QueryArgs, RetrievalMode};
use crate::semantic::resolve_model_config;

use super::fusion::fuse_rrf_candidates;
use super::intent::is_exact_intent_query;
use super::output::{write_json_response, write_text_response};
use super::result_hydration::to_results;
use super::retrieval::collect_lexical_candidates;
use super::semantic_retrieval::{collect_semantic_candidates, semantic_index_status};

const MAX_QUERY_CANDIDATES: usize = 256;

#[derive(Debug, Clone)]
pub(super) struct QueryCandidate {
    pub(super) score: f64,
    pub(super) match_kind: String,
    pub(super) source_tags: Vec<String>,
    pub(super) lexical_rank: Option<usize>,
    pub(super) semantic_rank: Option<usize>,
    pub(super) lexical_score: Option<f64>,
    pub(super) semantic_score: Option<f64>,
    pub(super) rrf_score: Option<f64>,
    pub(super) chunk_id: String,
    pub(super) doc_id: String,
    pub(super) part: u32,
    pub(super) year: u32,
    pub(super) chunk_type: String,
    pub(super) reference: String,
    pub(super) heading: String,
    pub(super) page_pdf_start: Option<i64>,
    pub(super) page_pdf_end: Option<i64>,
    pub(super) source_hash: String,
    pub(super) snippet: String,
    pub(super) origin_node_id: Option<String>,
    pub(super) leaf_node_type: Option<String>,
    pub(super) ancestor_path: Option<String>,
    pub(super) anchor_type: Option<String>,
    pub(super) anchor_label_raw: Option<String>,
    pub(super) anchor_label_norm: Option<String>,
    pub(super) anchor_order: Option<i64>,
    pub(super) citation_anchor_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct DescendantNode {
    pub(super) node_id: String,
    pub(super) parent_node_id: Option<String>,
    pub(super) node_type: String,
    pub(super) reference: Option<String>,
    pub(super) heading: Option<String>,
    pub(super) order_index: i64,
    pub(super) page_pdf_start: Option<i64>,
    pub(super) page_pdf_end: Option<i64>,
    pub(super) text_preview: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct PinpointUnit {
    pub(super) unit_id: String,
    pub(super) unit_type: String,
    pub(super) score: f64,
    pub(super) text_preview: String,
    pub(super) token_signature: String,
    pub(super) char_start: Option<usize>,
    pub(super) char_end: Option<usize>,
    pub(super) row_idx: Option<i64>,
    pub(super) col_idx: Option<i64>,
    pub(super) row_key: Option<String>,
    pub(super) origin_node_id: Option<String>,
    pub(super) citation_anchor_id: Option<String>,
    pub(super) citation_anchor_compatible: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct QueryRankTrace {
    pub(super) lexical_rank: Option<usize>,
    pub(super) semantic_rank: Option<usize>,
    pub(super) lexical_score: Option<f64>,
    pub(super) semantic_score: Option<f64>,
    pub(super) rrf_score: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct QueryResult {
    pub(super) rank: usize,
    pub(super) score: f64,
    pub(super) match_kind: String,
    pub(super) source_tags: Vec<String>,
    pub(super) rank_trace: QueryRankTrace,
    pub(super) chunk_id: String,
    pub(super) doc_id: String,
    pub(super) part: u32,
    pub(super) year: u32,
    pub(super) chunk_type: String,
    pub(super) reference: String,
    pub(super) parent_ref: Option<String>,
    pub(super) heading: String,
    pub(super) page_pdf_start: Option<i64>,
    pub(super) page_pdf_end: Option<i64>,
    pub(super) source_hash: String,
    pub(super) snippet: String,
    pub(super) citation: String,
    pub(super) origin_node_id: Option<String>,
    pub(super) leaf_node_type: Option<String>,
    pub(super) ancestor_path: Option<String>,
    pub(super) anchor_type: Option<String>,
    pub(super) anchor_label_raw: Option<String>,
    pub(super) anchor_label_norm: Option<String>,
    pub(super) anchor_order: Option<i64>,
    pub(super) citation_anchor_id: Option<String>,
    pub(super) ancestor_nodes: Option<Vec<String>>,
    pub(super) descendants: Option<Vec<DescendantNode>>,
    pub(super) pinpoint_fallback_used: Option<bool>,
    pub(super) pinpoint_units: Option<Vec<PinpointUnit>>,
}

#[derive(Debug, Serialize)]
pub(super) struct RetrievalMetadata {
    pub(super) requested_mode: String,
    pub(super) effective_mode: String,
    pub(super) lexical_k: usize,
    pub(super) semantic_k: usize,
    pub(super) lexical_candidate_count: usize,
    pub(super) semantic_candidate_count: usize,
    pub(super) fused_candidate_count: usize,
    pub(super) fusion: String,
    pub(super) rrf_k: u32,
    pub(super) semantic_model_id: Option<String>,
    pub(super) exact_intent: bool,
    pub(super) exact_intent_forced_lexical: bool,
    pub(super) fallback_used: bool,
    pub(super) fallback_reason: Option<String>,
    pub(super) pinpoint_enabled: bool,
    pub(super) pinpoint_max_units: usize,
    pub(super) timeout_ms: u64,
    pub(super) query_duration_ms: f64,
}

#[derive(Debug, Serialize)]
pub(super) struct QueryResponse {
    pub(super) query: String,
    pub(super) limit: usize,
    pub(super) returned: usize,
    pub(super) part_filter: Option<u32>,
    pub(super) chunk_type_filter: Option<String>,
    pub(super) node_type_filter: Option<String>,
    pub(super) retrieval: RetrievalMetadata,
    pub(super) results: Vec<QueryResult>,
}

pub fn run(args: QueryArgs) -> Result<()> {
    let query_started = Instant::now();
    let query_text = args.query.trim();
    if query_text.is_empty() {
        bail!("query must not be empty");
    }

    let db_path = args
        .db_path
        .clone()
        .unwrap_or_else(|| args.cache_root.join("iso26262_index.sqlite"));

    let connection = Connection::open_with_flags(
        &db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("failed to open database read-only: {}", db_path.display()))?;

    let chunk_type_filter = args
        .chunk_type
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_lowercase);
    let node_type_filter = args
        .node_type
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_lowercase);

    let limit = args.limit.max(1);
    let pinpoint_max_units = args.pinpoint_max_units.max(1).min(12);
    let lexical_k = clamp_candidates(args.lexical_k.max(limit));
    let semantic_k = clamp_candidates(args.semantic_k.max(limit));
    let timeout_budget = QueryTimeoutBudget::new(args.timeout_ms);
    let exact_intent = is_exact_intent_query(query_text);

    let requested_mode = args.retrieval_mode;
    let exact_intent_forced_lexical = exact_intent
        && matches!(
            requested_mode,
            RetrievalMode::Hybrid | RetrievalMode::Semantic
        );
    let mut effective_mode = if exact_intent_forced_lexical {
        RetrievalMode::Lexical
    } else {
        requested_mode
    };

    let mut fallback_used = false;
    let mut fallback_reason = None::<String>;
    let semantic_model_id = args.semantic_model_id.as_ref().map(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    });
    let semantic_model_id = semantic_model_id.flatten();

    let mut lexical_candidates = Vec::<QueryCandidate>::new();
    if matches!(
        effective_mode,
        RetrievalMode::Lexical | RetrievalMode::Hybrid
    ) {
        lexical_candidates = collect_lexical_candidates(
            &connection,
            query_text,
            args.part,
            chunk_type_filter.as_deref(),
            node_type_filter.as_deref(),
            lexical_k,
        )?;
        enforce_timeout(timeout_budget, "lexical retrieval")?;
    }

    let mut semantic_candidates = Vec::<QueryCandidate>::new();
    if matches!(
        effective_mode,
        RetrievalMode::Semantic | RetrievalMode::Hybrid
    ) {
        let Some(model_id) = semantic_model_id.as_deref() else {
            bail!(
                "semantic retrieval requires --semantic-model-id (recommended: {})",
                crate::semantic::DEFAULT_MODEL_ID
            );
        };

        let semantic_status = semantic_index_status(&connection, model_id)?;
        if !semantic_status.available {
            let reason = semantic_status
                .reason
                .unwrap_or_else(|| "semantic index is unavailable".to_string());
            if args.allow_lexical_fallback {
                warn!(reason = %reason, "semantic retrieval unavailable; falling back to lexical");
                fallback_used = true;
                fallback_reason = Some(reason);
                effective_mode = RetrievalMode::Lexical;
                if lexical_candidates.is_empty() {
                    lexical_candidates = collect_lexical_candidates(
                        &connection,
                        query_text,
                        args.part,
                        chunk_type_filter.as_deref(),
                        node_type_filter.as_deref(),
                        lexical_k,
                    )?;
                }
            } else {
                bail!(
                    "{}; run `cargo run -- embed --cache-root .cache/iso26262 --model-id {}` or pass --allow-lexical-fallback",
                    reason,
                    model_id
                );
            }
        } else {
            let model = resolve_model_config(model_id);
            semantic_candidates = collect_semantic_candidates(
                &connection,
                query_text,
                args.part,
                chunk_type_filter.as_deref(),
                node_type_filter.as_deref(),
                model_id,
                model.dimensions,
                semantic_k,
                timeout_budget,
            )?;
            enforce_timeout(timeout_budget, "semantic retrieval")?;
        }
    }

    let lexical_candidate_count = lexical_candidates.len();
    let semantic_candidate_count = semantic_candidates.len();
    let mut candidates = match effective_mode {
        RetrievalMode::Lexical => lexical_candidates,
        RetrievalMode::Semantic => semantic_candidates,
        RetrievalMode::Hybrid => fuse_rrf_candidates(
            &lexical_candidates,
            &semantic_candidates,
            args.rrf_k,
            args.fusion,
        )?,
    };
    let fused_candidate_count = candidates.len();

    sort_candidates(&mut candidates);
    if candidates.len() > limit {
        candidates.truncate(limit);
    }
    enforce_timeout(timeout_budget, "candidate ranking")?;

    let results = to_results(
        &connection,
        query_text,
        candidates,
        args.with_ancestors,
        args.with_descendants,
        args.with_pinpoint,
        pinpoint_max_units,
    )?;
    enforce_timeout(timeout_budget, "result hydration")?;

    let query_duration_ms = query_started.elapsed().as_secs_f64() * 1000.0;

    let retrieval_metadata = RetrievalMetadata {
        requested_mode: retrieval_mode_label(requested_mode).to_string(),
        effective_mode: retrieval_mode_label(effective_mode).to_string(),
        lexical_k,
        semantic_k,
        lexical_candidate_count,
        semantic_candidate_count,
        fused_candidate_count,
        fusion: fusion_mode_label(args.fusion).to_string(),
        rrf_k: args.rrf_k,
        semantic_model_id,
        exact_intent,
        exact_intent_forced_lexical,
        fallback_used,
        fallback_reason,
        pinpoint_enabled: args.with_pinpoint,
        pinpoint_max_units,
        timeout_ms: args.timeout_ms,
        query_duration_ms,
    };

    info!(
        query = %query_text,
        requested_mode = %retrieval_metadata.requested_mode,
        effective_mode = %retrieval_metadata.effective_mode,
        part_filter = ?args.part,
        chunk_type_filter = ?chunk_type_filter,
        node_type_filter = ?node_type_filter,
        lexical_candidate_count,
        semantic_candidate_count,
        fused_candidate_count,
        query_duration_ms,
        result_count = results.len(),
        "query completed"
    );

    if args.json {
        write_json_response(
            query_text,
            limit,
            args.part,
            chunk_type_filter,
            node_type_filter,
            retrieval_metadata,
            results,
        )?;
    } else {
        write_text_response(query_text, &retrieval_metadata, &results)?;
    }

    Ok(())
}

fn retrieval_mode_label(value: RetrievalMode) -> &'static str {
    match value {
        RetrievalMode::Lexical => "lexical",
        RetrievalMode::Semantic => "semantic",
        RetrievalMode::Hybrid => "hybrid",
    }
}

fn fusion_mode_label(value: FusionMode) -> &'static str {
    match value {
        FusionMode::Rrf => "rrf",
    }
}

fn clamp_candidates(value: usize) -> usize {
    value.clamp(1, MAX_QUERY_CANDIDATES)
}

#[derive(Clone, Copy)]
pub(super) struct QueryTimeoutBudget {
    started: Instant,
    timeout_ms: u64,
}

impl QueryTimeoutBudget {
    fn new(timeout_ms: u64) -> Option<Self> {
        if timeout_ms == 0 {
            return None;
        }
        Some(Self {
            started: Instant::now(),
            timeout_ms,
        })
    }

    fn elapsed_ms(self) -> f64 {
        self.started.elapsed().as_secs_f64() * 1000.0
    }

    fn enforce(self, stage: &str) -> Result<()> {
        let elapsed_ms = self.elapsed_ms();
        if elapsed_ms <= self.timeout_ms as f64 {
            return Ok(());
        }

        bail!(
            "query timeout exceeded during {} (elapsed {:.1} ms > budget {} ms); reduce --lexical-k/--semantic-k, narrow filters, or increase --timeout-ms",
            stage,
            elapsed_ms,
            self.timeout_ms
        )
    }
}

pub(super) fn enforce_timeout(
    timeout_budget: Option<QueryTimeoutBudget>,
    stage: &str,
) -> Result<()> {
    if let Some(timeout_budget) = timeout_budget {
        timeout_budget.enforce(stage)?;
    }
    Ok(())
}

pub(super) fn sort_candidates(candidates: &mut [QueryCandidate]) {
    candidates.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then(left.part.cmp(&right.part))
            .then(
                left.page_pdf_start
                    .unwrap_or(i64::MAX)
                    .cmp(&right.page_pdf_start.unwrap_or(i64::MAX)),
            )
            .then(left.chunk_id.cmp(&right.chunk_id))
    });
}
