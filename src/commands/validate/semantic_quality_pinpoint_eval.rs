pub fn compute_pinpoint_quality(
    connection: &Connection,
    manifest_dir: &Path,
    run_id: &str,
    _stage: Wp2GateStage,
    semantic_embeddings: &SemanticEmbeddingReport,
    eval_manifest: &PinpointEvalManifest,
) -> Result<PinpointQualityArtifact> {
    let active_model_id = if semantic_embeddings.active_model_id.trim().is_empty() {
        DEFAULT_MODEL_ID.to_string()
    } else {
        semantic_embeddings.active_model_id.trim().to_string()
    };
    let embedding_dim = semantic_embeddings
        .embedding_dim
        .unwrap_or(crate::semantic::DEFAULT_EMBEDDING_DIM);

    let mut relevance_hits = Vec::<f64>::new();
    let mut table_hits = Vec::<f64>::new();
    let mut mismatch_count = 0usize;
    let mut high_confidence_total = 0usize;
    let mut fallback_total = 0usize;
    let mut determinism_hits = Vec::<f64>::new();
    let mut overhead_ms = Vec::<f64>::new();
    let mut query_results = Vec::<PinpointQualityQueryResult>::new();
    let mut table_queries = 0usize;

    for query in &eval_manifest.queries {
        let exact_intent_priority = query.intent.eq_ignore_ascii_case("requirement");

        let baseline_started = std::time::Instant::now();
        semantic_eval_hybrid_hits(
            connection,
            &query.query_text,
            query.part_filter,
            query.chunk_type_filter.as_deref(),
            &active_model_id,
            embedding_dim,
            SEMANTIC_RETRIEVAL_LIMIT,
            exact_intent_priority,
        )?;
        let latency_without_pinpoint_ms = baseline_started.elapsed().as_secs_f64() * 1000.0;

        let pinpoint_started = std::time::Instant::now();
        let pinpoint_hits = semantic_eval_hybrid_hits(
            connection,
            &query.query_text,
            query.part_filter,
            query.chunk_type_filter.as_deref(),
            &active_model_id,
            embedding_dim,
            SEMANTIC_RETRIEVAL_LIMIT,
            exact_intent_priority,
        )?;
        let retrieved_parent_chunk_id = pinpoint_hits.first().map(|hit| hit.chunk_id.clone());
        let retrieved_parent_anchor_id = pinpoint_hits
            .first()
            .and_then(|hit| hit.citation_anchor_id.clone());
        let parent_chunk_id =
            select_pinpoint_parent_chunk(query, retrieved_parent_chunk_id.as_deref());
        let parent_anchor_id = match parent_chunk_id.as_deref() {
            Some(chunk_id) if Some(chunk_id) == retrieved_parent_chunk_id.as_deref() => {
                retrieved_parent_anchor_id.clone()
            }
            Some(chunk_id) => resolve_chunk_anchor_id(connection, chunk_id)?,
            None => None,
        };
        let pinpoint_eval = if let Some(chunk_id) = parent_chunk_id.as_deref() {
            evaluate_pinpoint_for_chunk(
                connection,
                chunk_id,
                parent_anchor_id.as_deref(),
                &query.query_text,
            )?
        } else {
            PinpointQueryEval {
                top_unit: None,
                fallback_used: true,
            }
        };
        let latency_with_pinpoint_ms = pinpoint_started.elapsed().as_secs_f64() * 1000.0;

        let latency_overhead_ms = (latency_with_pinpoint_ms - latency_without_pinpoint_ms).max(0.0);
        overhead_ms.push(latency_overhead_ms);

        let deterministic_repeat = if let Some(chunk_id) = parent_chunk_id.as_deref() {
            let repeated = evaluate_pinpoint_for_chunk(
                connection,
                chunk_id,
                parent_anchor_id.as_deref(),
                &query.query_text,
            )?;
            match (&pinpoint_eval.top_unit, &repeated.top_unit) {
                (Some(left), Some(right)) => Some(
                    left.unit_id == right.unit_id && left.token_signature == right.token_signature,
                ),
                (None, None) => Some(true),
                _ => Some(false),
            }
        } else {
            None
        };
        if let Some(value) = deterministic_repeat {
            determinism_hits.push(if value { 1.0 } else { 0.0 });
        }

        if query.high_confidence {
            high_confidence_total += 1;
            if pinpoint_eval.fallback_used {
                fallback_total += 1;
            }
        }

        let top_unit = pinpoint_eval.top_unit.as_ref();
        let top_tokens = top_unit
            .map(|unit| tokenize_pinpoint_value(&unit.text_preview))
            .unwrap_or_default();
        let relevance_hit = top_unit.map(|unit| {
            let expected_unit_hit = query
                .expected_unit_ids
                .iter()
                .any(|value| value == &unit.unit_id);
            let token_set_hit = query.expected_token_sets.iter().any(|tokens| {
                !tokens.is_empty()
                    && tokens
                        .iter()
                        .all(|token| top_tokens.iter().any(|value| value == token))
            });
            expected_unit_hit || token_set_hit
        });
        if let Some(hit) = relevance_hit {
            relevance_hits.push(if hit { 1.0 } else { 0.0 });
        }

        let row_accuracy = if query.expected_row_keys.is_empty() {
            None
        } else {
            table_queries += 1;
            Some(
                top_unit
                    .and_then(|unit| unit.row_key.as_ref())
                    .map(|row_key| query.expected_row_keys.iter().any(|value| value == row_key))
                    .unwrap_or(false),
            )
        };
        if let Some(hit) = row_accuracy {
            table_hits.push(if hit { 1.0 } else { 0.0 });
        }

        let anchor_consistent = top_unit.map(|unit| unit.citation_anchor_compatible);
        if anchor_consistent == Some(false) {
            mismatch_count += 1;
        }

        query_results.push(PinpointQualityQueryResult {
            query_id: query.query_id.clone(),
            intent: query.intent.clone(),
            query_text: query.query_text.clone(),
            parent_chunk_id,
            top_unit_id: top_unit.map(|unit| unit.unit_id.clone()),
            top_unit_type: top_unit.map(|unit| unit.unit_type.clone()),
            top_unit_text: top_unit.map(|unit| unit.text_preview.clone()),
            top_row_key: top_unit.and_then(|unit| unit.row_key.clone()),
            top_unit_score: top_unit.map(|unit| unit.score),
            relevance_hit_at_1: relevance_hit,
            row_accuracy_hit_at_1: row_accuracy,
            citation_anchor_compatible: anchor_consistent,
            fallback_used: pinpoint_eval.fallback_used,
            determinism_top1_match: deterministic_repeat,
            latency_without_pinpoint_ms,
            latency_with_pinpoint_ms,
            latency_overhead_ms,
        });
    }

    let mut summary = PinpointQualitySummary {
        source_eval_manifest: Some(
            manifest_dir
                .join(PINPOINT_EVAL_MANIFEST_FILENAME)
                .display()
                .to_string(),
        ),
        quality_report_path: Some(
            manifest_dir
                .join(PINPOINT_QUALITY_REPORT_FILENAME)
                .display()
                .to_string(),
        ),
        total_queries: eval_manifest.queries.len(),
        table_queries,
        high_confidence_queries: high_confidence_total,
        pinpoint_at_1_relevance: mean(&relevance_hits),
        table_row_accuracy_at_1: mean(&table_hits),
        citation_anchor_mismatch_count: mismatch_count,
        fallback_ratio: ratio(fallback_total, high_confidence_total),
        determinism_top1: mean(&determinism_hits),
        latency_overhead_p95_ms: percentile(&overhead_ms, 0.95),
        warnings: Vec::new(),
    };

    if summary.total_queries == 0 {
        summary
            .warnings
            .push("pinpoint eval manifest has no queries; Q-039..Q-044 are pending".to_string());
    }
    if summary.table_queries == 0 {
        summary
            .warnings
            .push("pinpoint eval manifest has no table intents; Q-040 is pending".to_string());
    }
    if summary.high_confidence_queries == 0 {
        summary.warnings.push(
            "pinpoint eval manifest has no high_confidence queries; Q-042 is pending".to_string(),
        );
    }

    let artifact = PinpointQualityArtifact {
        manifest_version: 1,
        run_id: run_id.to_string(),
        generated_at: now_utc_string(),
        source_eval_manifest: manifest_dir
            .join(PINPOINT_EVAL_MANIFEST_FILENAME)
            .display()
            .to_string(),
        summary,
        query_results,
    };
    write_json_pretty(
        &manifest_dir.join(PINPOINT_QUALITY_REPORT_FILENAME),
        &artifact,
    )?;
    Ok(artifact)
}

pub fn evaluate_pinpoint_for_chunk(
    connection: &Connection,
    chunk_id: &str,
    parent_anchor_id: Option<&str>,
    query_text: &str,
) -> Result<PinpointQueryEval> {
    let unit_candidates = collect_pinpoint_unit_candidates(connection, chunk_id, parent_anchor_id)?;
    if unit_candidates.is_empty() {
        return Ok(PinpointQueryEval {
            top_unit: None,
            fallback_used: true,
        });
    }

    let query_tokens = tokenize_pinpoint_value(query_text);
    let query_phrase = condense_whitespace(query_text).to_ascii_lowercase();
    let query_mentions_table = query_mentions_table_context(query_text);
    let query_is_table_reference = looks_like_table_reference_query(query_text);
    let mut scored = unit_candidates
        .into_iter()
        .map(|unit| {
            let unit_tokens = tokenize_pinpoint_value(&unit.text_preview);
            let overlap = token_overlap_score(&query_tokens, &unit_tokens);
            let phrase_bonus = if !query_phrase.is_empty()
                && query_phrase.len() >= 8
                && unit
                    .text_preview
                    .to_ascii_lowercase()
                    .contains(&query_phrase)
            {
                1.0
            } else {
                0.0
            };

            let mut score = overlap * 0.70 + phrase_bonus * 0.20;
            if overlap >= 0.50 {
                score += 0.10;
            }

            if query_is_table_reference {
                match unit.unit_type.as_str() {
                    "table_row" => score += 0.45,
                    "table_cell" => score += 0.20,
                    "sentence_window" => score -= 0.50,
                    _ => {}
                }
            } else if query_mentions_table {
                match unit.unit_type.as_str() {
                    "table_row" => score += 0.22,
                    "table_cell" => score += 0.12,
                    "sentence_window" => score -= 0.15,
                    _ => {}
                }
            }

            if !unit.citation_anchor_compatible {
                score -= 0.20;
            }
            if unit.text_preview.len() > 1200 {
                score -= 0.04;
            } else if unit.text_preview.len() > 700 {
                score -= 0.02;
            }

            PinpointUnitEval {
                unit_id: unit.unit_id,
                unit_type: unit.unit_type,
                score,
                text_preview: unit.text_preview,
                row_key: unit.row_key,
                token_signature: unit_tokens.join("|"),
                citation_anchor_compatible: unit.citation_anchor_compatible,
            }
        })
        .collect::<Vec<PinpointUnitEval>>();

    scored.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| {
                pinpoint_unit_priority(
                    &right.unit_type,
                    query_mentions_table,
                    query_is_table_reference,
                )
                .cmp(&pinpoint_unit_priority(
                    &left.unit_type,
                    query_mentions_table,
                    query_is_table_reference,
                ))
            })
            .then(left.unit_id.cmp(&right.unit_id))
    });
    scored.truncate(PINPOINT_UNIT_LIMIT);

    Ok(PinpointQueryEval {
        top_unit: scored.into_iter().next(),
        fallback_used: false,
    })
}

#[path = "semantic_quality_pinpoint_candidates.rs"]
mod semantic_quality_pinpoint_candidates;
#[path = "semantic_quality_pinpoint_utils.rs"]
mod semantic_quality_pinpoint_utils;

pub use self::semantic_quality_pinpoint_candidates::*;
pub use self::semantic_quality_pinpoint_utils::*;
use super::*;
