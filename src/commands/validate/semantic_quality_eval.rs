fn compute_semantic_eval_quality(
    connection: &Connection,
    manifest_dir: &Path,
    run_id: &str,
    stage: Wp2GateStage,
    refs: &[GoldReference],
    semantic_embeddings: &SemanticEmbeddingReport,
    eval_manifest: &SemanticEvalManifest,
) -> Result<SemanticEvalComputation> {
    let active_model_id = if semantic_embeddings.active_model_id.trim().is_empty() {
        DEFAULT_MODEL_ID.to_string()
    } else {
        semantic_embeddings.active_model_id.trim().to_string()
    };
    let embedding_dim = semantic_embeddings
        .embedding_dim
        .unwrap_or(crate::semantic::DEFAULT_EMBEDDING_DIM);

    let mut query_results = Vec::<SemanticQualityQueryResult>::new();
    let mut lexical_ndcgs = Vec::<f64>::new();
    let mut semantic_ndcgs = Vec::<f64>::new();
    let mut hybrid_ndcgs = Vec::<f64>::new();
    let mut exact_top1_hits = Vec::<f64>::new();
    let mut citation_parity_hits = Vec::<f64>::new();
    let mut manifest_exact_top1_hits = Vec::<f64>::new();
    let mut manifest_citation_parity_hits = Vec::<f64>::new();
    let mut lexical_latencies = Vec::<f64>::new();
    let mut hybrid_latencies = Vec::<f64>::new();
    let mut determinism_scores = Vec::<f64>::new();
    let mut first_hit_rr_scores = Vec::<f64>::new();
    let mut lexical_recall_scores = Vec::<f64>::new();
    let mut hybrid_recall_scores = Vec::<f64>::new();
    let mut judged_at_10_scores = Vec::<f64>::new();
    let mut paired_ndcg_deltas = Vec::<f64>::new();
    let mut non_exact_queries = 0usize;
    let exact_queries;
    let mut manifest_exact_queries = 0usize;

    for query in &eval_manifest.queries {
        let eval = evaluate_semantic_query(
            connection,
            query,
            &active_model_id,
            embedding_dim,
            SEMANTIC_RETRIEVAL_LIMIT,
        )?;

        let is_exact = query.intent.eq_ignore_ascii_case("exact_ref") || query.must_hit_top1;
        if is_exact {
            manifest_exact_queries += 1;
            if let Some(hit) = eval.exact_top1_hit_hybrid {
                manifest_exact_top1_hits.push(if hit { 1.0 } else { 0.0 });
            }
            if let Some(hit) = eval.citation_top1_match_lexical_vs_hybrid {
                manifest_citation_parity_hits.push(if hit { 1.0 } else { 0.0 });
            }
        } else {
            non_exact_queries += 1;
            if let Some(value) = eval.lexical_ndcg {
                lexical_ndcgs.push(value);
            }
            if let Some(value) = eval.semantic_ndcg {
                semantic_ndcgs.push(value);
            }
            if let Some(value) = eval.hybrid_ndcg {
                hybrid_ndcgs.push(value);
            }
            if let (Some(hybrid), Some(lexical)) = (eval.hybrid_ndcg, eval.lexical_ndcg) {
                paired_ndcg_deltas.push(hybrid - lexical);
            }
        }

        if is_first_hit_intent(&query.intent) && let Some(value) = eval.hybrid_rr_at_10 {
            first_hit_rr_scores.push(value);
        }
        if let Some(value) = eval.lexical_recall_at_50 {
            lexical_recall_scores.push(value);
        }
        if let Some(value) = eval.hybrid_recall_at_50 {
            hybrid_recall_scores.push(value);
        }
        if let Some(value) = eval.judged_at_10 {
            judged_at_10_scores.push(value);
        }

        lexical_latencies.push(eval.lexical_latency_ms);
        hybrid_latencies.push(eval.hybrid_latency_ms);
        if let Some(value) = eval.determinism_top10_overlap {
            determinism_scores.push(value);
        }

        query_results.push(SemanticQualityQueryResult {
            query_id: query.query_id.clone(),
            intent: query.intent.clone(),
            query_text: query.query_text.clone(),
            expected_chunk_ids: query.expected_chunk_ids.clone(),
            lexical_top_chunk_ids: eval
                .lexical_hits
                .iter()
                .take(SEMANTIC_TOP_K)
                .map(|hit| hit.chunk_id.clone())
                .collect(),
            semantic_top_chunk_ids: eval
                .semantic_hits
                .iter()
                .take(SEMANTIC_TOP_K)
                .map(|hit| hit.chunk_id.clone())
                .collect(),
            hybrid_top_chunk_ids: eval
                .hybrid_hits
                .iter()
                .take(SEMANTIC_TOP_K)
                .map(|hit| hit.chunk_id.clone())
                .collect(),
            lexical_ndcg_at_10: eval.lexical_ndcg,
            semantic_ndcg_at_10: eval.semantic_ndcg,
            hybrid_ndcg_at_10: eval.hybrid_ndcg,
            hybrid_rr_at_10: eval.hybrid_rr_at_10,
            lexical_recall_at_50: eval.lexical_recall_at_50,
            hybrid_recall_at_50: eval.hybrid_recall_at_50,
            judged_at_10: eval.judged_at_10,
            lexical_latency_ms: eval.lexical_latency_ms,
            semantic_latency_ms: eval.semantic_latency_ms,
            hybrid_latency_ms: eval.hybrid_latency_ms,
            exact_top1_hit_hybrid: eval.exact_top1_hit_hybrid,
            citation_top1_match_lexical_vs_hybrid: eval.citation_top1_match_lexical_vs_hybrid,
            determinism_top10_overlap: eval.determinism_top10_overlap,
        });
    }

    let exact_probe_evals = evaluate_exact_intent_probes(
        connection,
        refs,
        &active_model_id,
        embedding_dim,
        SEMANTIC_RETRIEVAL_LIMIT,
    )?;
    if !exact_probe_evals.is_empty() {
        exact_queries = exact_probe_evals.len();
        for probe in exact_probe_evals {
            if let Some(hit) = probe.eval.exact_top1_hit_hybrid {
                exact_top1_hits.push(if hit { 1.0 } else { 0.0 });
            }
            if let Some(hit) = probe.eval.citation_top1_match_lexical_vs_hybrid {
                citation_parity_hits.push(if hit { 1.0 } else { 0.0 });
            }

            query_results.push(SemanticQualityQueryResult {
                query_id: probe.query.query_id,
                intent: "exact_ref_probe".to_string(),
                query_text: probe.query.query_text,
                expected_chunk_ids: probe.query.expected_chunk_ids,
                lexical_top_chunk_ids: probe
                    .eval
                    .lexical_hits
                    .iter()
                    .take(SEMANTIC_TOP_K)
                    .map(|hit| hit.chunk_id.clone())
                    .collect(),
                semantic_top_chunk_ids: probe
                    .eval
                    .semantic_hits
                    .iter()
                    .take(SEMANTIC_TOP_K)
                    .map(|hit| hit.chunk_id.clone())
                    .collect(),
                hybrid_top_chunk_ids: probe
                    .eval
                    .hybrid_hits
                    .iter()
                    .take(SEMANTIC_TOP_K)
                    .map(|hit| hit.chunk_id.clone())
                    .collect(),
                lexical_ndcg_at_10: probe.eval.lexical_ndcg,
                semantic_ndcg_at_10: probe.eval.semantic_ndcg,
                hybrid_ndcg_at_10: probe.eval.hybrid_ndcg,
                hybrid_rr_at_10: probe.eval.hybrid_rr_at_10,
                lexical_recall_at_50: probe.eval.lexical_recall_at_50,
                hybrid_recall_at_50: probe.eval.hybrid_recall_at_50,
                judged_at_10: probe.eval.judged_at_10,
                lexical_latency_ms: probe.eval.lexical_latency_ms,
                semantic_latency_ms: probe.eval.semantic_latency_ms,
                hybrid_latency_ms: probe.eval.hybrid_latency_ms,
                exact_top1_hit_hybrid: probe.eval.exact_top1_hit_hybrid,
                citation_top1_match_lexical_vs_hybrid: probe
                    .eval
                    .citation_top1_match_lexical_vs_hybrid,
                determinism_top10_overlap: probe.eval.determinism_top10_overlap,
            });
        }
    } else {
        exact_queries = manifest_exact_queries;
        exact_top1_hits = manifest_exact_top1_hits;
        citation_parity_hits = manifest_citation_parity_hits;
    }

    let lexical_ndcg_at_10 = mean(&lexical_ndcgs);
    let semantic_ndcg_at_10 = mean(&semantic_ndcgs);
    let hybrid_ndcg_at_10 = mean(&hybrid_ndcgs);
    let hybrid_ndcg_uplift_vs_lexical = match (hybrid_ndcg_at_10, lexical_ndcg_at_10) {
        (Some(hybrid), Some(lexical)) => Some(hybrid - lexical),
        _ => None,
    };
    let hybrid_mrr_at_10_first_hit = mean(&first_hit_rr_scores);
    let lexical_recall_at_50 = mean(&lexical_recall_scores);
    let hybrid_recall_at_50 = mean(&hybrid_recall_scores);
    let hybrid_recall_at_50_delta_vs_lexical = match (hybrid_recall_at_50, lexical_recall_at_50) {
        (Some(hybrid), Some(lexical)) => Some(hybrid - lexical),
        _ => None,
    };
    let judged_at_10_label_completeness = mean(&judged_at_10_scores);
    let ndcg_uplift_p_value = sign_test_two_sided_p_value(&paired_ndcg_deltas);
    let (ndcg_uplift_bootstrap_ci_low, ndcg_uplift_bootstrap_ci_high) =
        bootstrap_confidence_interval_95(&paired_ndcg_deltas, 2000, 0xA5A5_1337_u64)
            .unwrap_or((None, None));

    let lexical_p95_latency_ms = percentile(&lexical_latencies, 0.95);
    let hybrid_p95_latency_ms = percentile(&hybrid_latencies, 0.95);
    let latency_ratio_vs_lexical = match (hybrid_p95_latency_ms, lexical_p95_latency_ms) {
        (Some(hybrid), Some(lexical)) if lexical > 0.0 => Some(hybrid / lexical),
        _ => None,
    };

    let mut summary = SemanticQualitySummaryReport {
        source_eval_manifest: Some(
            manifest_dir
                .join(SEMANTIC_EVAL_MANIFEST_FILENAME)
                .display()
                .to_string(),
        ),
        quality_report_path: Some(
            manifest_dir
                .join(SEMANTIC_QUALITY_REPORT_FILENAME)
                .display()
                .to_string(),
        ),
        active_model_id: Some(active_model_id.clone()),
        total_queries: eval_manifest.queries.len(),
        non_exact_queries,
        exact_queries,
        semantic_ndcg_at_10,
        lexical_ndcg_at_10,
        hybrid_ndcg_at_10,
        hybrid_ndcg_uplift_vs_lexical,
        exact_ref_top1_hit_rate: mean(&exact_top1_hits),
        citation_parity_top1: mean(&citation_parity_hits),
        lexical_p95_latency_ms,
        hybrid_p95_latency_ms,
        latency_ratio_vs_lexical,
        retrieval_determinism_topk_overlap: mean(&determinism_scores),
        hybrid_mrr_at_10_first_hit,
        lexical_recall_at_50,
        hybrid_recall_at_50,
        hybrid_recall_at_50_delta_vs_lexical,
        judged_at_10_label_completeness,
        ndcg_uplift_p_value,
        ndcg_uplift_bootstrap_ci_low,
        ndcg_uplift_bootstrap_ci_high,
        baseline_path: String::new(),
        baseline_mode: String::new(),
        baseline_run_id: None,
        baseline_checksum: None,
        baseline_created: false,
        baseline_missing: false,
        warnings: Vec::new(),
    };

    if summary.non_exact_queries == 0 {
        summary.warnings.push(
            "semantic eval manifest has no non-exact queries; Q-033/Q-034 metrics are pending"
                .to_string(),
        );
    }
    if summary.exact_queries == 0 {
        summary.warnings.push(
            "exact intent probe set is empty; Q-035/Q-036 metrics are pending"
                .to_string(),
        );
    } else if manifest_exact_queries > 0 && exact_queries == manifest_exact_queries {
        summary.warnings.push(
            "exact intent probes were unavailable; fell back to semantic_eval exact_ref rows for Q-035/Q-036"
                .to_string(),
        );
    }
    if first_hit_rr_scores.is_empty() {
        summary.warnings.push(
            "semantic eval manifest has no first-hit intents (exact_ref/keyword/table_intent); Q-045 is pending"
                .to_string(),
        );
    }
    if lexical_recall_scores.is_empty() || hybrid_recall_scores.is_empty() {
        summary.warnings.push(
            "semantic eval manifest has no recall-eligible labels; Q-046 is pending".to_string(),
        );
    }
    if judged_at_10_scores.is_empty() {
        summary.warnings.push(
            "semantic eval manifest has no judged_chunk_ids labels; Q-047 is pending"
                .to_string(),
        );
    }
    if ndcg_uplift_p_value.is_none()
        || ndcg_uplift_bootstrap_ci_low.is_none()
        || ndcg_uplift_bootstrap_ci_high.is_none()
    {
        summary.warnings.push(
            "paired non-exact nDCG deltas are insufficient for Q-048 significance/CI"
                .to_string(),
        );
    }

    let mut query_ids = query_results
        .iter()
        .map(|result| result.query_id.clone())
        .collect::<Vec<String>>();
    query_ids.sort();
    query_ids.dedup();
    let baseline = apply_semantic_retrieval_baseline_governance(
        stage,
        run_id,
        semantic_embeddings,
        &summary,
        &query_ids,
    )?;
    summary.baseline_path = baseline.baseline_path;
    summary.baseline_mode = baseline.baseline_mode;
    summary.baseline_run_id = baseline.baseline_run_id;
    summary.baseline_checksum = baseline.baseline_checksum;
    summary.baseline_created = baseline.baseline_created;
    summary.baseline_missing = baseline.baseline_missing;
    summary.warnings.extend(baseline.warnings);

    let artifact_path = manifest_dir.join(SEMANTIC_QUALITY_REPORT_FILENAME);
    let artifact = SemanticQualityArtifact {
        manifest_version: 1,
        run_id: run_id.to_string(),
        generated_at: now_utc_string(),
        source_eval_manifest: manifest_dir
            .join(SEMANTIC_EVAL_MANIFEST_FILENAME)
            .display()
            .to_string(),
        active_model_id: Some(active_model_id),
        summary: summary.clone(),
        query_results,
    };
    write_json_pretty(&artifact_path, &artifact)?;

    Ok(SemanticEvalComputation { summary })
}

fn evaluate_semantic_query(
    connection: &Connection,
    query: &SemanticEvalQuery,
    model_id: &str,
    embedding_dim: usize,
    retrieval_limit: usize,
) -> Result<QueryEvalRecord> {
    let exact_intent_priority = query.must_hit_top1
        || query.intent.eq_ignore_ascii_case("exact_ref")
        || query.intent.eq_ignore_ascii_case("exact_ref_probe");

    let lexical_started = std::time::Instant::now();
    let lexical_hits = semantic_eval_lexical_hits(
        connection,
        &query.query_text,
        query.part_filter,
        query.chunk_type_filter.as_deref(),
        retrieval_limit,
    )?;
    let lexical_latency_ms = lexical_started.elapsed().as_secs_f64() * 1000.0;

    let semantic_started = std::time::Instant::now();
    let semantic_hits = semantic_eval_semantic_hits(
        connection,
        &query.query_text,
        query.part_filter,
        query.chunk_type_filter.as_deref(),
        model_id,
        embedding_dim,
        retrieval_limit,
    )?;
    let semantic_latency_ms = semantic_started.elapsed().as_secs_f64() * 1000.0;

    let hybrid_started = std::time::Instant::now();
    let hybrid_hits = semantic_eval_hybrid_hits(
        connection,
        &query.query_text,
        query.part_filter,
        query.chunk_type_filter.as_deref(),
        model_id,
        embedding_dim,
        retrieval_limit,
        exact_intent_priority,
    )?;
    let hybrid_latency_ms = hybrid_started.elapsed().as_secs_f64() * 1000.0;

    let hybrid_repeat_hits = semantic_eval_hybrid_hits(
        connection,
        &query.query_text,
        query.part_filter,
        query.chunk_type_filter.as_deref(),
        model_id,
        embedding_dim,
        retrieval_limit,
        exact_intent_priority,
    )?;

    let expected = query
        .expected_chunk_ids
        .iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<HashSet<String>>();
    let judged = query
        .judged_chunk_ids
        .iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<HashSet<String>>();

    let lexical_ids = lexical_hits
        .iter()
        .map(|hit| hit.chunk_id.clone())
        .collect::<Vec<String>>();
    let semantic_ids = semantic_hits
        .iter()
        .map(|hit| hit.chunk_id.clone())
        .collect::<Vec<String>>();
    let hybrid_ids = hybrid_hits
        .iter()
        .map(|hit| hit.chunk_id.clone())
        .collect::<Vec<String>>();

    let lexical_ndcg = ndcg_at_k(&lexical_ids, &expected, SEMANTIC_TOP_K);
    let semantic_ndcg = ndcg_at_k(&semantic_ids, &expected, SEMANTIC_TOP_K);
    let hybrid_ndcg = ndcg_at_k(&hybrid_ids, &expected, SEMANTIC_TOP_K);
    let hybrid_rr_at_10 = reciprocal_rank_at_k(&hybrid_ids, &expected, SEMANTIC_TOP_K);
    let lexical_recall_at_50 = recall_at_k(&lexical_ids, &expected, 50);
    let hybrid_recall_at_50 = recall_at_k(&hybrid_ids, &expected, 50);
    let judged_at_10 = judged_at_k(&hybrid_ids, &judged, SEMANTIC_TOP_K);

    let exact_top1_hit_hybrid =
        if query.intent.eq_ignore_ascii_case("exact_ref") || query.must_hit_top1 {
            Some(
                hybrid_ids
                    .first()
                    .map(|chunk_id| expected.contains(chunk_id))
                    .unwrap_or(false),
            )
        } else {
            None
        };

    let citation_top1_match_lexical_vs_hybrid = match (lexical_hits.first(), hybrid_hits.first()) {
        (Some(left), Some(right)) => {
            Some(semantic_hit_identity(left) == semantic_hit_identity(right))
        }
        _ => None,
    };

    let determinism_top10_overlap = top_k_jaccard_overlap(&hybrid_hits, &hybrid_repeat_hits, 10);

    Ok(QueryEvalRecord {
        lexical_hits,
        semantic_hits,
        hybrid_hits,
        lexical_ndcg,
        semantic_ndcg,
        hybrid_ndcg,
        hybrid_rr_at_10,
        lexical_recall_at_50,
        hybrid_recall_at_50,
        judged_at_10,
        lexical_latency_ms,
        semantic_latency_ms,
        hybrid_latency_ms,
        exact_top1_hit_hybrid,
        citation_top1_match_lexical_vs_hybrid,
        determinism_top10_overlap,
    })
}
