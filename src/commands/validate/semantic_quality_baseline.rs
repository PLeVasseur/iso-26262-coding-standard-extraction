use super::*;

pub const WP3_SEMANTIC_BASELINE_MODE_ENV: &str = "WP3_SEMANTIC_BASELINE_MODE";
pub const WP3_SEMANTIC_BASELINE_PATH_ENV: &str = "WP3_SEMANTIC_BASELINE_PATH";
pub const WP3_SEMANTIC_BASELINE_DECISION_ENV: &str = "WP3_SEMANTIC_BASELINE_DECISION_ID";
pub const WP3_SEMANTIC_BASELINE_REASON_ENV: &str = "WP3_SEMANTIC_BASELINE_REASON";

#[derive(Debug)]
pub struct SemanticBaselineComputation {
    pub baseline_path: String,
    pub baseline_mode: String,
    pub baseline_run_id: Option<String>,
    pub baseline_checksum: Option<String>,
    pub baseline_created: bool,
    pub baseline_missing: bool,
    pub warnings: Vec<String>,
}

pub fn apply_semantic_retrieval_baseline_governance(
    stage: Wp2GateStage,
    run_id: &str,
    semantic_embeddings: &SemanticEmbeddingReport,
    summary: &SemanticQualitySummaryReport,
    query_ids: &[String],
) -> Result<SemanticBaselineComputation> {
    let baseline_mode = resolve_semantic_baseline_mode();
    let baseline_path = resolve_semantic_baseline_path();
    if stage == Wp2GateStage::B && baseline_mode == SemanticBaselineMode::Bootstrap {
        bail!(
            "{}=bootstrap is not allowed with WP2_GATE_STAGE=B; run Stage A to bootstrap/rotate {}",
            WP3_SEMANTIC_BASELINE_MODE_ENV,
            baseline_path.display()
        );
    }

    let check_ids = semantic_retrieval_check_ids();
    let mut normalized_query_ids = query_ids
        .iter()
        .map(|query_id| query_id.trim().to_string())
        .filter(|query_id| !query_id.is_empty())
        .collect::<Vec<String>>();
    normalized_query_ids.sort();
    normalized_query_ids.dedup();

    let thresholds = semantic_retrieval_thresholds();
    let summary_metrics = semantic_retrieval_metrics(semantic_embeddings, summary);
    let checksum = checksum_semantic_baseline_payload(
        &check_ids,
        &normalized_query_ids,
        &thresholds,
        &summary_metrics,
    )?;

    let mut baseline_created = false;
    let mut baseline_missing = false;
    let mut warnings = Vec::<String>::new();
    let (decision_id, change_reason) = resolve_semantic_baseline_rationale();

    let baseline = if baseline_mode == SemanticBaselineMode::Bootstrap {
        if baseline_path.exists() && (decision_id.is_none() || change_reason.is_none()) {
            bail!(
                "{}=bootstrap would rotate existing lockfile at {}; set both {} and {}",
                WP3_SEMANTIC_BASELINE_MODE_ENV,
                baseline_path.display(),
                WP3_SEMANTIC_BASELINE_DECISION_ENV,
                WP3_SEMANTIC_BASELINE_REASON_ENV
            );
        }

        baseline_created = true;
        let baseline = SemanticRetrievalBaseline {
            manifest_version: 1,
            run_id: run_id.to_string(),
            generated_at: now_utc_string(),
            db_schema_version: Some(DB_SCHEMA_VERSION.to_string()),
            decision_id,
            change_reason,
            check_ids,
            query_ids: normalized_query_ids.clone(),
            thresholds,
            summary_metrics,
            checksum,
        };
        write_semantic_retrieval_lockfile(&baseline_path, &baseline)?;
        Some(baseline)
    } else if baseline_path.exists() {
        Some(read_semantic_retrieval_lockfile(&baseline_path)?)
    } else {
        baseline_missing = true;
        None
    };

    if let Some(existing) = baseline.as_ref() {
        let mut current_check_ids = semantic_retrieval_check_ids();
        current_check_ids.sort();
        let mut baseline_check_ids = existing.check_ids.clone();
        baseline_check_ids.sort();
        if current_check_ids != baseline_check_ids {
            warnings.push(
                "semantic retrieval baseline check IDs differ from current validate set; consider rotating lockfile with rationale"
                    .to_string(),
            );
        }

        let mut baseline_query_ids = existing.query_ids.clone();
        baseline_query_ids.sort();
        if baseline_query_ids != normalized_query_ids {
            warnings.push(
                "semantic retrieval baseline query IDs differ from current evaluation set; review label drift before Stage B promotion"
                    .to_string(),
            );
        }

        if existing.thresholds != semantic_retrieval_thresholds() {
            warnings.push(
                "semantic retrieval baseline thresholds differ from current check thresholds; rotate lockfile after documenting rationale"
                    .to_string(),
            );
        }
    } else {
        warnings.push(
            "semantic retrieval baseline lockfile is missing; run validate with WP3_SEMANTIC_BASELINE_MODE=bootstrap to create manifests/semantic_retrieval_baseline.lock.json"
                .to_string(),
        );
    }

    Ok(SemanticBaselineComputation {
        baseline_path: baseline_path.display().to_string(),
        baseline_mode: baseline_mode.as_str().to_string(),
        baseline_run_id: baseline.as_ref().map(|value| value.run_id.clone()),
        baseline_checksum: baseline.as_ref().map(|value| value.checksum.clone()),
        baseline_created,
        baseline_missing,
        warnings,
    })
}

pub fn resolve_semantic_baseline_mode() -> SemanticBaselineMode {
    parse_semantic_baseline_mode(
        std::env::var(WP3_SEMANTIC_BASELINE_MODE_ENV)
            .ok()
            .as_deref(),
    )
}

pub fn parse_semantic_baseline_mode(value: Option<&str>) -> SemanticBaselineMode {
    match value {
        Some(value)
            if value.trim().eq_ignore_ascii_case("bootstrap")
                || value.trim().eq_ignore_ascii_case("rotate") =>
        {
            SemanticBaselineMode::Bootstrap
        }
        _ => SemanticBaselineMode::Verify,
    }
}

pub fn resolve_semantic_baseline_path() -> PathBuf {
    parse_semantic_baseline_path(
        std::env::var(WP3_SEMANTIC_BASELINE_PATH_ENV)
            .ok()
            .as_deref(),
    )
}

pub fn parse_semantic_baseline_path(value: Option<&str>) -> PathBuf {
    if let Some(value) = value {
        let candidate = value.trim();
        if !candidate.is_empty() {
            return PathBuf::from(candidate);
        }
    }
    PathBuf::from("manifests").join("semantic_retrieval_baseline.lock.json")
}

pub fn resolve_semantic_baseline_rationale() -> (Option<String>, Option<String>) {
    let decision_id = std::env::var(WP3_SEMANTIC_BASELINE_DECISION_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let reason = std::env::var(WP3_SEMANTIC_BASELINE_REASON_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    (decision_id, reason)
}

pub fn write_semantic_retrieval_lockfile(path: &Path, baseline: &SemanticRetrievalBaseline) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create lockfile directory {}", parent.display()))?;
    }
    write_json_pretty(path, baseline)
}

pub fn read_semantic_retrieval_lockfile(path: &Path) -> Result<SemanticRetrievalBaseline> {
    let raw = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let parsed = serde_json::from_slice::<serde_json::Value>(&raw)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    ensure_semantic_baseline_metadata_only(&parsed)?;
    serde_json::from_value::<SemanticRetrievalBaseline>(parsed)
        .with_context(|| format!("failed to decode {}", path.display()))
}

pub fn ensure_semantic_baseline_metadata_only(value: &serde_json::Value) -> Result<()> {
    const FORBIDDEN_KEYS: &[&str] = &[
        "text",
        "snippet",
        "heading",
        "chunk_text",
        "table_md",
        "table_csv",
        "raw_text",
        "content",
        "query_text",
        "expected_chunk_ids",
        "lexical_top_chunk_ids",
        "semantic_top_chunk_ids",
        "hybrid_top_chunk_ids",
    ];

    let mut stack = vec![("$".to_string(), value)];
    while let Some((path, node)) = stack.pop() {
        match node {
            serde_json::Value::Object(map) => {
                for (key, child) in map {
                    let lowered = key.to_ascii_lowercase();
                    if FORBIDDEN_KEYS.iter().any(|forbidden| *forbidden == lowered) {
                        bail!(
                            "semantic baseline lockfile contains forbidden text-bearing key '{}' at {}",
                            key,
                            path
                        );
                    }
                    stack.push((format!("{}.{}", path, key), child));
                }
            }
            serde_json::Value::Array(values) => {
                for (index, child) in values.iter().enumerate() {
                    stack.push((format!("{}[{}]", path, index), child));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

pub fn semantic_retrieval_check_ids() -> Vec<String> {
    [
        "Q-031", "Q-032", "Q-033", "Q-034", "Q-035", "Q-036", "Q-037", "Q-038",
        "Q-045", "Q-046", "Q-047", "Q-048",
    ]
    .into_iter()
    .map(|value| value.to_string())
    .collect()
}

pub fn semantic_retrieval_thresholds() -> SemanticRetrievalBaselineThresholds {
    SemanticRetrievalBaselineThresholds {
        q031_stage_a_min: WP3_EMBEDDING_COVERAGE_STAGE_A_MIN,
        q031_stage_b_min: WP3_EMBEDDING_COVERAGE_STAGE_B_MIN,
        q032_stage_a_max: WP3_STALE_EMBEDDING_STAGE_A_MAX,
        q032_stage_b_max: WP3_STALE_EMBEDDING_STAGE_B_MAX,
        q033_stage_a_min: WP3_SEMANTIC_NDCG_STAGE_A_MIN,
        q033_stage_b_min: WP3_SEMANTIC_NDCG_STAGE_B_MIN,
        q034_stage_a_min: WP3_HYBRID_NDCG_UPLIFT_STAGE_A_MIN,
        q034_stage_b_min: WP3_HYBRID_NDCG_UPLIFT_STAGE_B_MIN,
        q035_stage_a_min: WP3_EXACT_TOP1_MIN,
        q035_stage_b_min: WP3_EXACT_TOP1_MIN,
        q036_stage_a_min: WP3_CITATION_PARITY_MIN,
        q036_stage_b_min: WP3_CITATION_PARITY_MIN,
        q037_latency_ratio_stage_a_max: WP3_LATENCY_RATIO_MAX,
        q037_latency_ratio_stage_b_max: WP3_LATENCY_RATIO_MAX,
        q037_hybrid_p95_stage_b_max_ms: WP3_HYBRID_P95_MAX_MS,
        q038_stage_a_min: WP3_DETERMINISM_STAGE_A_MIN,
        q038_stage_b_min: WP3_DETERMINISM_STAGE_B_MIN,
        q045_stage_a_min: WP3_HYBRID_MRR_STAGE_A_MIN,
        q045_stage_b_min: WP3_HYBRID_MRR_STAGE_B_MIN,
        q046_stage_a_max_drop: WP3_RECALL_DROP_STAGE_A_MAX,
        q046_stage_b_max_drop: WP3_RECALL_DROP_STAGE_B_MAX,
        q047_stage_a_min: WP3_JUDGED_STAGE_A_MIN,
        q047_stage_b_min: WP3_JUDGED_STAGE_B_MIN,
        q048_stage_a_p_max: WP3_NDCG_UPLIFT_P_STAGE_A_MAX,
        q048_stage_b_p_max: WP3_NDCG_UPLIFT_P_STAGE_B_MAX,
    }
}

pub fn semantic_retrieval_metrics(
    semantic_embeddings: &SemanticEmbeddingReport,
    summary: &SemanticQualitySummaryReport,
) -> SemanticRetrievalBaselineMetrics {
    SemanticRetrievalBaselineMetrics {
        q031_chunk_embedding_coverage_ratio: semantic_embeddings.chunk_embedding_coverage_ratio,
        q032_stale_embedding_ratio: semantic_embeddings.stale_embedding_ratio,
        q033_semantic_ndcg_at_10: summary.semantic_ndcg_at_10,
        q034_hybrid_ndcg_uplift_vs_lexical: summary.hybrid_ndcg_uplift_vs_lexical,
        q035_exact_ref_top1_hit_rate: summary.exact_ref_top1_hit_rate,
        q036_citation_parity_top1: summary.citation_parity_top1,
        q037_hybrid_p95_latency_ms: summary.hybrid_p95_latency_ms,
        q037_latency_ratio_vs_lexical: summary.latency_ratio_vs_lexical,
        q038_retrieval_determinism_topk_overlap: summary.retrieval_determinism_topk_overlap,
        q045_hybrid_mrr_at_10_first_hit: summary.hybrid_mrr_at_10_first_hit,
        q046_hybrid_recall_at_50_delta_vs_lexical: summary.hybrid_recall_at_50_delta_vs_lexical,
        q047_judged_at_10_label_completeness: summary.judged_at_10_label_completeness,
        q048_ndcg_uplift_p_value: summary.ndcg_uplift_p_value,
        q048_ndcg_uplift_bootstrap_ci_low: summary.ndcg_uplift_bootstrap_ci_low,
        q048_ndcg_uplift_bootstrap_ci_high: summary.ndcg_uplift_bootstrap_ci_high,
    }
}

pub fn checksum_semantic_baseline_payload(
    check_ids: &[String],
    query_ids: &[String],
    thresholds: &SemanticRetrievalBaselineThresholds,
    summary_metrics: &SemanticRetrievalBaselineMetrics,
) -> Result<String> {
    let payload = serde_json::json!({
        "check_ids": check_ids,
        "query_ids": query_ids,
        "thresholds": thresholds,
        "summary_metrics": summary_metrics,
    });
    let bytes = serde_json::to_vec(&payload)?;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    Ok(format!("{:016x}", hasher.finish()))
}
