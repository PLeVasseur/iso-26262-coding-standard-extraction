fn resolve_wp2_gate_stage() -> Wp2GateStage {
    match std::env::var("WP2_GATE_STAGE") {
        Ok(value) if value.trim().eq_ignore_ascii_case("B") => Wp2GateStage::B,
        _ => Wp2GateStage::A,
    }
}
fn resolve_citation_baseline_mode() -> CitationBaselineMode {
    parse_citation_baseline_mode(
        std::env::var(WP2_CITATION_BASELINE_MODE_ENV)
            .ok()
            .as_deref(),
    )
}
fn resolve_citation_baseline_path() -> PathBuf {
    parse_citation_baseline_path(
        std::env::var(WP2_CITATION_BASELINE_PATH_ENV)
            .ok()
            .as_deref(),
    )
}
fn parse_citation_baseline_mode(value: Option<&str>) -> CitationBaselineMode {
    match value {
        Some(value)
            if value.trim().eq_ignore_ascii_case("bootstrap")
                || value.trim().eq_ignore_ascii_case("rotate") =>
        {
            CitationBaselineMode::Bootstrap
        }
        _ => CitationBaselineMode::Verify,
    }
}

fn parse_citation_baseline_path(value: Option<&str>) -> PathBuf {
    if let Some(value) = value {
        let candidate = value.trim();
        if !candidate.is_empty() {
            return PathBuf::from(candidate);
        }
    }

    PathBuf::from("manifests").join("citation_parity_baseline.lock.json")
}

#[allow(clippy::too_many_arguments)]
fn build_wp2_assessment(
    connection: &Connection,
    manifest_dir: &Path,
    run_id: &str,
    refs: &[GoldReference],
    stage: Wp2GateStage,
    citation_baseline_path: &Path,
    citation_baseline_mode: CitationBaselineMode,
    latest_snapshot: Option<&NamedIngestRunSnapshot>,
    previous_snapshot: Option<&NamedIngestRunSnapshot>,
) -> Result<Wp2Assessment> {
    let mut checks = Vec::<QualityCheck>::new();
    let mut recommendations = Vec::<String>::new();

    let mut extraction = ExtractionFidelityReport {
        source_manifest: latest_snapshot.map(|snapshot| snapshot.manifest_name.clone()),
        ..ExtractionFidelityReport::default()
    };
    let mut hierarchy = HierarchySemanticsReport::default();
    let mut table_semantics = TableSemanticsReport::default();
    let mut citation_parity = CitationParitySummaryReport {
        baseline_path: citation_baseline_path.display().to_string(),
        baseline_mode: citation_baseline_mode.as_str().to_string(),
        ..CitationParitySummaryReport::default()
    };
    let mut semantic_embeddings = SemanticEmbeddingReport::default();

    let latest_counts = latest_snapshot
        .map(|snapshot| snapshot.snapshot.counts.clone())
        .unwrap_or_default();

    extraction.processed_pages = latest_counts.text_layer_page_count + latest_counts.ocr_page_count;
    if extraction.processed_pages == 0 {
        extraction.processed_pages = latest_counts.empty_page_count;
    }
    extraction.ocr_page_ratio = ratio(latest_counts.ocr_page_count, extraction.processed_pages);

    let current_page_provenance =
        load_page_provenance_entries(manifest_dir, latest_snapshot).unwrap_or_default();
    let previous_page_provenance =
        load_page_provenance_entries(manifest_dir, previous_snapshot).unwrap_or_default();

    extraction.provenance_entries = current_page_provenance.len();
    extraction.provenance_coverage =
        ratio(extraction.provenance_entries, extraction.processed_pages);
    extraction.unknown_backend_pages = current_page_provenance
        .iter()
        .filter(|entry| !matches!(entry.backend.as_str(), "text_layer" | "ocr"))
        .count();

    extraction.text_layer_replay_stability = replay_stability_ratio(
        &current_page_provenance,
        &previous_page_provenance,
        "text_layer",
    );
    extraction.ocr_replay_stability =
        replay_stability_ratio(&current_page_provenance, &previous_page_provenance, "ocr");

    let printed_metrics = compute_printed_page_metrics(connection, &current_page_provenance)?;
    extraction.total_chunks = printed_metrics.total_chunks;
    extraction.printed_mapped_chunks = printed_metrics.mapped_chunks;
    extraction.printed_mapping_coverage =
        ratio(printed_metrics.mapped_chunks, printed_metrics.total_chunks);
    extraction.printed_status_coverage = ratio(
        printed_metrics.pages_with_explicit_status,
        printed_metrics.total_pages,
    );
    extraction.printed_detectability_rate = ratio(
        printed_metrics.detectable_pages,
        printed_metrics.total_pages,
    );
    extraction.printed_mapping_on_detectable = ratio(
        printed_metrics.mapped_detectable_chunks,
        printed_metrics.detectable_chunks,
    );
    extraction.invalid_printed_label_count = printed_metrics.invalid_label_count;
    extraction.invalid_printed_range_count = printed_metrics.invalid_range_count;

    if !previous_page_provenance.is_empty() {
        let previous_detectability = ratio(
            previous_page_provenance
                .iter()
                .filter(|entry| entry.printed_page_status == "detected")
                .count(),
            previous_page_provenance.len(),
        )
        .unwrap_or(0.0);
        let current_detectability = extraction.printed_detectability_rate.unwrap_or(0.0);
        extraction.printed_detectability_drop_pp =
            Some((previous_detectability - current_detectability).max(0.0));
    }

    let clause_stats = compute_clause_split_metrics(connection)?;
    extraction.clause_chunks_over_900 = clause_stats.clause_chunks_over_900;
    extraction.max_clause_chunk_words = clause_stats.max_clause_chunk_words;
    extraction.overlap_pair_count = clause_stats.overlap_pair_count;
    extraction.overlap_compliant_pairs = clause_stats.overlap_compliant_pairs;
    extraction.overlap_compliance = ratio(
        clause_stats.overlap_compliant_pairs,
        clause_stats.overlap_pair_count,
    );
    extraction.split_sequence_violations = clause_stats.sequence_violations;
    extraction.q025_exemption_count = clause_stats.exemption_count;
    extraction.non_exempt_oversize_chunks = clause_stats.non_exempt_oversize_chunks;

    let normalization = compute_normalization_metrics(connection, refs)?;
    extraction.normalization_noise_ratio = normalization.global_noise_ratio;
    extraction.normalization_target_noise_count = normalization.target_noise_count;
    extraction.dehyphenation_false_positive_rate =
        estimate_dehyphenation_false_positive_rate(latest_snapshot);

    let list_semantics = compute_list_semantics_metrics(connection, &latest_counts)?;
    hierarchy.list_items_total = list_semantics.list_items_total;
    hierarchy.list_semantics_complete = list_semantics.list_semantics_complete;
    hierarchy.list_semantics_completeness = ratio(
        list_semantics.list_semantics_complete,
        list_semantics.list_items_total,
    );
    hierarchy.nested_parent_depth_violations = list_semantics.parent_depth_violations;
    hierarchy.list_parse_candidate_total = list_semantics.list_parse_candidate_total;
    hierarchy.list_parse_fallback_total = list_semantics.list_parse_fallback_total;
    hierarchy.list_parse_fallback_ratio = ratio(
        list_semantics.list_parse_fallback_total,
        list_semantics.list_parse_candidate_total,
    );

    let table_metrics = compute_table_semantics_metrics(connection)?;
    table_semantics.table_cells_total = table_metrics.table_cells_total;
    table_semantics.table_cells_semantics_complete = table_metrics.table_cells_semantics_complete;
    table_semantics.table_cell_semantics_completeness = ratio(
        table_metrics.table_cells_semantics_complete,
        table_metrics.table_cells_total,
    );
    table_semantics.invalid_span_count = table_metrics.invalid_span_count;
    table_semantics.header_flag_completeness = ratio(
        table_metrics.header_cells_flagged,
        table_metrics.header_cells_total,
    );
    table_semantics.one_cell_row_ratio =
        ratio(table_metrics.one_cell_rows, table_metrics.total_table_rows);
    table_semantics.asil_one_cell_row_ratio = ratio(
        table_metrics.asil_one_cell_rows,
        table_metrics.asil_total_rows,
    );

    let asil_alignment = collect_asil_table_alignment(
        connection,
        "ISO26262-6-2018",
        &["Table 3", "Table 6", "Table 10"],
    )?;
    table_semantics.asil_rating_coverage = asil_alignment.rating_coverage();
    table_semantics.asil_malformed_ratio = asil_alignment.malformed_ratio();
    table_semantics.asil_outlier_ratio = asil_alignment.outlier_ratio();

    let parity_artifacts = build_citation_parity_artifacts(
        connection,
        manifest_dir,
        citation_baseline_path,
        citation_baseline_mode,
        run_id,
        refs,
        latest_snapshot,
    )?;
    citation_parity.baseline_run_id = parity_artifacts.baseline_run_id;
    citation_parity.baseline_checksum = parity_artifacts.baseline_checksum.clone();
    citation_parity.baseline_created = parity_artifacts.baseline_created;
    citation_parity.baseline_missing = parity_artifacts.baseline_missing;
    citation_parity.target_linked_total = parity_artifacts.target_linked_total;
    citation_parity.comparable_total = parity_artifacts.comparable_total;
    citation_parity.top1_parity = parity_artifacts.top1_parity;
    citation_parity.top3_containment = parity_artifacts.top3_containment;
    citation_parity.page_range_parity = parity_artifacts.page_range_parity;

    let semantic_metrics = compute_semantic_embedding_metrics(connection)?;
    semantic_embeddings.active_model_id = semantic_metrics.active_model_id.clone();
    semantic_embeddings.embedding_dim = semantic_metrics.embedding_dim;
    semantic_embeddings.eligible_chunks = semantic_metrics.eligible_chunks;
    semantic_embeddings.embedded_chunks = semantic_metrics.embedded_chunks;
    semantic_embeddings.stale_rows = semantic_metrics.stale_rows;
    semantic_embeddings.embedding_rows_for_active_model =
        semantic_metrics.embedding_rows_for_active_model;
    semantic_embeddings.chunk_embedding_coverage_ratio = ratio(
        semantic_metrics.embedded_chunks,
        semantic_metrics.eligible_chunks,
    );
    semantic_embeddings.stale_embedding_ratio = ratio(
        semantic_metrics.stale_rows,
        semantic_metrics.eligible_chunks,
    );

    let q023_hard_fail = extraction
        .provenance_coverage
        .map(|coverage| coverage < WP2_EXTRACTION_PROVENANCE_COVERAGE_MIN)
        .unwrap_or(true)
        || extraction.unknown_backend_pages > 0;
    let q023_stage_b_fail = extraction
        .text_layer_replay_stability
        .map(|value| value < WP2_TEXT_LAYER_REPLAY_STABILITY_MIN)
        .unwrap_or(true)
        || (latest_counts.ocr_page_count > 0
            && extraction
                .ocr_replay_stability
                .map(|value| value < WP2_OCR_REPLAY_STABILITY_MIN)
                .unwrap_or(true));
    checks.push(QualityCheck {
        check_id: "Q-023".to_string(),
        name: "Extraction backend provenance completeness".to_string(),
        result: wp2_result(stage, q023_hard_fail, q023_stage_b_fail).to_string(),
    });

    let q024_hard_fail =
        extraction.invalid_printed_label_count > 0 || extraction.invalid_printed_range_count > 0;
    let q024_stage_b_fail = extraction
        .printed_status_coverage
        .map(|coverage| coverage < 1.0)
        .unwrap_or(true)
        || extraction
            .printed_mapping_on_detectable
            .map(|coverage| coverage < WP2_PRINTED_MAPPING_DETECTABLE_MIN)
            .unwrap_or(true)
        || extraction
            .printed_detectability_drop_pp
            .map(|drop| drop > WP2_PRINTED_DETECTABILITY_DROP_MAX)
            .unwrap_or(false);
    checks.push(QualityCheck {
        check_id: "Q-024".to_string(),
        name: "Printed-page mapping coverage/status completeness".to_string(),
        result: wp2_result(stage, q024_hard_fail, q024_stage_b_fail).to_string(),
    });

    let q025_stage_b_fail = extraction.non_exempt_oversize_chunks > 0
        || extraction
            .overlap_compliance
            .map(|ratio| ratio < WP2_OVERLAP_COMPLIANCE_MIN)
            .unwrap_or(true)
        || extraction.split_sequence_violations > 0;
    checks.push(QualityCheck {
        check_id: "Q-025".to_string(),
        name: "Long-clause split contract compliance".to_string(),
        result: wp2_result(stage, false, q025_stage_b_fail).to_string(),
    });

    let q026_stage_b_fail = hierarchy
        .list_semantics_completeness
        .map(|ratio| ratio < 1.0)
        .unwrap_or(true)
        || hierarchy.nested_parent_depth_violations > 0
        || hierarchy
            .list_parse_fallback_ratio
            .map(|ratio| ratio > WP2_LIST_FALLBACK_RATIO_MAX)
            .unwrap_or(true);
    checks.push(QualityCheck {
        check_id: "Q-026".to_string(),
        name: "Nested list depth/marker semantics completeness".to_string(),
        result: wp2_result(stage, false, q026_stage_b_fail).to_string(),
    });

    let q027_hard_fail = table_semantics.invalid_span_count > 0;
    let q027_stage_b_fail = table_semantics
        .table_cell_semantics_completeness
        .map(|ratio| ratio < 1.0)
        .unwrap_or(true)
        || table_metrics.targeted_semantic_miss_count > 0
        || table_semantics
            .header_flag_completeness
            .map(|ratio| ratio < 0.98)
            .unwrap_or(true);
    checks.push(QualityCheck {
        check_id: "Q-027".to_string(),
        name: "Table-cell semantic field completeness".to_string(),
        result: wp2_result(stage, q027_hard_fail, q027_stage_b_fail).to_string(),
    });

    let q028_hard_fail = evaluate_asil_table_alignment(&asil_alignment) == "failed";
    let q028_stage_b_fail = table_semantics
        .asil_rating_coverage
        .map(|value| value < WP2_ASIL_STRICT_MIN_RATING_COVERAGE)
        .unwrap_or(true)
        || table_semantics
            .asil_malformed_ratio
            .map(|value| value > WP2_ASIL_STRICT_MAX_MALFORMED_RATIO)
            .unwrap_or(true)
        || table_semantics
            .asil_outlier_ratio
            .map(|value| value > WP2_ASIL_STRICT_MAX_OUTLIER_RATIO)
            .unwrap_or(true)
        || table_semantics
            .asil_one_cell_row_ratio
            .map(|value| value > WP2_ASIL_STRICT_MAX_ONE_CELL_RATIO)
            .unwrap_or(true);
    checks.push(QualityCheck {
        check_id: "Q-028".to_string(),
        name: "Strict ASIL row-column alignment".to_string(),
        result: wp2_result(stage, q028_hard_fail, q028_stage_b_fail).to_string(),
    });

    let q029_hard_fail = extraction
        .normalization_noise_ratio
        .map(|ratio| ratio > 0.50)
        .unwrap_or(false);
    let q029_stage_b_fail = extraction
        .normalization_noise_ratio
        .map(|ratio| ratio > WP2_NOISE_LEAKAGE_GLOBAL_MAX)
        .unwrap_or(true)
        || extraction.normalization_target_noise_count > 0
        || extraction
            .dehyphenation_false_positive_rate
            .map(|ratio| ratio > 0.02)
            .unwrap_or(false);
    checks.push(QualityCheck {
        check_id: "Q-029".to_string(),
        name: "Normalization effectiveness/non-regression gate".to_string(),
        result: wp2_result(stage, q029_hard_fail, q029_stage_b_fail).to_string(),
    });

    let q030_stage_b_fail = citation_parity.baseline_missing
        || citation_parity.comparable_total == 0
        || citation_parity
            .top1_parity
            .map(|ratio| ratio < WP2_CITATION_TOP1_MIN)
            .unwrap_or(true)
        || citation_parity
            .top3_containment
            .map(|ratio| ratio < WP2_CITATION_TOP3_MIN)
            .unwrap_or(true)
        || citation_parity
            .page_range_parity
            .map(|ratio| ratio < WP2_CITATION_PAGE_RANGE_MIN)
            .unwrap_or(true);
    checks.push(QualityCheck {
        check_id: "Q-030".to_string(),
        name: "Citation parity non-regression for target-linked references".to_string(),
        result: wp2_result(stage, false, q030_stage_b_fail).to_string(),
    });

    if stage == Wp2GateStage::A {
        if q023_stage_b_fail {
            extraction.warnings.push(
                "Q-023 Stage A warning: replay-stability metrics are below Stage B targets."
                    .to_string(),
            );
        }
        if q024_stage_b_fail {
            extraction.warnings.push(
                "Q-024 Stage A warning: printed-page mapping is below Stage B policy targets."
                    .to_string(),
            );
        }
        if q025_stage_b_fail {
            extraction.warnings.push(
                "Q-025 Stage A warning: long-clause split overlap/sequence policy needs tuning."
                    .to_string(),
            );
        }
        if q026_stage_b_fail {
            hierarchy.warnings.push(
                "Q-026 Stage A warning: list semantic completeness/fallback ratio is below Stage B targets."
                    .to_string(),
            );
        }
        if q027_stage_b_fail {
            table_semantics.warnings.push(
                "Q-027 Stage A warning: table semantic completeness/targeted coverage is below Stage B targets."
                    .to_string(),
            );
        }
        if q028_stage_b_fail {
            table_semantics.warnings.push(
                "Q-028 Stage A warning: strict ASIL alignment thresholds are not yet met."
                    .to_string(),
            );
        }
        if q029_stage_b_fail {
            extraction.warnings.push(
                "Q-029 Stage A warning: normalization leakage/dehyphenation metrics are below Stage B targets."
                    .to_string(),
            );
        }
        if q030_stage_b_fail {
            if citation_parity.baseline_missing {
                citation_parity.warnings.push(
                    format!(
                        "Q-030 Stage A warning: citation lockfile is missing at {}; bootstrap with {}=bootstrap.",
                        citation_parity.baseline_path,
                        WP2_CITATION_BASELINE_MODE_ENV
                    ),
                );
            } else {
                citation_parity.warnings.push(
                    "Q-030 Stage A warning: citation parity is below Stage B thresholds."
                        .to_string(),
                );
            }
        }
    }

    for check in &checks {
        if check.result == "failed" {
            let recommendation = match check.check_id.as_str() {
                "Q-023" => Some(
                    "Ensure page provenance covers all processed pages and stabilize backend-specific replay behavior before Stage B.".to_string(),
                ),
                "Q-024" => Some(
                    "Improve printed-page label detectability and mapping on detectable chunks; eliminate invalid labels/ranges.".to_string(),
                ),
                "Q-025" => Some(
                    "Tune split boundaries/overlap for clause chunks and resolve any chunk_seq contiguity violations (or maintain approved exemptions).".to_string(),
                ),
                "Q-026" => Some(
                    "Complete list semantics population and reduce list parse fallback ratio using the fixed candidate denominator.".to_string(),
                ),
                "Q-027" => Some(
                    "Populate all table semantic fields for table_cell nodes and eliminate targeted semantic misses in Table 3/6/10.".to_string(),
                ),
                "Q-028" => Some(
                    "Improve ASIL row/column alignment so strict rating, malformed, outlier, and one-cell thresholds pass for Table 3/6/10.".to_string(),
                ),
                "Q-029" => Some(
                    "Reduce normalization noise leakage (global and target-linked) and keep dehyphenation behavior within fixture tolerance.".to_string(),
                ),
                "Q-030" => Some(
                    "Regressions in citation parity must be resolved before Stage B; verify lockfile continuity and tie-aware top-k parity (bootstrap with WP2_CITATION_BASELINE_MODE=bootstrap in Stage A when missing).".to_string(),
                ),
                _ => None,
            };

            if let Some(recommendation) = recommendation {
                recommendations.push(recommendation);
            }
        }
    }

    let semantic_assessment = build_semantic_quality_assessment(
        connection,
        manifest_dir,
        run_id,
        refs,
        stage,
        &semantic_metrics,
        &mut semantic_embeddings,
    )?;
    checks.extend(semantic_assessment.checks);
    recommendations.extend(semantic_assessment.recommendations);
    let semantic_quality = semantic_assessment.summary;

    Ok(Wp2Assessment {
        checks,
        extraction_fidelity: extraction,
        hierarchy_semantics: hierarchy,
        table_semantics,
        citation_parity,
        semantic_embeddings,
        semantic_quality,
        recommendations,
    })
}
