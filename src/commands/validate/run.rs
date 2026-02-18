pub fn run(args: ValidateArgs) -> Result<()> {
    let manifest_dir = args.cache_root.join("manifests");
    let gold_manifest_path = args
        .gold_manifest_path
        .clone()
        .unwrap_or_else(|| manifest_dir.join("gold_set_expected_results.json"));
    let quality_report_path = args
        .quality_report_path
        .clone()
        .unwrap_or_else(|| manifest_dir.join("extraction_quality_report.json"));
    let db_path = args
        .db_path
        .clone()
        .unwrap_or_else(|| args.cache_root.join("iso26262_index.sqlite"));

    let mut gold_manifest = load_gold_manifest(&gold_manifest_path)?;
    let run_id = resolve_run_id(&manifest_dir, &gold_manifest.run_id);
    let wp2_stage = resolve_wp2_gate_stage();
    let wp2_stage_policy = Wp2StagePolicy {
        requested_stage: std::env::var("WP2_GATE_STAGE").unwrap_or_else(|_| "A".to_string()),
        effective_stage: wp2_stage.as_str().to_string(),
        enforcement_mode: wp2_stage.mode_label().to_string(),
    };
    let citation_baseline_mode = resolve_citation_baseline_mode();
    let citation_baseline_path = resolve_citation_baseline_path();
    if wp2_stage == Wp2GateStage::B && citation_baseline_mode == CitationBaselineMode::Bootstrap {
        bail!(
            "{}=bootstrap is not allowed with WP2_GATE_STAGE=B; run Stage A first to bootstrap lockfile at {}",
            WP2_CITATION_BASELINE_MODE_ENV,
            citation_baseline_path.display()
        );
    }
    let ingest_snapshots = load_ingest_snapshots(&manifest_dir).unwrap_or_default();
    let latest_ingest_snapshot = ingest_snapshots.last().cloned();
    let previous_ingest_snapshot = if ingest_snapshots.len() > 1 {
        ingest_snapshots
            .get(ingest_snapshots.len().saturating_sub(2))
            .cloned()
    } else {
        None
    };
    let table_quality_scorecard =
        load_table_quality_scorecard(&manifest_dir).unwrap_or_else(|_| empty_table_scorecard());

    let connection = Connection::open_with_flags(
        &db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("failed to open database read-only: {}", db_path.display()))?;

    let evaluable_doc_ids = collect_evaluable_doc_ids(&connection)?;

    let mut evaluations = Vec::with_capacity(gold_manifest.gold_references.len());
    for reference in &mut gold_manifest.gold_references {
        if !evaluable_doc_ids.contains(&reference.doc_id) {
            reference.status = "skip".to_string();
            evaluations.push(skipped_reference_evaluation());
            continue;
        }

        let evaluation = evaluate_reference(&connection, reference)?;
        let hierarchy_required = has_hierarchy_expectations(reference);
        let hierarchy_ok = !hierarchy_required || evaluation.hierarchy_ok;
        reference.status = if evaluation.found && evaluation.has_all_terms && hierarchy_ok {
            "pass".to_string()
        } else {
            "fail".to_string()
        };
        evaluations.push(evaluation);
    }

    write_json_pretty(&gold_manifest_path, &gold_manifest)?;

    let target_sections = load_target_sections_manifest(&manifest_dir)?;
    let target_coverage =
        build_target_coverage_report(&target_sections, &gold_manifest.gold_references);
    let freshness = build_freshness_report(&manifest_dir, &target_sections)?;

    let mut checks = build_quality_checks(
        &connection,
        &gold_manifest.gold_references,
        &evaluations,
        &table_quality_scorecard,
        &target_coverage,
        &freshness,
    )?;
    let wp2_assessment = build_wp2_assessment(
        &connection,
        &manifest_dir,
        &run_id,
        &gold_manifest.gold_references,
        wp2_stage,
        &citation_baseline_path,
        citation_baseline_mode,
        latest_ingest_snapshot.as_ref(),
        previous_ingest_snapshot.as_ref(),
    )?;
    checks.extend(wp2_assessment.checks.iter().cloned());

    let summary = summarize_checks(&checks);
    let hierarchy_metrics = build_hierarchy_metrics(&evaluations);

    let issues = checks
        .iter()
        .filter(|check| check.result == "failed")
        .map(|check| format!("{} failed", check.name))
        .collect::<Vec<String>>();

    let mut recommendations = Vec::new();
    if checks
        .iter()
        .any(|check| check.check_id == "Q-001" && check.result == "failed")
    {
        recommendations.push(
            "Review heading parsing and reference normalization for missing gold references."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-002" && check.result == "pending")
    {
        recommendations.push(
            "Populate expected page patterns in gold set for citation range validation."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-003" && check.result == "failed")
    {
        recommendations.push(
            "Improve structured table extraction to populate table_row/table_cell descendants for key references."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-007" && check.result == "failed")
    {
        recommendations.push(
            "Ensure chunk lineage columns (origin_node_id, leaf_node_type, ancestor_path) are populated on ingest."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-011" && check.result == "failed")
    {
        recommendations.push(
            "Reduce sparse table rows by improving continuation merge rules for marker-bearing rows."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-012" && check.result == "failed")
    {
        recommendations.push(
            "Reduce overloaded table rows by splitting rows that contain multiple marker tokens."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-013" && check.result == "failed")
    {
        recommendations.push(
            "Improve table marker sequence coverage by repairing missing marker rows and preserving marker order."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-014" && check.result == "failed")
    {
        recommendations.push(
            "Increase table description coverage by populating non-empty description cells for marker rows."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-015" && check.result == "failed")
    {
        recommendations.push(
            "Improve marker extraction coverage by expanding marker parsing for list and note patterns."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-016" && check.result == "failed")
    {
        recommendations.push(
            "Improve marker citation accuracy by validating expected marker labels against extracted anchors."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-017" && check.result == "failed")
    {
        recommendations.push(
            "Improve paragraph fallback citation accuracy by stabilizing paragraph segmentation and indices."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-018" && check.result == "failed")
    {
        recommendations.push(
            "Fix structural hierarchy violations (parent lineage, dangling pointers, and note/list/table parent contracts)."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-019" && check.result == "failed")
    {
        recommendations.push(
            "Improve ASIL table row/cell alignment by distributing rating cells across marker rows and reducing malformed marker descriptions."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-020" && check.result == "failed")
    {
        recommendations.push(
            "Ensure target_sections.json and target-linked gold rows stay in one-to-one alignment (no missing, duplicate, or unexpected target_id values)."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-021" && check.result == "failed")
    {
        recommendations.push(
            "Resolve target-linked retrieval failures by correcting canonical references and expected node/anchor metadata for target-linked gold rows."
                .to_string(),
        );
    }
    if checks
        .iter()
        .any(|check| check.check_id == "Q-022" && check.result == "failed")
    {
        recommendations.push(
            "Run a single full-target ingest cycle for Parts 2, 6, 8, and 9 so freshness is consistent across all required target parts."
                .to_string(),
        );
    }
    recommendations.extend(wp2_assessment.recommendations.iter().cloned());

    let report = QualityReport {
        manifest_version: 2,
        run_id,
        generated_at: now_utc_string(),
        status: if summary.failed > 0 {
            "failed".to_string()
        } else if summary.pending > 0 {
            "partial".to_string()
        } else {
            "passed".to_string()
        },
        summary,
        wp2_stage_policy,
        target_coverage,
        freshness,
        hierarchy_metrics,
        table_quality_scorecard,
        extraction_fidelity: wp2_assessment.extraction_fidelity,
        hierarchy_semantics: wp2_assessment.hierarchy_semantics,
        table_semantics: wp2_assessment.table_semantics,
        citation_parity: wp2_assessment.citation_parity,
        semantic_embeddings: wp2_assessment.semantic_embeddings,
        semantic_quality: wp2_assessment.semantic_quality,
        checks,
        issues,
        recommendations,
    };

    write_json_pretty(&quality_report_path, &report)?;

    info!(
        gold_path = %gold_manifest_path.display(),
        report_path = %quality_report_path.display(),
        "validation completed"
    );

    Ok(())
}
