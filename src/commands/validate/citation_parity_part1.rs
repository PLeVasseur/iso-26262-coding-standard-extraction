use super::*;

pub fn build_citation_parity_artifacts(
    connection: &Connection,
    manifest_dir: &Path,
    baseline_path: &Path,
    baseline_mode: CitationBaselineMode,
    run_id: &str,
    refs: &[GoldReference],
    latest_snapshot: Option<&NamedIngestRunSnapshot>,
) -> Result<CitationParityComputation> {
    let report_path = manifest_dir.join("citation_parity_report.json");
    let current_entries = collect_citation_parity_entries(connection, refs)?;
    let current_checksum = checksum_citation_entries(&current_entries);

    let mut baseline_created = false;
    let mut baseline_missing = false;
    let (decision_id, change_reason) = resolve_citation_baseline_rationale();

    let baseline = if baseline_mode == CitationBaselineMode::Bootstrap {
        if baseline_path.exists() && (decision_id.is_none() || change_reason.is_none()) {
            bail!(
                "{}=bootstrap would rotate existing lockfile at {}; set both {} and {}",
                WP2_CITATION_BASELINE_MODE_ENV,
                baseline_path.display(),
                WP2_CITATION_BASELINE_DECISION_ENV,
                WP2_CITATION_BASELINE_REASON_ENV
            );
        }

        baseline_created = true;
        let baseline = CitationParityBaseline {
            manifest_version: 1,
            run_id: run_id.to_string(),
            generated_at: now_utc_string(),
            db_schema_version: latest_snapshot
                .and_then(|snapshot| snapshot.snapshot.db_schema_version.clone()),
            decision_id,
            change_reason,
            target_linked_count: current_entries.len(),
            query_options: "doc+reference deterministic top3".to_string(),
            checksum: current_checksum.clone(),
            entries: current_entries.clone(),
        };
        write_citation_parity_lockfile(baseline_path, &baseline)?;
        Some(baseline)
    } else if baseline_path.exists() {
        Some(read_citation_parity_lockfile(baseline_path)?)
    } else {
        baseline_missing = true;
        None
    };

    let baseline_map: HashMap<String, &CitationParityEntry> = baseline
        .as_ref()
        .map(|value| {
            value
                .entries
                .iter()
                .map(|entry| (entry.target_id.clone(), entry))
                .collect::<HashMap<String, &CitationParityEntry>>()
        })
        .unwrap_or_default();

    let mut comparable = 0usize;
    let mut top1_ok = 0usize;
    let mut top3_ok = 0usize;
    let mut page_ok = 0usize;
    let mut comparison_entries = Vec::<CitationParityComparisonEntry>::new();

    for entry in &current_entries {
        let Some(baseline_entry) = baseline_map.get(&entry.target_id) else {
            continue;
        };

        comparable += 1;
        let top1_match = baseline_entry.top_results.first() == entry.top_results.first();

        let baseline_set = baseline_entry
            .top_results
            .iter()
            .cloned()
            .collect::<HashSet<CitationParityIdentity>>();
        let current_set = entry
            .top_results
            .iter()
            .cloned()
            .collect::<HashSet<CitationParityIdentity>>();
        let top3_contains_baseline = baseline_set.is_subset(&current_set);

        let page_range_match = match (baseline_entry.top_results.first(), entry.top_results.first()) {
            (Some(left), Some(right)) => {
                left.page_start == right.page_start && left.page_end == right.page_end
            }
            _ => false,
        };

        if top1_match {
            top1_ok += 1;
        }
        if top3_contains_baseline {
            top3_ok += 1;
        }
        if page_range_match {
            page_ok += 1;
        }

        comparison_entries.push(CitationParityComparisonEntry {
            target_id: entry.target_id.clone(),
            top1_match,
            top3_contains_baseline,
            page_range_match,
        });
    }

    let top1_parity = ratio(top1_ok, comparable);
    let top3_containment = ratio(top3_ok, comparable);
    let page_range_parity = ratio(page_ok, comparable);

    let artifact = CitationParityArtifact {
        manifest_version: 1,
        run_id: run_id.to_string(),
        generated_at: now_utc_string(),
        baseline_path: baseline_path.display().to_string(),
        baseline_mode: baseline_mode.as_str().to_string(),
        baseline_checksum: baseline.as_ref().map(|value| value.checksum.clone()),
        baseline_missing,
        target_linked_count: current_entries.len(),
        comparable_count: comparable,
        top1_parity,
        top3_containment,
        page_range_parity,
        baseline_created,
        entries: comparison_entries,
    };
    write_json_pretty(&report_path, &artifact)?;

    Ok(CitationParityComputation {
        baseline_run_id: baseline.as_ref().map(|value| value.run_id.clone()),
        baseline_checksum: baseline.as_ref().map(|value| value.checksum.clone()),
        baseline_created,
        baseline_missing,
        target_linked_total: current_entries.len(),
        comparable_total: comparable,
        top1_parity,
        top3_containment,
        page_range_parity,
    })
}

pub fn resolve_citation_baseline_rationale() -> (Option<String>, Option<String>) {
    let decision_id = std::env::var(WP2_CITATION_BASELINE_DECISION_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let reason = std::env::var(WP2_CITATION_BASELINE_REASON_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    (decision_id, reason)
}

pub fn write_citation_parity_lockfile(path: &Path, baseline: &CitationParityBaseline) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create lockfile directory {}", parent.display()))?;
    }

    write_json_pretty(path, baseline)
}

pub fn read_citation_parity_lockfile(path: &Path) -> Result<CitationParityBaseline> {
    let raw = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let parsed = serde_json::from_slice::<serde_json::Value>(&raw)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    ensure_citation_baseline_metadata_only(&parsed)?;
    serde_json::from_value::<CitationParityBaseline>(parsed)
        .with_context(|| format!("failed to decode {}", path.display()))
}

pub fn ensure_citation_baseline_metadata_only(value: &serde_json::Value) -> Result<()> {
    const FORBIDDEN_KEYS: &[&str] = &[
        "text",
        "snippet",
        "heading",
        "chunk_text",
        "table_md",
        "table_csv",
        "raw_text",
        "content",
    ];

    let mut stack = vec![("$".to_string(), value)];
    while let Some((path, node)) = stack.pop() {
        match node {
            serde_json::Value::Object(map) => {
                for (key, child) in map {
                    let lowered = key.to_ascii_lowercase();
                    if FORBIDDEN_KEYS.iter().any(|forbidden| *forbidden == lowered) {
                        bail!(
                            "citation parity lockfile contains forbidden text-bearing key '{}' at {}",
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

pub fn collect_citation_parity_entries(
    connection: &Connection,
    refs: &[GoldReference],
) -> Result<Vec<CitationParityEntry>> {
    let mut target_refs = refs
        .iter()
        .filter_map(|reference| {
            reference.target_id.as_ref().map(|target_id| {
                (
                    target_id.trim().to_string(),
                    reference.doc_id.clone(),
                    reference.reference.clone(),
                )
            })
        })
        .collect::<Vec<(String, String, String)>>();
    target_refs.sort_by(|left, right| left.0.cmp(&right.0));
    target_refs.dedup_by(|left, right| left.0 == right.0);

    let mut entries = Vec::<CitationParityEntry>::new();
    for (target_id, doc_id, reference) in target_refs {
        let top_results = query_citation_parity_results(connection, &doc_id, &reference)?;
        entries.push(CitationParityEntry {
            target_id,
            doc_id,
            reference,
            top_results,
        });
    }

    Ok(entries)
}

pub fn query_citation_parity_results(
    connection: &Connection,
    doc_id: &str,
    reference: &str,
) -> Result<Vec<CitationParityIdentity>> {
    let mut statement = connection.prepare(
        "
        SELECT
          COALESCE(ref, ''),
          COALESCE(anchor_type, ''),
          COALESCE(anchor_label_norm, ''),
          COALESCE(citation_anchor_id, ''),
          page_pdf_start,
          page_pdf_end,
          chunk_id
        FROM chunks
        WHERE doc_id = ?1
          AND (
            lower(COALESCE(ref, '')) = lower(?2)
            OR lower(COALESCE(heading, '')) = lower(?2)
            OR lower(COALESCE(ref, '')) LIKE '%' || lower(?2) || '%'
            OR lower(COALESCE(heading, '')) LIKE '%' || lower(?2) || '%'
          )
        ORDER BY
          CASE
            WHEN lower(COALESCE(ref, '')) = lower(?2) THEN 1000
            WHEN lower(COALESCE(heading, '')) = lower(?2) THEN 900
            WHEN lower(COALESCE(ref, '')) LIKE '%' || lower(?2) || '%' THEN 700
            ELSE 600
          END DESC,
          page_pdf_start ASC,
          chunk_id ASC
        LIMIT 3
        ",
    )?;

    let mut rows = statement.query(params![doc_id, reference])?;
    let mut out = Vec::<CitationParityIdentity>::new();
    while let Some(row) = rows.next()? {
        let raw_ref: String = row.get(0)?;
        let anchor_type: String = row.get(1)?;
        let anchor_label_norm: String = row.get(2)?;
        let citation_anchor_id: String = row.get(3)?;
        let page_start: Option<i64> = row.get(4)?;
        let page_end: Option<i64> = row.get(5)?;

        let anchor_identity = if !citation_anchor_id.trim().is_empty() {
            citation_anchor_id
        } else {
            format!("{}:{}", anchor_type.trim(), anchor_label_norm.trim())
        };

        out.push(CitationParityIdentity {
            canonical_ref: canonicalize_reference_for_parity(&raw_ref),
            anchor_identity,
            page_start,
            page_end,
        });
    }

    Ok(out)
}

pub fn canonicalize_reference_for_parity(reference: &str) -> String {
    if let Some((base, _)) = reference.split_once(" item ") {
        return base.trim().to_string();
    }
    if let Some((base, _)) = reference.split_once(" note ") {
        return base.trim().to_string();
    }
    if let Some((base, _)) = reference.split_once(" para ") {
        return base.trim().to_string();
    }
    if let Some((base, _)) = reference.split_once(" row ") {
        return base.trim().to_string();
    }
    reference.trim().to_string()
}

pub fn checksum_citation_entries(entries: &[CitationParityEntry]) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for entry in entries {
        entry.target_id.hash(&mut hasher);
        entry.doc_id.hash(&mut hasher);
        entry.reference.hash(&mut hasher);
        for result in &entry.top_results {
            result.hash(&mut hasher);
        }
    }
    format!("{:016x}", hasher.finish())
}

pub fn replay_stability_ratio(
    current_entries: &[PageProvenanceEntry],
    previous_entries: &[PageProvenanceEntry],
    backend: &str,
) -> Option<f64> {
    if current_entries.is_empty() || previous_entries.is_empty() {
        return None;
    }

    let previous_map = previous_entries
        .iter()
        .filter(|entry| entry.backend == backend)
        .map(|entry| ((entry.doc_id.clone(), entry.page_pdf), entry.text_char_count))
        .collect::<HashMap<(String, i64), usize>>();

    if previous_map.is_empty() {
        return None;
    }

    let mut comparable = 0usize;
    let mut stable = 0usize;
    for entry in current_entries.iter().filter(|entry| entry.backend == backend) {
        if let Some(previous_chars) = previous_map.get(&(entry.doc_id.clone(), entry.page_pdf)) {
            comparable += 1;
            if *previous_chars == entry.text_char_count {
                stable += 1;
            }
        }
    }

    ratio(stable, comparable)
}

