fn load_page_provenance_entries(
    manifest_dir: &Path,
    snapshot: Option<&NamedIngestRunSnapshot>,
) -> Result<Vec<PageProvenanceEntry>> {
    let Some(snapshot) = snapshot else {
        return Ok(Vec::new());
    };

    let Some(path_value) = snapshot.snapshot.paths.page_provenance_path.as_deref() else {
        return Ok(Vec::new());
    };

    let candidate = Path::new(path_value);
    let path = if candidate.is_absolute() {
        candidate.to_path_buf()
    } else if candidate.exists() {
        candidate.to_path_buf()
    } else {
        manifest_dir.join(candidate.file_name().unwrap_or_default())
    };

    if !path.exists() {
        return Ok(Vec::new());
    }

    let raw = fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
    let manifest: PageProvenanceManifestSnapshot = serde_json::from_slice(&raw)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(manifest.entries)
}

fn load_gold_manifest(path: &Path) -> Result<GoldSetManifest> {
    let raw = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    let manifest: GoldSetManifest = serde_json::from_slice(&raw)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    Ok(manifest)
}

fn resolve_run_id(manifest_dir: &Path, fallback: &str) -> String {
    let latest_ingest_run_id = load_latest_ingest_run_id(manifest_dir).ok().flatten();

    let run_state_path = manifest_dir.join("run_state.json");
    let run_state_run_id = fs::read(&run_state_path)
        .ok()
        .and_then(|raw| serde_json::from_slice::<RunStateManifest>(&raw).ok())
        .and_then(|state| state.active_run_id);

    latest_ingest_run_id
        .or(run_state_run_id)
        .unwrap_or_else(|| fallback.to_string())
}

fn load_target_sections_manifest(manifest_dir: &Path) -> Result<Option<TargetSectionsManifest>> {
    let target_sections_path = manifest_dir.join("target_sections.json");
    if !target_sections_path.exists() {
        return Ok(None);
    }

    let raw = fs::read(&target_sections_path)
        .with_context(|| format!("failed to read {}", target_sections_path.display()))?;
    let manifest: TargetSectionsManifest = serde_json::from_slice(&raw)
        .with_context(|| format!("failed to parse {}", target_sections_path.display()))?;

    Ok(Some(manifest))
}

fn build_target_coverage_report(
    target_sections: &Option<TargetSectionsManifest>,
    refs: &[GoldReference],
) -> TargetCoverageReport {
    let Some(target_sections) = target_sections.as_ref() else {
        return TargetCoverageReport {
            source_manifest: None,
            target_total: 0,
            target_linked_gold_total: refs
                .iter()
                .filter(|reference| reference.target_id.is_some())
                .count(),
            covered_target_total: 0,
            missing_target_ids: Vec::new(),
            duplicate_target_ids: Vec::new(),
            unexpected_target_ids: Vec::new(),
        };
    };

    let target_ids = target_sections
        .targets
        .iter()
        .map(|target| target.id.trim().to_string())
        .collect::<Vec<String>>();
    let target_lookup = target_ids.iter().cloned().collect::<HashSet<String>>();

    let mut counts = HashMap::<String, usize>::new();
    let mut unexpected_target_ids = Vec::<String>::new();

    for reference in refs {
        let Some(target_id) = reference.target_id.as_deref().map(str::trim) else {
            continue;
        };

        if target_lookup.contains(target_id) {
            *counts.entry(target_id.to_string()).or_insert(0) += 1;
        } else {
            unexpected_target_ids.push(target_id.to_string());
        }
    }

    let mut missing_target_ids = Vec::<String>::new();
    let mut duplicate_target_ids = Vec::<String>::new();
    for target_id in &target_ids {
        match counts.get(target_id).copied().unwrap_or(0) {
            0 => missing_target_ids.push(target_id.clone()),
            1 => {}
            _ => duplicate_target_ids.push(target_id.clone()),
        }
    }

    unexpected_target_ids.sort();
    unexpected_target_ids.dedup();

    let target_linked_gold_total = refs
        .iter()
        .filter(|reference| reference.target_id.is_some())
        .count();

    TargetCoverageReport {
        source_manifest: Some("target_sections.json".to_string()),
        target_total: target_sections.target_count.unwrap_or(target_ids.len()),
        target_linked_gold_total,
        covered_target_total: target_ids.len().saturating_sub(missing_target_ids.len()),
        missing_target_ids,
        duplicate_target_ids,
        unexpected_target_ids,
    }
}

fn build_freshness_report(
    manifest_dir: &Path,
    target_sections: &Option<TargetSectionsManifest>,
) -> Result<FreshnessReport> {
    let required_parts = target_sections
        .as_ref()
        .map(required_target_parts)
        .unwrap_or_default();

    let snapshots = load_ingest_snapshots(manifest_dir)?;
    let latest = snapshots.last();

    let latest_run_parts = latest
        .map(|snapshot| resolve_processed_parts(&snapshot.snapshot, &required_parts))
        .unwrap_or_default();

    let stale_parts = required_parts
        .iter()
        .copied()
        .filter(|part| !latest_run_parts.contains(part))
        .collect::<Vec<u32>>();

    let mut latest_run_by_part = Vec::<PartFreshness>::new();
    for part in &required_parts {
        let mut entry = PartFreshness {
            part: *part,
            manifest: None,
            run_id: None,
            started_at: None,
        };

        for snapshot in snapshots.iter().rev() {
            let processed_parts = resolve_processed_parts(&snapshot.snapshot, &required_parts);
            if processed_parts.contains(part) {
                entry.manifest = Some(snapshot.manifest_name.clone());
                entry.run_id = snapshot.snapshot.run_id.clone();
                entry.started_at = snapshot.snapshot.started_at.clone();
                break;
            }
        }

        latest_run_by_part.push(entry);
    }

    let full_target_cycle_run_id = snapshots.iter().rev().find_map(|snapshot| {
        let processed_parts = resolve_processed_parts(&snapshot.snapshot, &required_parts);
        let all_parts_present = required_parts
            .iter()
            .all(|required| processed_parts.contains(required));
        if all_parts_present {
            snapshot.snapshot.run_id.clone()
        } else {
            None
        }
    });

    Ok(FreshnessReport {
        source_manifest_dir: manifest_dir.display().to_string(),
        required_parts,
        latest_manifest: latest.map(|snapshot| snapshot.manifest_name.clone()),
        latest_run_id: latest.and_then(|snapshot| snapshot.snapshot.run_id.clone()),
        latest_started_at: latest.and_then(|snapshot| snapshot.snapshot.started_at.clone()),
        latest_run_parts,
        latest_run_by_part,
        full_target_cycle_run_id,
        stale_parts,
    })
}

fn required_target_parts(manifest: &TargetSectionsManifest) -> Vec<u32> {
    let mut parts = manifest
        .targets
        .iter()
        .map(|target| target.part)
        .collect::<Vec<u32>>();
    parts.sort_unstable();
    parts.dedup();
    parts
}

fn load_ingest_snapshots(manifest_dir: &Path) -> Result<Vec<NamedIngestRunSnapshot>> {
    let mut snapshots = Vec::<NamedIngestRunSnapshot>::new();

    for entry in fs::read_dir(manifest_dir)? {
        let entry = entry?;
        let file_name = entry.file_name().to_string_lossy().to_string();
        if !file_name.starts_with("ingest_run_") || !file_name.ends_with(".json") {
            continue;
        }

        let manifest_path = entry.path();
        let raw = fs::read(&manifest_path)
            .with_context(|| format!("failed to read {}", manifest_path.display()))?;
        let snapshot: IngestRunSnapshot = serde_json::from_slice(&raw)
            .with_context(|| format!("failed to parse {}", manifest_path.display()))?;

        snapshots.push(NamedIngestRunSnapshot {
            manifest_name: file_name,
            snapshot,
        });
    }

    snapshots.sort_by(|left, right| left.manifest_name.cmp(&right.manifest_name));
    Ok(snapshots)
}

fn resolve_processed_parts(snapshot: &IngestRunSnapshot, required_parts: &[u32]) -> Vec<u32> {
    let mut processed_parts = if !snapshot.processed_parts.is_empty() {
        snapshot.processed_parts.clone()
    } else {
        parse_target_parts_from_command(snapshot.command.as_deref().unwrap_or(""))
    };

    if processed_parts.is_empty() {
        processed_parts = required_parts.to_vec();
    }

    processed_parts.sort_unstable();
    processed_parts.dedup();
    processed_parts
}

fn parse_target_parts_from_command(command: &str) -> Vec<u32> {
    let mut parts = Vec::<u32>::new();
    let mut tokens = command.split_whitespace().peekable();

    while let Some(token) = tokens.next() {
        if token != "--target-part" {
            continue;
        }

        let Some(value) = tokens.next() else {
            continue;
        };

        if let Ok(parsed) = value.parse::<u32>() {
            parts.push(parsed);
        }
    }

    parts.sort_unstable();
    parts.dedup();
    parts
}

fn load_latest_ingest_run_id(manifest_dir: &Path) -> Result<Option<String>> {
    let mut latest_manifest_path: Option<PathBuf> = None;
    let mut latest_manifest_name: Option<String> = None;

    for entry in fs::read_dir(manifest_dir)? {
        let entry = entry?;
        let file_name = entry.file_name().to_string_lossy().to_string();
        if !file_name.starts_with("ingest_run_") || !file_name.ends_with(".json") {
            continue;
        }

        match &latest_manifest_name {
            Some(current) if file_name <= *current => {}
            _ => {
                latest_manifest_name = Some(file_name);
                latest_manifest_path = Some(entry.path());
            }
        }
    }

    let Some(manifest_path) = latest_manifest_path else {
        return Ok(None);
    };

    let raw = fs::read(&manifest_path)
        .with_context(|| format!("failed to read {}", manifest_path.display()))?;
    let snapshot: IngestRunSnapshot = serde_json::from_slice(&raw)
        .with_context(|| format!("failed to parse {}", manifest_path.display()))?;

    Ok(snapshot
        .run_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty()))
}

fn load_table_quality_scorecard(manifest_dir: &Path) -> Result<TableQualityScorecard> {
    let mut latest_manifest: Option<(String, PathBuf)> = None;

    for entry in fs::read_dir(manifest_dir)? {
        let entry = entry?;
        let file_name = entry.file_name().to_string_lossy().to_string();
        if !file_name.starts_with("ingest_run_") || !file_name.ends_with(".json") {
            continue;
        }

        match &latest_manifest {
            Some((current, _)) if file_name <= *current => {}
            _ => latest_manifest = Some((file_name, entry.path())),
        }
    }

    let Some((manifest_name, manifest_path)) = latest_manifest else {
        return Ok(empty_table_scorecard());
    };

    let raw = fs::read(&manifest_path)
        .with_context(|| format!("failed to read {}", manifest_path.display()))?;
    let snapshot: IngestRunSnapshot = serde_json::from_slice(&raw)
        .with_context(|| format!("failed to parse {}", manifest_path.display()))?;

    Ok(build_table_quality_scorecard(
        Some(manifest_name),
        snapshot.counts.table_quality_counters(),
    ))
}

