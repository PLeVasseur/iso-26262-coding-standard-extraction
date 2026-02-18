fn fuse_rrf_candidates(
    lexical_candidates: &[QueryCandidate],
    semantic_candidates: &[QueryCandidate],
    rrf_k: u32,
    fusion_mode: FusionMode,
) -> Result<Vec<QueryCandidate>> {
    let FusionMode::Rrf = fusion_mode;
    let mut merged = HashMap::<String, QueryCandidate>::new();
    let rrf_base = f64::from(rrf_k.max(1));

    for (index, candidate) in lexical_candidates.iter().enumerate() {
        let rank = candidate.lexical_rank.unwrap_or(index + 1);
        let contribution = 1.0 / (rrf_base + rank as f64);
        let entry = merged
            .entry(candidate.chunk_id.clone())
            .or_insert_with(|| seed_fusion_candidate(candidate));
        entry.score += contribution;
        entry.rrf_score = Some(entry.score);
        entry.lexical_rank = Some(rank);
        entry.lexical_score = candidate.lexical_score.or(Some(candidate.score));
        merge_source_tag(entry, "lexical");
    }

    for (index, candidate) in semantic_candidates.iter().enumerate() {
        let rank = candidate.semantic_rank.unwrap_or(index + 1);
        let contribution = 1.0 / (rrf_base + rank as f64);
        let entry = merged
            .entry(candidate.chunk_id.clone())
            .or_insert_with(|| seed_fusion_candidate(candidate));
        entry.score += contribution;
        entry.rrf_score = Some(entry.score);
        entry.semantic_rank = Some(rank);
        entry.semantic_score = candidate.semantic_score.or(Some(candidate.score));
        merge_source_tag(entry, "semantic");
    }

    let mut out = merged
        .into_values()
        .map(|mut value| {
            value.match_kind = match (value.lexical_rank, value.semantic_rank) {
                (Some(_), Some(_)) => "hybrid_rrf",
                (Some(_), None) => "lexical_rrf",
                (None, Some(_)) => "semantic_rrf",
                (None, None) => "hybrid_rrf",
            }
            .to_string();
            value
        })
        .collect::<Vec<QueryCandidate>>();

    sort_candidates(&mut out);
    Ok(out)
}

fn seed_fusion_candidate(candidate: &QueryCandidate) -> QueryCandidate {
    let mut seeded = candidate.clone();
    seeded.score = 0.0;
    seeded.rrf_score = Some(0.0);
    seeded.source_tags = Vec::new();
    seeded
}

fn merge_source_tag(candidate: &mut QueryCandidate, source: &str) {
    if candidate.source_tags.iter().all(|value| value != source) {
        candidate.source_tags.push(source.to_string());
    }
}
