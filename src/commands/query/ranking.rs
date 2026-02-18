use std::collections::HashMap;

use super::run::QueryCandidate;

pub(super) fn upsert_candidate(
    dedup: &mut HashMap<String, QueryCandidate>,
    mut candidate: QueryCandidate,
) {
    let Some(existing) = dedup.get_mut(&candidate.chunk_id) else {
        dedup.insert(candidate.chunk_id.clone(), candidate);
        return;
    };

    let replace_existing = candidate.score > existing.score;
    if replace_existing {
        inherit_candidate_traces(&mut candidate, existing);
        *existing = candidate;
    } else {
        inherit_candidate_traces(existing, &candidate);
    }
}

fn inherit_candidate_traces(target: &mut QueryCandidate, source: &QueryCandidate) {
    for tag in &source.source_tags {
        if target.source_tags.iter().all(|value| value != tag) {
            target.source_tags.push(tag.clone());
        }
    }

    target.lexical_rank = target.lexical_rank.or(source.lexical_rank);
    target.semantic_rank = target.semantic_rank.or(source.semantic_rank);
    target.lexical_score = target.lexical_score.or(source.lexical_score);
    target.semantic_score = target.semantic_score.or(source.semantic_score);
    target.rrf_score = target.rrf_score.or(source.rrf_score);
}
