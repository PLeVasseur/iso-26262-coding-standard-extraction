use anyhow::Result;
use rusqlite::Connection;

use super::citation::render_citation;
use super::hierarchy::{fetch_descendants, resolve_parent_ref};
use super::pinpoint::compute_pinpoint_units_for_candidate;
use super::run::{QueryCandidate, QueryRankTrace, QueryResult};
use super::text::condense_whitespace;

pub(super) fn to_results(
    connection: &Connection,
    query_text: &str,
    candidates: Vec<QueryCandidate>,
    with_ancestors: bool,
    with_descendants: bool,
    with_pinpoint: bool,
    pinpoint_max_units: usize,
) -> Result<Vec<QueryResult>> {
    let mut out = Vec::with_capacity(candidates.len());

    for (index, candidate) in candidates.into_iter().enumerate() {
        let citation = render_citation(&candidate);

        let ancestor_nodes = if with_ancestors {
            candidate
                .ancestor_path
                .as_deref()
                .map(|value| value.split(" > ").map(ToOwned::to_owned).collect())
        } else {
            None
        };

        let descendants = if with_descendants {
            if let Some(origin_node_id) = candidate.origin_node_id.as_deref() {
                Some(fetch_descendants(connection, origin_node_id)?)
            } else {
                Some(Vec::new())
            }
        } else {
            None
        };

        let (pinpoint_units, pinpoint_fallback_used) = if with_pinpoint {
            let computation = compute_pinpoint_units_for_candidate(
                connection,
                &candidate,
                query_text,
                pinpoint_max_units,
            )?;
            (Some(computation.units), Some(computation.fallback_used))
        } else {
            (None, None)
        };

        out.push(QueryResult {
            rank: index + 1,
            score: candidate.score,
            match_kind: candidate.match_kind,
            source_tags: candidate.source_tags,
            rank_trace: QueryRankTrace {
                lexical_rank: candidate.lexical_rank,
                semantic_rank: candidate.semantic_rank,
                lexical_score: candidate.lexical_score,
                semantic_score: candidate.semantic_score,
                rrf_score: candidate.rrf_score,
            },
            chunk_id: candidate.chunk_id,
            doc_id: candidate.doc_id,
            part: candidate.part,
            year: candidate.year,
            chunk_type: candidate.chunk_type,
            parent_ref: resolve_parent_ref(connection, candidate.origin_node_id.as_deref())?,
            reference: candidate.reference,
            heading: candidate.heading,
            page_pdf_start: candidate.page_pdf_start,
            page_pdf_end: candidate.page_pdf_end,
            source_hash: candidate.source_hash,
            snippet: condense_whitespace(&candidate.snippet),
            citation,
            origin_node_id: candidate.origin_node_id,
            leaf_node_type: candidate.leaf_node_type,
            ancestor_path: candidate.ancestor_path,
            anchor_type: candidate.anchor_type,
            anchor_label_raw: candidate.anchor_label_raw,
            anchor_label_norm: candidate.anchor_label_norm,
            anchor_order: candidate.anchor_order,
            citation_anchor_id: candidate.citation_anchor_id,
            ancestor_nodes,
            descendants,
            pinpoint_fallback_used,
            pinpoint_units,
        });
    }

    Ok(out)
}
