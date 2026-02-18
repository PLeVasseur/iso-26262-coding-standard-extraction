fn resolve_chunk_type_filter(raw_values: &[String]) -> HashSet<String> {
    raw_values
        .iter()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .collect::<HashSet<String>>()
}

fn is_supported_chunk_type(chunk_type: &str) -> bool {
    matches!(chunk_type, "clause" | "annex" | "table")
}

fn is_eligible_chunk(
    chunk_type: &str,
    chunk_type_filter: &HashSet<String>,
    payload: &Option<String>,
) -> bool {
    if !is_supported_chunk_type(chunk_type) {
        return false;
    }

    if !chunk_type_filter.is_empty() && !chunk_type_filter.contains(chunk_type) {
        return false;
    }

    payload.is_some()
}

fn build_chunk_payload(row: &EmbedChunkRow) -> Option<String> {
    chunk_payload_for_embedding(
        &row.chunk_type,
        &row.reference,
        &row.heading,
        row.text.as_deref(),
        row.table_md.as_deref(),
    )
}
