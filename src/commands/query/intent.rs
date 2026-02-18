pub(super) fn is_exact_intent_query(query_text: &str) -> bool {
    let trimmed = query_text.trim();
    if trimmed.is_empty() {
        return false;
    }

    let lowered = trimmed.to_ascii_lowercase();
    if lowered.starts_with("table ") {
        return lowered
            .split_whitespace()
            .nth(1)
            .map(|value| value.chars().all(|character| character.is_ascii_digit()))
            .unwrap_or(false);
    }

    if lowered.starts_with("annex ") {
        return lowered
            .split_whitespace()
            .nth(1)
            .map(|value| {
                value
                    .chars()
                    .all(|character| character.is_ascii_alphabetic())
            })
            .unwrap_or(false);
    }

    let first_token = lowered.split_whitespace().next().unwrap_or("");
    looks_like_clause_reference(first_token)
}

fn looks_like_clause_reference(value: &str) -> bool {
    let parts = value.split('.').collect::<Vec<&str>>();
    if parts.len() < 2 {
        return false;
    }

    parts
        .iter()
        .all(|part| !part.is_empty() && part.chars().all(|character| character.is_ascii_digit()))
}
