use super::*;

pub fn tokenize_pinpoint_value(value: &str) -> Vec<String> {
    let mut tokens = value
        .to_ascii_lowercase()
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|token| {
            !token.is_empty()
                && (token.len() >= 2 || token.chars().all(|character| character.is_ascii_digit()))
        })
        .filter(|token| {
            PINPOINT_TOKEN_STOPWORDS
                .iter()
                .all(|stopword| stopword != token)
        })
        .map(str::to_string)
        .collect::<Vec<String>>();
    tokens.sort();
    tokens.dedup();
    tokens
}

pub fn condense_whitespace(input: &str) -> String {
    input
        .split_whitespace()
        .collect::<Vec<&str>>()
        .join(" ")
        .trim()
        .to_string()
}

pub fn pinpoint_anchor_compatible(unit_anchor: Option<&str>, parent_anchor: Option<&str>) -> bool {
    let Some(parent_anchor) = parent_anchor.filter(|value| !value.trim().is_empty()) else {
        return true;
    };
    let Some(unit_anchor) = unit_anchor.filter(|value| !value.trim().is_empty()) else {
        return true;
    };
    if unit_anchor == parent_anchor {
        return true;
    }

    let parent_family = anchor_family(parent_anchor);
    let unit_family = anchor_family(unit_anchor);
    parent_family.is_some() && parent_family == unit_family
}

pub fn anchor_family(anchor: &str) -> Option<(String, String)> {
    let mut parts = anchor.split(':');
    let first = parts.next()?.trim();
    let second = parts.next()?.trim();
    if first.is_empty() || second.is_empty() {
        return None;
    }
    Some((first.to_string(), second.to_string()))
}

pub fn token_overlap_score(query_tokens: &[String], unit_tokens: &[String]) -> f64 {
    if query_tokens.is_empty() || unit_tokens.is_empty() {
        return 0.0;
    }
    let set = unit_tokens.iter().collect::<HashSet<&String>>();
    let overlap = query_tokens
        .iter()
        .filter(|token| set.contains(*token))
        .count();
    overlap as f64 / query_tokens.len() as f64
}

pub fn query_mentions_table_context(query_text: &str) -> bool {
    let lowered = query_text.to_ascii_lowercase();
    lowered.contains("table") || lowered.contains(" row ") || lowered.contains(" cell ")
}

pub fn looks_like_table_reference_query(query_text: &str) -> bool {
    let lowered = condense_whitespace(query_text).to_ascii_lowercase();
    let mut tokens = lowered.split_whitespace();
    match (tokens.next(), tokens.next(), tokens.next()) {
        (Some("table"), Some(value), None) => {
            value.chars().all(|character| character.is_ascii_digit())
        }
        _ => false,
    }
}

pub fn pinpoint_unit_priority(unit_type: &str, mentions_table: bool, table_reference: bool) -> i32 {
    if table_reference {
        return match unit_type {
            "table_row" => 4,
            "table_cell" => 3,
            "sentence_window" => 1,
            _ => 2,
        };
    }
    if mentions_table {
        return match unit_type {
            "table_row" => 4,
            "table_cell" => 3,
            "sentence_window" => 2,
            _ => 1,
        };
    }

    match unit_type {
        "sentence_window" => 3,
        "table_row" => 2,
        "table_cell" => 1,
        _ => 0,
    }
}

pub fn select_pinpoint_parent_chunk(
    query: &PinpointEvalQuery,
    retrieved_parent_chunk_id: Option<&str>,
) -> Option<String> {
    if let Some(retrieved_chunk_id) = retrieved_parent_chunk_id {
        if query
            .parent_expected_chunk_ids
            .iter()
            .any(|value| value == retrieved_chunk_id)
        {
            return Some(retrieved_chunk_id.to_string());
        }
    }

    query
        .parent_expected_chunk_ids
        .first()
        .cloned()
        .or_else(|| retrieved_parent_chunk_id.map(str::to_string))
}

pub fn resolve_chunk_anchor_id(connection: &Connection, chunk_id: &str) -> Result<Option<String>> {
    connection
        .query_row(
            "SELECT citation_anchor_id FROM chunks WHERE chunk_id = ?1 LIMIT 1",
            [chunk_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()
        .map(|value| value.flatten())
        .map_err(Into::into)
}
