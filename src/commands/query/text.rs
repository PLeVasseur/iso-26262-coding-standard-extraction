fn to_fts_query(query_text: &str) -> String {
    query_text
        .split_whitespace()
        .filter(|token| !token.trim().is_empty())
        .map(|token| format!("\"{}\"", token.replace('"', "")))
        .collect::<Vec<String>>()
        .join(" ")
}

fn condense_whitespace(input: &str) -> String {
    input.split_whitespace().collect::<Vec<&str>>().join(" ")
}
