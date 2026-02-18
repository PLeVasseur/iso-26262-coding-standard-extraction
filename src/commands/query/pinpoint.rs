#[derive(Debug, Clone)]
struct PinpointComputation {
    units: Vec<PinpointUnit>,
    fallback_used: bool,
}

const PINPOINT_QUERY_STOPWORDS: &[&str] = &[
    "a",
    "an",
    "and",
    "as",
    "at",
    "by",
    "concept",
    "concerning",
    "for",
    "from",
    "guidance",
    "in",
    "into",
    "of",
    "on",
    "or",
    "related",
    "requirement",
    "requirements",
    "table",
    "that",
    "the",
    "to",
    "with",
];

#[derive(Debug, Clone)]
struct PinpointUnitDraft {
    unit_id: String,
    unit_type: String,
    text: String,
    char_start: Option<usize>,
    char_end: Option<usize>,
    row_idx: Option<i64>,
    col_idx: Option<i64>,
    row_key: Option<String>,
    origin_node_id: Option<String>,
    citation_anchor_id: Option<String>,
}

fn compute_pinpoint_units_for_candidate(
    connection: &Connection,
    candidate: &QueryCandidate,
    query_text: &str,
    max_units: usize,
) -> Result<PinpointComputation> {
    if max_units == 0 {
        return Ok(PinpointComputation {
            units: Vec::new(),
            fallback_used: true,
        });
    }

    let mut units = collect_candidate_pinpoint_units(connection, candidate)?;
    let fallback_used = units.is_empty();
    if units.is_empty() {
        units.push(fallback_pinpoint_unit(candidate));
    }

    let query_tokens = tokenize_pinpoint_text(query_text);
    let query_phrase = condense_whitespace(query_text).to_ascii_lowercase();
    let query_mentions_table = query_mentions_table_context(query_text);
    let query_is_table_reference = looks_like_table_reference_query(query_text);

    let parent_anchor = candidate.citation_anchor_id.clone();
    let mut scored = units
        .into_iter()
        .map(|unit| {
            let normalized_text = condense_whitespace(&unit.text);
            let unit_tokens = tokenize_pinpoint_text(&normalized_text);
            let token_overlap = token_overlap_ratio(&query_tokens, &unit_tokens);
            let phrase_bonus = phrase_match_bonus(&query_phrase, &normalized_text);
            let mut score = token_overlap * 0.70 + phrase_bonus * 0.20;

            if token_overlap >= 0.50 {
                score += 0.10;
            }

            if query_is_table_reference {
                match unit.unit_type.as_str() {
                    "table_row" => score += 0.45,
                    "table_cell" => score += 0.20,
                    "sentence_window" => score -= 0.50,
                    _ => {}
                }
            } else if query_mentions_table {
                match unit.unit_type.as_str() {
                    "table_row" => score += 0.22,
                    "table_cell" => score += 0.12,
                    "sentence_window" => score -= 0.15,
                    _ => {}
                }
            }

            if !pinpoint_anchor_compatible(
                unit.citation_anchor_id.as_deref(),
                parent_anchor.as_deref(),
            ) {
                score -= 0.20;
            }
            if unit.text.len() > 1200 {
                score -= 0.04;
            } else if unit.text.len() > 700 {
                score -= 0.02;
            }
            if unit.unit_type == "chunk_snippet" {
                score -= 0.05;
            }

            let token_signature = unit_tokens.join("|");
            let compatible = pinpoint_anchor_compatible(
                unit.citation_anchor_id.as_deref(),
                parent_anchor.as_deref(),
            );

            PinpointUnit {
                unit_id: unit.unit_id,
                unit_type: unit.unit_type,
                score,
                text_preview: normalized_text,
                token_signature,
                char_start: unit.char_start,
                char_end: unit.char_end,
                row_idx: unit.row_idx,
                col_idx: unit.col_idx,
                row_key: unit.row_key,
                origin_node_id: unit.origin_node_id,
                citation_anchor_id: unit.citation_anchor_id,
                citation_anchor_compatible: compatible,
            }
        })
        .collect::<Vec<PinpointUnit>>();

    scored.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| {
                pinpoint_unit_priority(
                    &right.unit_type,
                    query_mentions_table,
                    query_is_table_reference,
                )
                .cmp(&pinpoint_unit_priority(
                    &left.unit_type,
                    query_mentions_table,
                    query_is_table_reference,
                ))
            })
            .then(
                left.row_idx
                    .unwrap_or(i64::MAX)
                    .cmp(&right.row_idx.unwrap_or(i64::MAX)),
            )
            .then(
                left.col_idx
                    .unwrap_or(i64::MAX)
                    .cmp(&right.col_idx.unwrap_or(i64::MAX)),
            )
            .then(left.unit_id.cmp(&right.unit_id))
    });

    if scored.len() > max_units {
        scored.truncate(max_units);
    }

    Ok(PinpointComputation {
        units: scored,
        fallback_used,
    })
}

fn collect_candidate_pinpoint_units(
    connection: &Connection,
    candidate: &QueryCandidate,
) -> Result<Vec<PinpointUnitDraft>> {
    let mut drafts = Vec::<PinpointUnitDraft>::new();

    let (text, table_md, chunk_anchor) = connection
        .query_row(
            "
            SELECT COALESCE(text, ''), COALESCE(table_md, ''), citation_anchor_id
            FROM chunks
            WHERE chunk_id = ?1
            LIMIT 1
            ",
            [candidate.chunk_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .optional()?
        .unwrap_or_else(|| {
            (
                String::new(),
                String::new(),
                candidate.citation_anchor_id.clone(),
            )
        });

    let has_table_structure = candidate.origin_node_id.is_some() || !table_md.trim().is_empty();
    if !has_table_structure {
        drafts.extend(sentence_window_units(
            &candidate.chunk_id,
            &text,
            chunk_anchor.as_deref(),
        ));
    }

    let mut table_units = if let Some(table_node_id) = candidate.origin_node_id.as_deref() {
        table_node_units(connection, &candidate.chunk_id, table_node_id)?
    } else {
        Vec::new()
    };
    if table_units.is_empty() {
        table_units.extend(table_markdown_row_units(
            &candidate.chunk_id,
            &table_md,
            chunk_anchor.as_deref(),
        ));
    }

    drafts.extend(table_units);
    if drafts.is_empty() {
        drafts.extend(sentence_window_units(
            &candidate.chunk_id,
            &text,
            chunk_anchor.as_deref(),
        ));
    }
    Ok(drafts)
}

fn sentence_window_units(
    chunk_id: &str,
    text: &str,
    citation_anchor_id: Option<&str>,
) -> Vec<PinpointUnitDraft> {
    let mut units = Vec::<PinpointUnitDraft>::new();
    let mut start = 0usize;

    for (index, character) in text.char_indices() {
        if !matches!(character, '.' | '!' | '?' | '\n' | ';') {
            continue;
        }
        let end = index + character.len_utf8();
        let slice = text.get(start..end).unwrap_or_default();
        let normalized = condense_whitespace(slice);
        if normalized.len() >= 24 {
            let unit_index = units.len() + 1;
            units.push(PinpointUnitDraft {
                unit_id: format!("{chunk_id}:sentence:{unit_index:03}"),
                unit_type: "sentence_window".to_string(),
                text: normalized,
                char_start: Some(start),
                char_end: Some(end),
                row_idx: None,
                col_idx: None,
                row_key: None,
                origin_node_id: None,
                citation_anchor_id: citation_anchor_id.map(str::to_string),
            });
        }
        start = end;
    }

    if units.is_empty() {
        let normalized = condense_whitespace(text);
        if normalized.len() >= 24 {
            units.push(PinpointUnitDraft {
                unit_id: format!("{chunk_id}:sentence:001"),
                unit_type: "sentence_window".to_string(),
                text: normalized,
                char_start: None,
                char_end: None,
                row_idx: None,
                col_idx: None,
                row_key: None,
                origin_node_id: None,
                citation_anchor_id: citation_anchor_id.map(str::to_string),
            });
        }
    }

    units
}

fn table_node_units(
    connection: &Connection,
    chunk_id: &str,
    table_node_id: &str,
) -> Result<Vec<PinpointUnitDraft>> {
    let mut statement = connection.prepare(
        "
        SELECT
          node_id,
          node_type,
          COALESCE(text, ''),
          row_idx,
          col_idx,
          citation_anchor_id
        FROM nodes
        WHERE table_node_id = ?1
          AND node_type IN ('table_row', 'table_cell')
        ORDER BY row_idx ASC, col_idx ASC, node_id ASC
        LIMIT 512
        ",
    )?;

    let mut rows = statement.query([table_node_id])?;
    let mut units = Vec::<PinpointUnitDraft>::new();
    while let Some(row) = rows.next()? {
        let text = condense_whitespace(&row.get::<_, String>(2)?);
        if text.is_empty() {
            continue;
        }

        let row_idx = row.get::<_, Option<i64>>(3)?;
        let col_idx = row.get::<_, Option<i64>>(4)?;
        units.push(PinpointUnitDraft {
            unit_id: row.get::<_, String>(0)?,
            unit_type: row.get::<_, String>(1)?,
            text,
            char_start: None,
            char_end: None,
            row_idx,
            col_idx,
            row_key: row_idx.map(|value| format!("{chunk_id}:{value}")),
            origin_node_id: row.get::<_, Option<String>>(0)?,
            citation_anchor_id: row.get::<_, Option<String>>(5)?,
        });
    }

    Ok(units)
}
fn table_markdown_row_units(
    chunk_id: &str,
    table_md: &str,
    citation_anchor_id: Option<&str>,
) -> Vec<PinpointUnitDraft> {
    let mut units = Vec::<PinpointUnitDraft>::new();
    let mut row_index = 0i64;
    for line in table_md.lines() {
        if !line.trim_start().starts_with('|') {
            continue;
        }
        let normalized = condense_whitespace(line);
        if normalized.is_empty() {
            continue;
        }

        row_index += 1;
        units.push(PinpointUnitDraft {
            unit_id: format!("{chunk_id}:table_md_row:{row_index:03}"),
            unit_type: "table_row".to_string(),
            text: normalized,
            char_start: None,
            char_end: None,
            row_idx: Some(row_index),
            col_idx: None,
            row_key: Some(format!("{chunk_id}:{row_index}")),
            origin_node_id: None,
            citation_anchor_id: citation_anchor_id.map(str::to_string),
        });
    }
    units
}
fn fallback_pinpoint_unit(candidate: &QueryCandidate) -> PinpointUnitDraft {
    PinpointUnitDraft {
        unit_id: format!("{}:fallback:001", candidate.chunk_id),
        unit_type: "chunk_snippet".to_string(),
        text: condense_whitespace(&candidate.snippet),
        char_start: None,
        char_end: None,
        row_idx: None,
        col_idx: None,
        row_key: None,
        origin_node_id: candidate.origin_node_id.clone(),
        citation_anchor_id: candidate.citation_anchor_id.clone(),
    }
}
fn tokenize_pinpoint_text(value: &str) -> Vec<String> {
    let mut tokens = value
        .to_ascii_lowercase()
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|token| {
            !token.is_empty()
                && (token.len() >= 2 || token.chars().all(|character| character.is_ascii_digit()))
        })
        .filter(|token| {
            PINPOINT_QUERY_STOPWORDS
                .iter()
                .all(|stopword| stopword != token)
        })
        .map(str::to_string)
        .collect::<Vec<String>>();
    tokens.sort();
    tokens.dedup();
    tokens
}
fn token_overlap_ratio(query_tokens: &[String], unit_tokens: &[String]) -> f64 {
    if query_tokens.is_empty() || unit_tokens.is_empty() {
        return 0.0;
    }
    let unit = unit_tokens.iter().collect::<HashSet<&String>>();
    let overlap = query_tokens
        .iter()
        .filter(|token| unit.contains(*token))
        .count();
    overlap as f64 / query_tokens.len() as f64
}
fn phrase_match_bonus(query_phrase: &str, unit_text: &str) -> f64 {
    let query_phrase = query_phrase.trim();
    if query_phrase.len() < 8 {
        return 0.0;
    }
    let unit_text = unit_text.to_ascii_lowercase();
    if unit_text.contains(query_phrase) {
        1.0
    } else {
        0.0
    }
}
fn pinpoint_anchor_compatible(unit_anchor: Option<&str>, parent_anchor: Option<&str>) -> bool {
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
fn anchor_family(anchor: &str) -> Option<(String, String)> {
    let mut parts = anchor.split(':');
    let first = parts.next()?.trim();
    let second = parts.next()?.trim();
    if first.is_empty() || second.is_empty() {
        return None;
    }
    Some((first.to_string(), second.to_string()))
}
fn query_mentions_table_context(query_text: &str) -> bool {
    let lowered = query_text.to_ascii_lowercase();
    lowered.contains("table") || lowered.contains(" row ") || lowered.contains(" cell ")
}
fn looks_like_table_reference_query(query_text: &str) -> bool {
    let lowered = condense_whitespace(query_text).to_ascii_lowercase();
    let mut tokens = lowered.split_whitespace();
    match (tokens.next(), tokens.next(), tokens.next()) {
        (Some("table"), Some(value), None) => {
            value.chars().all(|character| character.is_ascii_digit())
        }
        _ => false,
    }
}
fn pinpoint_unit_priority(unit_type: &str, mentions_table: bool, table_reference: bool) -> i32 {
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
