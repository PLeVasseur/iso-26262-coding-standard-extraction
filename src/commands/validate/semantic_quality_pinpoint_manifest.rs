use super::*;

pub fn load_or_bootstrap_pinpoint_eval_manifest(
    connection: &Connection,
    manifest_dir: &Path,
    semantic_eval_manifest: &SemanticEvalManifest,
) -> Result<PinpointEvalManifest> {
    let path = manifest_dir.join(PINPOINT_EVAL_MANIFEST_FILENAME);
    if path.exists() {
        let raw = fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
        let mut manifest: PinpointEvalManifest = serde_json::from_slice(&raw)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        normalize_pinpoint_eval_manifest(&mut manifest);

        let enriched = enrich_pinpoint_eval_manifest(connection, &mut manifest)?;
        if enriched {
            write_json_pretty(&path, &manifest)?;
        }

        if !manifest.queries.is_empty() {
            return Ok(manifest);
        }
    }

    let mut manifest = bootstrap_pinpoint_eval_manifest(connection, semantic_eval_manifest)?;
    normalize_pinpoint_eval_manifest(&mut manifest);
    write_json_pretty(&path, &manifest)?;
    Ok(manifest)
}

pub fn bootstrap_pinpoint_eval_manifest(
    connection: &Connection,
    semantic_eval_manifest: &SemanticEvalManifest,
) -> Result<PinpointEvalManifest> {
    let mut queries = Vec::<PinpointEvalQuery>::new();
    for semantic_query in &semantic_eval_manifest.queries {
        let Some(parent_chunk_id) = semantic_query.expected_chunk_ids.first().cloned() else {
            continue;
        };

        let chunk_type = resolve_chunk_type(connection, &parent_chunk_id)?.unwrap_or_default();
        let is_table = chunk_type.eq_ignore_ascii_case("table")
            || semantic_query.intent.eq_ignore_ascii_case("table_intent");

        let (expected_unit_ids, expected_row_keys) = if is_table {
            bootstrap_expected_table_units(connection, &parent_chunk_id)?
        } else {
            (Vec::new(), Vec::new())
        };

        let expected_token_sets = build_expected_token_sets(
            connection,
            &semantic_query.query_text,
            semantic_query.expected_refs.first().map(String::as_str),
            &parent_chunk_id,
        )?;

        let intent = if is_table {
            "table_row"
        } else if semantic_query.must_hit_top1
            || semantic_query.intent.eq_ignore_ascii_case("exact_ref")
        {
            "requirement"
        } else {
            "narrative"
        }
        .to_string();

        let high_confidence = semantic_query.must_hit_top1
            || semantic_query.intent.eq_ignore_ascii_case("keyword")
            || semantic_query.intent.eq_ignore_ascii_case("table_intent")
            || is_table;

        queries.push(PinpointEvalQuery {
            query_id: format!("pinpoint-{}", semantic_query.query_id),
            query_text: semantic_query.query_text.clone(),
            parent_expected_chunk_ids: vec![parent_chunk_id],
            expected_unit_ids,
            expected_token_sets,
            expected_row_keys,
            high_confidence,
            intent,
            part_filter: semantic_query.part_filter,
            chunk_type_filter: semantic_query.chunk_type_filter.clone(),
            notes: Some("bootstrap from semantic eval manifest".to_string()),
        });
    }

    Ok(PinpointEvalManifest {
        manifest_version: 1,
        generated_at: now_utc_string(),
        source: PINPOINT_EVAL_MANIFEST_SOURCE.to_string(),
        queries,
    })
}

pub fn normalize_pinpoint_eval_manifest(manifest: &mut PinpointEvalManifest) {
    manifest.queries.retain(|query| {
        !query.query_id.trim().is_empty()
            && !query.query_text.trim().is_empty()
            && !query.parent_expected_chunk_ids.is_empty()
    });

    for query in &mut manifest.queries {
        query.parent_expected_chunk_ids = query
            .parent_expected_chunk_ids
            .iter()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect::<Vec<String>>();
        query.parent_expected_chunk_ids.sort();
        query.parent_expected_chunk_ids.dedup();

        query.expected_unit_ids = query
            .expected_unit_ids
            .iter()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect::<Vec<String>>();
        query.expected_unit_ids.sort();
        query.expected_unit_ids.dedup();

        query.expected_row_keys = query
            .expected_row_keys
            .iter()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect::<Vec<String>>();
        query.expected_row_keys.sort();
        query.expected_row_keys.dedup();

        query.expected_token_sets = query
            .expected_token_sets
            .iter()
            .map(|tokens| {
                let mut normalized = tokens
                    .iter()
                    .map(|token| token.trim().to_ascii_lowercase())
                    .filter(|token| !token.is_empty())
                    .collect::<Vec<String>>();
                normalized.sort();
                normalized.dedup();
                normalized
            })
            .filter(|tokens| !tokens.is_empty())
            .collect::<Vec<Vec<String>>>();
    }

    manifest
        .queries
        .sort_by(|left, right| left.query_id.cmp(&right.query_id));
    manifest
        .queries
        .dedup_by(|left, right| left.query_id == right.query_id);
}

pub fn enrich_pinpoint_eval_manifest(
    connection: &Connection,
    manifest: &mut PinpointEvalManifest,
) -> Result<bool> {
    let mut changed = false;
    for query in &mut manifest.queries {
        if let Some(parent_chunk_id) = query.parent_expected_chunk_ids.first() {
            let supplemental_sets =
                build_expected_token_sets(connection, &query.query_text, None, parent_chunk_id)?;
            for token_set in supplemental_sets {
                if query
                    .expected_token_sets
                    .iter()
                    .all(|existing| existing != &token_set)
                {
                    query.expected_token_sets.push(token_set);
                    changed = true;
                }
            }
        }

        if query.intent.eq_ignore_ascii_case("table_row") {
            if let Some(parent_chunk_id) = query.parent_expected_chunk_ids.first() {
                let (unit_ids, row_keys) =
                    bootstrap_expected_table_units(connection, parent_chunk_id)?;
                for unit_id in unit_ids {
                    if query
                        .expected_unit_ids
                        .iter()
                        .all(|value| value != &unit_id)
                    {
                        query.expected_unit_ids.push(unit_id);
                        changed = true;
                    }
                }
                for row_key in row_keys {
                    if query
                        .expected_row_keys
                        .iter()
                        .all(|value| value != &row_key)
                    {
                        query.expected_row_keys.push(row_key);
                        changed = true;
                    }
                }
                if !query.expected_row_keys.is_empty() {
                    query.expected_unit_ids.sort();
                    query.expected_unit_ids.dedup();
                    query.expected_row_keys.sort();
                    query.expected_row_keys.dedup();
                }
                if query.expected_row_keys.is_empty() {
                    let heading_tokens = tokenize_pinpoint_value(&query.query_text);
                    if is_useful_expected_token_set(&heading_tokens)
                        && query
                            .expected_token_sets
                            .iter()
                            .all(|existing| existing != &heading_tokens)
                    {
                        query.expected_token_sets.push(heading_tokens);
                        changed = true;
                    }
                }
            }
        }
    }

    normalize_pinpoint_eval_manifest(manifest);
    Ok(changed)
}

pub fn bootstrap_expected_table_units(
    connection: &Connection,
    parent_chunk_id: &str,
) -> Result<(Vec<String>, Vec<String>)> {
    let Some(table_node_id) = resolve_table_node_id(connection, parent_chunk_id)? else {
        return Ok((Vec::new(), Vec::new()));
    };

    let mut statement = connection.prepare(
        "
        SELECT node_id, row_idx
        FROM nodes
        WHERE table_node_id = ?1
          AND node_type = 'table_row'
          AND row_idx IS NOT NULL
        ORDER BY row_idx ASC, node_id ASC
        LIMIT ?2
        ",
    )?;
    let mut rows = statement.query(params![table_node_id, PINPOINT_TABLE_ROW_LIMIT as i64])?;

    let mut unit_ids = Vec::<String>::new();
    let mut row_keys = Vec::<String>::new();
    while let Some(row) = rows.next()? {
        let unit_id = row.get::<_, String>(0)?;
        let row_idx = row.get::<_, i64>(1)?;
        unit_ids.push(unit_id);
        row_keys.push(format!("{parent_chunk_id}:{row_idx}"));
    }
    Ok((unit_ids, row_keys))
}

pub fn resolve_chunk_type(connection: &Connection, chunk_id: &str) -> Result<Option<String>> {
    connection
        .query_row(
            "SELECT type FROM chunks WHERE chunk_id = ?1 LIMIT 1",
            [chunk_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()
        .map(|value| value.flatten())
        .map_err(Into::into)
}

pub fn resolve_table_node_id(connection: &Connection, chunk_id: &str) -> Result<Option<String>> {
    connection
        .query_row(
            "SELECT origin_node_id FROM chunks WHERE chunk_id = ?1 LIMIT 1",
            [chunk_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()
        .map(|value| value.flatten())
        .map_err(Into::into)
}

pub fn build_expected_token_sets(
    connection: &Connection,
    query_text: &str,
    reference_hint: Option<&str>,
    parent_chunk_id: &str,
) -> Result<Vec<Vec<String>>> {
    let mut token_sets = Vec::<Vec<String>>::new();

    let query_tokens = compact_expected_token_set(tokenize_pinpoint_value(query_text), 4);
    if is_useful_expected_token_set(&query_tokens) {
        token_sets.push(query_tokens);
    }

    if let Some(reference_hint) = reference_hint {
        let reference_tokens =
            compact_expected_token_set(tokenize_pinpoint_value(reference_hint), 3);
        if is_useful_expected_token_set(&reference_tokens) {
            token_sets.push(reference_tokens);
        }
    }

    if let Some((reference, heading, text)) =
        resolve_chunk_pinpoint_hints(connection, parent_chunk_id)?
    {
        let reference_tokens = compact_expected_token_set(tokenize_pinpoint_value(&reference), 3);
        if is_useful_expected_token_set(&reference_tokens) {
            token_sets.push(reference_tokens);
        }

        let heading_tokens = compact_expected_token_set(tokenize_pinpoint_value(&heading), 4);
        if is_useful_expected_token_set(&heading_tokens) {
            token_sets.push(heading_tokens);
        }

        let sentence_tokens = compact_expected_token_set(primary_chunk_sentence_tokens(&text), 5);
        if is_useful_expected_token_set(&sentence_tokens) {
            token_sets.push(sentence_tokens);
        }
    }

    for token_set in &mut token_sets {
        token_set.sort();
        token_set.dedup();
    }
    token_sets.retain(|token_set| !token_set.is_empty());
    token_sets.sort();
    token_sets.dedup();

    Ok(token_sets)
}

pub fn resolve_chunk_pinpoint_hints(
    connection: &Connection,
    chunk_id: &str,
) -> Result<Option<(String, String, String)>> {
    connection
        .query_row(
            "
            SELECT COALESCE(ref, ''), COALESCE(heading, ''), COALESCE(text, '')
            FROM chunks
            WHERE chunk_id = ?1
            LIMIT 1
            ",
            [chunk_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .map_err(Into::into)
}

pub fn primary_chunk_sentence_tokens(text: &str) -> Vec<String> {
    let sentence = text
        .split(|character| matches!(character, '.' | '!' | '?' | ';' | '\n'))
        .map(str::trim)
        .find(|value| !value.is_empty())
        .unwrap_or_default();
    tokenize_pinpoint_value(sentence)
}

pub fn compact_expected_token_set(tokens: Vec<String>, max_tokens: usize) -> Vec<String> {
    let mut ranked = tokens;
    ranked.sort_by(|left, right| right.len().cmp(&left.len()).then(left.cmp(right)));
    ranked.truncate(max_tokens);
    ranked.sort();
    ranked.dedup();
    ranked
}

pub fn is_useful_expected_token_set(tokens: &[String]) -> bool {
    !tokens.is_empty()
        && tokens.iter().any(|token| {
            token
                .chars()
                .any(|character| character.is_ascii_alphabetic())
        })
}
