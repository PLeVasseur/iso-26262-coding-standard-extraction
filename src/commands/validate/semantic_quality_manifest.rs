fn load_or_bootstrap_semantic_eval_manifest(
    connection: &Connection,
    manifest_dir: &Path,
    refs: &[GoldReference],
) -> Result<SemanticEvalManifest> {
    let path = manifest_dir.join(SEMANTIC_EVAL_MANIFEST_FILENAME);
    if path.exists() {
        let raw = fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
        let mut manifest: SemanticEvalManifest = serde_json::from_slice(&raw)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        normalize_semantic_eval_manifest(&mut manifest);
        let enriched = fill_missing_judged_chunk_ids(connection, &mut manifest)?;
        if enriched {
            write_json_pretty(&path, &manifest)?;
        }
        if !manifest.queries.is_empty() {
            return Ok(manifest);
        }
    }

    let manifest = bootstrap_semantic_eval_manifest(connection, refs)?;
    write_json_pretty(&path, &manifest)?;
    Ok(manifest)
}

fn bootstrap_semantic_eval_manifest(
    connection: &Connection,
    refs: &[GoldReference],
) -> Result<SemanticEvalManifest> {
    let mut queries = Vec::<SemanticEvalQuery>::new();
    let mut ordered_refs = refs.iter().collect::<Vec<&GoldReference>>();
    ordered_refs.sort_by(|left, right| left.id.cmp(&right.id));

    let mut exact_count = 0usize;
    let mut keyword_count = 0usize;
    let mut paraphrase_count = 0usize;
    let mut concept_count = 0usize;

    for reference in ordered_refs {
        let Some(expected_chunk_id) =
            resolve_reference_chunk_id(connection, &reference.doc_id, &reference.reference)?
        else {
            continue;
        };
        let part_filter = resolve_doc_part(connection, &reference.doc_id)?;

        exact_count += 1;
        let judged_chunk_ids = bootstrap_judged_chunk_ids(connection, &expected_chunk_id)?;
        queries.push(SemanticEvalQuery {
            query_id: format!("exact-ref-{exact_count:03}"),
            query_text: reference.reference.clone(),
            intent: "exact_ref".to_string(),
            expected_chunk_ids: vec![expected_chunk_id.clone()],
            judged_chunk_ids: judged_chunk_ids.clone(),
            expected_refs: vec![reference.reference.clone()],
            must_hit_top1: true,
            part_filter,
            chunk_type_filter: None,
            notes: Some("bootstrap from gold reference".to_string()),
        });

        if !reference.must_match_terms.is_empty() {
            let keyword_text = reference
                .must_match_terms
                .iter()
                .take(4)
                .cloned()
                .collect::<Vec<String>>()
                .join(" ");
            if !keyword_text.trim().is_empty() {
                keyword_count += 1;
                queries.push(SemanticEvalQuery {
                    query_id: format!("keyword-{keyword_count:03}"),
                    query_text: keyword_text.clone(),
                    intent: "keyword".to_string(),
                    expected_chunk_ids: vec![expected_chunk_id.clone()],
                    judged_chunk_ids: judged_chunk_ids.clone(),
                    expected_refs: vec![reference.reference.clone()],
                    must_hit_top1: false,
                    part_filter,
                    chunk_type_filter: None,
                    notes: Some("bootstrap keyword intent".to_string()),
                });

                paraphrase_count += 1;
                queries.push(SemanticEvalQuery {
                    query_id: format!("paraphrase-{paraphrase_count:03}"),
                    query_text: format!("requirements concerning {keyword_text}"),
                    intent: "paraphrase".to_string(),
                    expected_chunk_ids: vec![expected_chunk_id.clone()],
                    judged_chunk_ids: judged_chunk_ids.clone(),
                    expected_refs: vec![reference.reference.clone()],
                    must_hit_top1: false,
                    part_filter,
                    chunk_type_filter: None,
                    notes: Some("bootstrap paraphrase intent".to_string()),
                });

                concept_count += 1;
                let concept_token = reference
                    .must_match_terms
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "safety".to_string());
                queries.push(SemanticEvalQuery {
                    query_id: format!("concept-{concept_count:03}"),
                    query_text: format!("concept guidance for {concept_token}"),
                    intent: "concept".to_string(),
                    expected_chunk_ids: vec![expected_chunk_id],
                    judged_chunk_ids,
                    expected_refs: vec![reference.reference.clone()],
                    must_hit_top1: false,
                    part_filter,
                    chunk_type_filter: None,
                    notes: Some("bootstrap concept intent".to_string()),
                });
            }
        }
    }

    if !queries
        .iter()
        .any(|query| !query.intent.eq_ignore_ascii_case("exact_ref"))
        && let Some(seed) = queries
            .iter()
            .find(|query| query.intent.eq_ignore_ascii_case("exact_ref"))
            .cloned()
    {
        queries.push(SemanticEvalQuery {
            query_id: "concept-seed-001".to_string(),
            query_text: format!("requirements related to {}", seed.query_text),
            intent: "concept".to_string(),
            expected_chunk_ids: seed.expected_chunk_ids,
            judged_chunk_ids: seed.judged_chunk_ids,
            expected_refs: seed.expected_refs,
            must_hit_top1: false,
            part_filter: seed.part_filter,
            chunk_type_filter: seed.chunk_type_filter,
            notes: Some("fallback concept seed".to_string()),
        });
    }

    if queries.is_empty()
        && let Some((chunk_id, reference, part)) = fallback_seed_chunk(connection)?
    {
        queries.push(SemanticEvalQuery {
            query_id: "exact-ref-seed-001".to_string(),
            query_text: reference.clone(),
            intent: "exact_ref".to_string(),
            expected_chunk_ids: vec![chunk_id.clone()],
            judged_chunk_ids: vec![chunk_id.clone()],
            expected_refs: vec![reference.clone()],
            must_hit_top1: true,
            part_filter: Some(part),
            chunk_type_filter: None,
            notes: Some("fallback seed from first chunk".to_string()),
        });
        queries.push(SemanticEvalQuery {
            query_id: "concept-seed-001".to_string(),
            query_text: format!("requirements around {reference}"),
            intent: "concept".to_string(),
            expected_chunk_ids: vec![chunk_id],
            judged_chunk_ids: vec![],
            expected_refs: vec![reference],
            must_hit_top1: false,
            part_filter: Some(part),
            chunk_type_filter: None,
            notes: Some("fallback concept seed from first chunk".to_string()),
        });
    }

    let mut manifest = SemanticEvalManifest {
        manifest_version: 1,
        generated_at: now_utc_string(),
        source: SEMANTIC_EVAL_MANIFEST_SOURCE.to_string(),
        queries,
    };
    normalize_semantic_eval_manifest(&mut manifest);
    Ok(manifest)
}

fn normalize_semantic_eval_manifest(manifest: &mut SemanticEvalManifest) {
    manifest.queries.retain(|query| {
        !query.query_id.trim().is_empty()
            && !query.query_text.trim().is_empty()
            && !query.expected_chunk_ids.is_empty()
    });

    for query in &mut manifest.queries {
        query.expected_chunk_ids = query
            .expected_chunk_ids
            .iter()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect::<Vec<String>>();
        query.expected_chunk_ids.sort();
        query.expected_chunk_ids.dedup();

        query.judged_chunk_ids = query
            .judged_chunk_ids
            .iter()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .collect::<Vec<String>>();
        query.judged_chunk_ids.sort();
        query.judged_chunk_ids.dedup();
    }

    manifest.queries.sort_by(|left, right| {
        left.query_id
            .cmp(&right.query_id)
            .then(left.query_text.cmp(&right.query_text))
    });
    manifest
        .queries
        .dedup_by(|left, right| left.query_id == right.query_id);
}

fn fill_missing_judged_chunk_ids(
    connection: &Connection,
    manifest: &mut SemanticEvalManifest,
) -> Result<bool> {
    let mut changed = false;
    for query in &mut manifest.queries {
        if !query.judged_chunk_ids.is_empty() {
            continue;
        }
        let Some(seed_chunk_id) = query.expected_chunk_ids.first().cloned() else {
            continue;
        };
        query.judged_chunk_ids = bootstrap_judged_chunk_ids(connection, &seed_chunk_id)?;
        changed = true;
    }
    Ok(changed)
}

fn bootstrap_judged_chunk_ids(connection: &Connection, expected_chunk_id: &str) -> Result<Vec<String>> {
    let mut statement = connection.prepare(
        "
        WITH seed AS (
          SELECT doc_id, COALESCE(page_pdf_start, 0) AS seed_page
          FROM chunks
          WHERE chunk_id = ?1
          LIMIT 1
        )
        SELECT c.chunk_id
        FROM chunks c
        JOIN seed s ON s.doc_id = c.doc_id
        ORDER BY
          CASE WHEN c.chunk_id = ?1 THEN 0 ELSE 1 END,
          abs(COALESCE(c.page_pdf_start, s.seed_page) - s.seed_page) ASC,
          c.chunk_id ASC
        LIMIT 10
        ",
    )?;

    let rows = statement.query_map([expected_chunk_id], |row| row.get::<_, String>(0))?;
    let mut chunk_ids = Vec::<String>::new();
    for row in rows {
        chunk_ids.push(row?);
    }
    if !chunk_ids.iter().any(|chunk_id| chunk_id == expected_chunk_id) {
        chunk_ids.insert(0, expected_chunk_id.to_string());
    }
    chunk_ids.sort();
    chunk_ids.dedup();
    Ok(chunk_ids)
}

fn resolve_reference_chunk_id(
    connection: &Connection,
    doc_id: &str,
    reference: &str,
) -> Result<Option<String>> {
    let exact = connection
        .query_row(
            "
            SELECT chunk_id
            FROM chunks
            WHERE doc_id = ?1 AND lower(COALESCE(ref, '')) = lower(?2)
            ORDER BY page_pdf_start ASC, chunk_id ASC
            LIMIT 1
            ",
            params![doc_id, reference],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if exact.is_some() {
        return Ok(exact);
    }

    let contains = connection
        .query_row(
            "
            SELECT chunk_id
            FROM chunks
            WHERE doc_id = ?1
              AND (
                lower(COALESCE(ref, '')) LIKE '%' || lower(?2) || '%'
                OR lower(COALESCE(heading, '')) LIKE '%' || lower(?2) || '%'
              )
            ORDER BY page_pdf_start ASC, chunk_id ASC
            LIMIT 1
            ",
            params![doc_id, reference],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    Ok(contains)
}

fn resolve_doc_part(connection: &Connection, doc_id: &str) -> Result<Option<u32>> {
    let part = connection
        .query_row(
            "SELECT part FROM docs WHERE doc_id = ?1 LIMIT 1",
            [doc_id],
            |row| row.get::<_, u32>(0),
        )
        .optional()?;
    Ok(part)
}

fn fallback_seed_chunk(connection: &Connection) -> Result<Option<(String, String, u32)>> {
    let row = connection
        .query_row(
            "
            SELECT c.chunk_id, COALESCE(c.ref, ''), d.part
            FROM chunks c
            JOIN docs d ON d.doc_id = c.doc_id
            ORDER BY d.part ASC, c.page_pdf_start ASC, c.chunk_id ASC
            LIMIT 1
            ",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, u32>(2)?,
                ))
            },
        )
        .optional()?;
    Ok(row)
}
