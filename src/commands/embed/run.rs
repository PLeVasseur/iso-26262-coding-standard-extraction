pub fn run(args: EmbedArgs) -> Result<()> {
    let batch_size = args.batch_size.max(1);
    let model = resolve_model_config(&args.model_id);
    let chunk_type_filter = resolve_chunk_type_filter(&args.chunk_types);

    let db_path = args
        .db_path
        .clone()
        .unwrap_or_else(|| args.cache_root.join("iso26262_index.sqlite"));
    let manifest_dir = args.cache_root.join("manifests");
    ensure_directory(&manifest_dir)?;
    let semantic_model_lock_path = args
        .semantic_model_lock_path
        .clone()
        .unwrap_or_else(|| PathBuf::from(SEMANTIC_MODEL_CONFIG_LOCK_PATH));

    let mut connection = open_embed_connection(&db_path)?;
    ensure_embedding_schema(&connection)?;
    ensure_model_entry(&connection, &model)?;
    write_semantic_model_config_lockfile(&model, &semantic_model_lock_path)?;

    let chunk_rows = load_chunk_rows(&connection)?;
    let started_at = now_utc_string();
    let started = Instant::now();
    let run_id = format!("embed-{}", utc_compact_string(Utc::now()));

    let mut eligible_chunks = 0usize;
    let mut skipped_empty_chunks = 0usize;
    let mut stale_rows_before = 0usize;
    let mut updated_chunks = 0usize;
    let mut pending_updates = Vec::<(String, String, Vec<u8>)>::new();
    let mut warnings = Vec::<String>::new();

    for row in &chunk_rows {
        let payload = build_chunk_payload(row);
        let chunk_type = row.chunk_type.as_str();
        if !is_supported_chunk_type(chunk_type)
            || (!chunk_type_filter.is_empty() && !chunk_type_filter.contains(chunk_type))
        {
            continue;
        }

        if !is_eligible_chunk(chunk_type, &chunk_type_filter, &payload) {
            skipped_empty_chunks += 1;
            continue;
        }

        eligible_chunks += 1;

        let Some(payload) = payload else {
            continue;
        };

        let text_hash = embedding_text_hash(&payload);
        let existing = load_existing_embedding(&connection, &row.chunk_id, &model.model_id)?;
        let stale = existing
            .as_ref()
            .map(|value| value.text_hash != text_hash || value.embedding_dim != model.dimensions)
            .unwrap_or(true);
        if stale {
            stale_rows_before += 1;
        }

        let should_update = match args.refresh_mode {
            EmbedRefreshMode::Full => true,
            EmbedRefreshMode::MissingOrStale => stale,
        };

        if !should_update {
            continue;
        }

        let embedding = embed_text_local(&payload, model.dimensions);
        let embedding_blob = encode_embedding_blob(&embedding);
        pending_updates.push((row.chunk_id.clone(), text_hash, embedding_blob));

        if pending_updates.len() >= batch_size {
            updated_chunks += flush_embed_batch(
                &mut connection,
                &model.model_id,
                model.dimensions,
                &mut pending_updates,
            )?;
            info!(
                model_id = %model.model_id,
                updated_chunks,
                eligible_chunks,
                "embed batch committed"
            );
        }
    }

    if !pending_updates.is_empty() {
        updated_chunks += flush_embed_batch(
            &mut connection,
            &model.model_id,
            model.dimensions,
            &mut pending_updates,
        )?;
    }

    if eligible_chunks == 0 {
        warnings.push("no eligible chunks matched embed filters".to_string());
    }

    let stale_rows_after = count_stale_rows(
        &connection,
        &chunk_rows,
        &chunk_type_filter,
        &model.model_id,
        model.dimensions,
    )?;
    let embedded_chunks = eligible_chunks.saturating_sub(stale_rows_after);
    let duration_ms = started.elapsed().as_millis();

    let manifest = EmbeddingRunManifest {
        manifest_version: 1,
        run_id,
        generated_at: started_at,
        model_id: model.model_id.clone(),
        model_name: model.model_name.clone(),
        embedding_dim: model.dimensions,
        normalization: model.normalization.clone(),
        backend: model.backend.clone(),
        db_schema_version: EMBEDDING_DB_SCHEMA_VERSION.to_string(),
        refresh_mode: match args.refresh_mode {
            EmbedRefreshMode::Full => "full",
            EmbedRefreshMode::MissingOrStale => "missing-or-stale",
        }
        .to_string(),
        chunk_type_filter: if chunk_type_filter.is_empty() {
            vec![
                "clause".to_string(),
                "annex".to_string(),
                "table".to_string(),
            ]
        } else {
            let mut values = chunk_type_filter.iter().cloned().collect::<Vec<String>>();
            values.sort();
            values
        },
        eligible_chunks,
        embedded_chunks,
        updated_chunks,
        skipped_empty_chunks,
        stale_rows_before,
        stale_rows_after,
        batch_size,
        duration_ms,
        status: "completed".to_string(),
        warnings,
    };

    let manifest_path = manifest_dir.join(format!(
        "embedding_run_{}.json",
        utc_compact_string(Utc::now())
    ));
    write_json_pretty(&manifest_path, &manifest)?;

    info!(
        path = %manifest_path.display(),
        model_id = %model.model_id,
        eligible_chunks,
        updated_chunks,
        stale_rows_after,
        "embedding refresh completed"
    );

    Ok(())
}

fn write_semantic_model_config_lockfile(
    model: &SemanticModelConfig,
    lock_path: &Path,
) -> Result<()> {
    let created_at = now_utc_string();
    let checksum_input = format!(
        "{}|{}|{}|{}|{}",
        model.model_id, model.model_name, model.dimensions, model.normalization, model.backend
    );
    let mut hasher = Sha256::new();
    hasher.update(checksum_input.as_bytes());
    let checksum = format!("{:x}", hasher.finalize());

    let lock = SemanticModelConfigLock {
        manifest_version: 1,
        model_id: model.model_id.clone(),
        model_name: model.model_name.clone(),
        embedding_dim: model.dimensions,
        normalization: model.normalization.clone(),
        runtime_backend: model.backend.clone(),
        created_at,
        checksum,
    };

    if let Some(parent) = lock_path.parent() {
        if !parent.as_os_str().is_empty() {
            ensure_directory(parent)?;
        }
    }

    write_json_pretty(lock_path, &lock)
}

fn flush_embed_batch(
    connection: &mut Connection,
    model_id: &str,
    dimensions: usize,
    pending_updates: &mut Vec<(String, String, Vec<u8>)>,
) -> Result<usize> {
    if pending_updates.is_empty() {
        return Ok(0);
    }

    let tx = connection.transaction()?;
    let mut updated = 0usize;
    for (chunk_id, text_hash, embedding_blob) in pending_updates.drain(..) {
        upsert_chunk_embedding(
            &tx,
            &chunk_id,
            model_id,
            &embedding_blob,
            dimensions,
            &text_hash,
        )?;
        updated += 1;
    }
    tx.commit()?;

    Ok(updated)
}

fn count_stale_rows(
    connection: &Connection,
    chunk_rows: &[EmbedChunkRow],
    chunk_type_filter: &HashSet<String>,
    model_id: &str,
    dimensions: usize,
) -> Result<usize> {
    let mut stale = 0usize;

    for row in chunk_rows {
        let payload = build_chunk_payload(row);
        let chunk_type = row.chunk_type.as_str();
        if !is_eligible_chunk(chunk_type, chunk_type_filter, &payload) {
            continue;
        }

        let Some(payload) = payload else {
            continue;
        };
        let text_hash = embedding_text_hash(&payload);

        let existing = load_existing_embedding(connection, &row.chunk_id, model_id)?;
        let is_stale = existing
            .as_ref()
            .map(|value| value.text_hash != text_hash || value.embedding_dim != dimensions)
            .unwrap_or(true);
        if is_stale {
            stale += 1;
        }
    }

    Ok(stale)
}
