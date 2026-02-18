fn open_embed_connection(db_path: &PathBuf) -> Result<Connection> {
    let connection = Connection::open_with_flags(
        db_path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| {
        format!(
            "failed to open database for embedding: {}",
            db_path.display()
        )
    })?;

    connection
        .pragma_update(None, "journal_mode", "WAL")
        .context("failed to set journal_mode=WAL for embed")?;
    connection
        .pragma_update(None, "synchronous", "NORMAL")
        .context("failed to set synchronous=NORMAL for embed")?;

    Ok(connection)
}

fn ensure_model_entry(connection: &Connection, model: &SemanticModelConfig) -> Result<()> {
    let created_at = now_utc_string();
    let config_json = serde_json::json!({
        "model_id": model.model_id,
        "model_name": model.model_name,
        "dimensions": model.dimensions,
        "normalization": model.normalization,
        "backend": model.backend,
    })
    .to_string();

    connection.execute(
        "
        INSERT INTO embedding_models(model_id, backend, model_name, dimensions, normalize, created_at, config_json)
        VALUES(?1, ?2, ?3, ?4, 1, ?5, ?6)
        ON CONFLICT(model_id) DO UPDATE SET
          backend=excluded.backend,
          model_name=excluded.model_name,
          dimensions=excluded.dimensions,
          normalize=excluded.normalize,
          config_json=excluded.config_json
        ",
        params![
            model.model_id,
            model.backend,
            model.model_name,
            model.dimensions as i64,
            created_at,
            config_json,
        ],
    )?;

    Ok(())
}

fn load_chunk_rows(connection: &Connection) -> Result<Vec<EmbedChunkRow>> {
    let mut statement = connection.prepare(
        "
        SELECT
          chunk_id,
          lower(COALESCE(type, '')),
          COALESCE(ref, ''),
          COALESCE(heading, ''),
          text,
          table_md
        FROM chunks
        ORDER BY chunk_id ASC
        ",
    )?;

    let mut rows = statement.query([])?;
    let mut out = Vec::<EmbedChunkRow>::new();

    while let Some(row) = rows.next()? {
        out.push(EmbedChunkRow {
            chunk_id: row.get(0)?,
            chunk_type: row.get(1)?,
            reference: row.get(2)?,
            heading: row.get(3)?,
            text: row.get(4)?,
            table_md: row.get(5)?,
        });
    }

    Ok(out)
}

fn load_existing_embedding(
    connection: &Connection,
    chunk_id: &str,
    model_id: &str,
) -> Result<Option<ExistingEmbeddingRow>> {
    let row = connection
        .query_row(
            "
            SELECT text_hash, embedding_dim
            FROM chunk_embeddings
            WHERE chunk_id = ?1 AND model_id = ?2
            LIMIT 1
            ",
            params![chunk_id, model_id],
            |row| {
                Ok(ExistingEmbeddingRow {
                    text_hash: row.get(0)?,
                    embedding_dim: row.get::<_, i64>(1)? as usize,
                })
            },
        )
        .ok();

    Ok(row)
}

fn upsert_chunk_embedding(
    connection: &Connection,
    chunk_id: &str,
    model_id: &str,
    embedding_blob: &[u8],
    embedding_dim: usize,
    text_hash: &str,
) -> Result<()> {
    connection.execute(
        "
        INSERT INTO chunk_embeddings(chunk_id, model_id, embedding, embedding_dim, text_hash, generated_at)
        VALUES(?1, ?2, ?3, ?4, ?5, ?6)
        ON CONFLICT(chunk_id, model_id) DO UPDATE SET
          embedding=excluded.embedding,
          embedding_dim=excluded.embedding_dim,
          text_hash=excluded.text_hash,
          generated_at=excluded.generated_at
        ",
        params![
            chunk_id,
            model_id,
            embedding_blob,
            embedding_dim as i64,
            text_hash,
            now_utc_string(),
        ],
    )?;

    Ok(())
}
