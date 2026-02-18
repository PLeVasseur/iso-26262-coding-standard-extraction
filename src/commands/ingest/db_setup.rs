fn configure_connection(connection: &Connection) -> Result<()> {
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .context("failed to set journal_mode=WAL")?;
    connection
        .pragma_update(None, "synchronous", "NORMAL")
        .context("failed to set synchronous=NORMAL")?;
    Ok(())
}

fn ensure_schema(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS metadata (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS docs (
          doc_id TEXT PRIMARY KEY,
          filename TEXT NOT NULL,
          sha256 TEXT NOT NULL,
          part INTEGER,
          year INTEGER,
          title TEXT
        );

        CREATE TABLE IF NOT EXISTS nodes (
          node_id TEXT PRIMARY KEY,
          parent_node_id TEXT,
          doc_id TEXT NOT NULL,
          node_type TEXT NOT NULL,
          ref TEXT,
          ref_path TEXT,
          heading TEXT,
          order_index INTEGER DEFAULT 0,
          page_pdf_start INTEGER,
          page_pdf_end INTEGER,
          text TEXT,
          source_hash TEXT,
          ancestor_path TEXT,
          anchor_type TEXT,
          anchor_label_raw TEXT,
          anchor_label_norm TEXT,
          anchor_order INTEGER,
          citation_anchor_id TEXT,
          list_depth INTEGER,
          list_marker_style TEXT,
          item_index INTEGER,
          table_node_id TEXT,
          row_idx INTEGER,
          col_idx INTEGER,
          is_header INTEGER,
          row_span INTEGER,
          col_span INTEGER,
          FOREIGN KEY(doc_id) REFERENCES docs(doc_id),
          FOREIGN KEY(parent_node_id) REFERENCES nodes(node_id)
        );

        CREATE TABLE IF NOT EXISTS chunks (
          chunk_id TEXT PRIMARY KEY,
          doc_id TEXT NOT NULL,
          type TEXT NOT NULL,
          ref TEXT,
          ref_path TEXT,
          heading TEXT,
          chunk_seq INTEGER DEFAULT 0,
          page_pdf_start INTEGER,
          page_pdf_end INTEGER,
          page_printed_start TEXT,
          page_printed_end TEXT,
          text TEXT,
          table_md TEXT,
          table_csv TEXT,
          source_hash TEXT,
          origin_node_id TEXT,
          leaf_node_type TEXT,
          ancestor_path TEXT,
          anchor_type TEXT,
          anchor_label_raw TEXT,
          anchor_label_norm TEXT,
          anchor_order INTEGER,
          citation_anchor_id TEXT,
          FOREIGN KEY(doc_id) REFERENCES docs(doc_id)
        );
        ",
    )?;

    ensure_column_exists(connection, "nodes", "ancestor_path TEXT")?;
    ensure_column_exists(connection, "nodes", "anchor_type TEXT")?;
    ensure_column_exists(connection, "nodes", "anchor_label_raw TEXT")?;
    ensure_column_exists(connection, "nodes", "anchor_label_norm TEXT")?;
    ensure_column_exists(connection, "nodes", "anchor_order INTEGER")?;
    ensure_column_exists(connection, "nodes", "citation_anchor_id TEXT")?;
    ensure_column_exists(connection, "nodes", "list_depth INTEGER")?;
    ensure_column_exists(connection, "nodes", "list_marker_style TEXT")?;
    ensure_column_exists(connection, "nodes", "item_index INTEGER")?;
    ensure_column_exists(connection, "nodes", "table_node_id TEXT")?;
    ensure_column_exists(connection, "nodes", "row_idx INTEGER")?;
    ensure_column_exists(connection, "nodes", "col_idx INTEGER")?;
    ensure_column_exists(connection, "nodes", "is_header INTEGER")?;
    ensure_column_exists(connection, "nodes", "row_span INTEGER")?;
    ensure_column_exists(connection, "nodes", "col_span INTEGER")?;
    ensure_column_exists(connection, "chunks", "origin_node_id TEXT")?;
    ensure_column_exists(connection, "chunks", "leaf_node_type TEXT")?;
    ensure_column_exists(connection, "chunks", "ancestor_path TEXT")?;
    ensure_column_exists(connection, "chunks", "anchor_type TEXT")?;
    ensure_column_exists(connection, "chunks", "anchor_label_raw TEXT")?;
    ensure_column_exists(connection, "chunks", "anchor_label_norm TEXT")?;
    ensure_column_exists(connection, "chunks", "anchor_order INTEGER")?;
    ensure_column_exists(connection, "chunks", "citation_anchor_id TEXT")?;

    connection
        .execute(
            "
            CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts
            USING fts5(chunk_id, doc_id, ref, heading, text, content='chunks', content_rowid='rowid')
            ",
            [],
        )
        .context("failed to initialize FTS5 table chunks_fts")?;

    connection.execute_batch(
        "
        CREATE INDEX IF NOT EXISTS idx_nodes_parent ON nodes(parent_node_id);
        CREATE INDEX IF NOT EXISTS idx_nodes_doc_type ON nodes(doc_id, node_type);
        CREATE INDEX IF NOT EXISTS idx_nodes_doc_parent_order ON nodes(doc_id, parent_node_id, order_index);
        CREATE INDEX IF NOT EXISTS idx_nodes_doc_citation_anchor ON nodes(doc_id, citation_anchor_id);
        CREATE INDEX IF NOT EXISTS idx_nodes_table_semantics ON nodes(table_node_id, row_idx, col_idx);
        CREATE INDEX IF NOT EXISTS idx_chunks_origin_node ON chunks(origin_node_id);
        CREATE INDEX IF NOT EXISTS idx_chunks_doc_citation_anchor ON chunks(doc_id, citation_anchor_id);
        CREATE INDEX IF NOT EXISTS idx_chunks_doc_ref_anchor_label ON chunks(doc_id, ref, anchor_label_norm);
        ",
    )?;

    ensure_embedding_schema(connection)?;

    let now = now_utc_string();
    connection.execute(
        "INSERT INTO metadata(key, value) VALUES('db_schema_version', ?1)
         ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        [DB_SCHEMA_VERSION],
    )?;
    connection.execute(
        "INSERT INTO metadata(key, value) VALUES('db_updated_at', ?1)
         ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        [now],
    )?;

    Ok(())
}

pub(crate) fn ensure_embedding_schema(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS embedding_models (
          model_id TEXT PRIMARY KEY,
          backend TEXT NOT NULL,
          model_name TEXT NOT NULL,
          dimensions INTEGER NOT NULL,
          normalize INTEGER NOT NULL,
          created_at TEXT NOT NULL,
          config_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS chunk_embeddings (
          chunk_id TEXT NOT NULL,
          model_id TEXT NOT NULL,
          embedding BLOB NOT NULL,
          embedding_dim INTEGER NOT NULL,
          text_hash TEXT NOT NULL,
          generated_at TEXT NOT NULL,
          PRIMARY KEY (chunk_id, model_id),
          FOREIGN KEY (chunk_id) REFERENCES chunks(chunk_id) ON DELETE CASCADE,
          FOREIGN KEY (model_id) REFERENCES embedding_models(model_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_chunk_embeddings_model ON chunk_embeddings(model_id);
        CREATE INDEX IF NOT EXISTS idx_chunk_embeddings_chunk ON chunk_embeddings(chunk_id);
        CREATE INDEX IF NOT EXISTS idx_chunk_embeddings_model_hash ON chunk_embeddings(model_id, text_hash);
        ",
    )?;

    Ok(())
}

fn ensure_column_exists(
    connection: &Connection,
    table_name: &str,
    column_definition: &str,
) -> Result<()> {
    let Some(column_name) = column_definition.split_whitespace().next() else {
        bail!("invalid column definition: {column_definition}");
    };

    let pragma_sql = format!("PRAGMA table_info({table_name})");
    let mut statement = connection
        .prepare(&pragma_sql)
        .with_context(|| format!("failed to inspect schema for table {table_name}"))?;

    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let existing_name: String = row.get(1)?;
        if existing_name == column_name {
            return Ok(());
        }
    }

    let alter_sql = format!("ALTER TABLE {table_name} ADD COLUMN {column_definition}");
    connection
        .execute(&alter_sql, [])
        .with_context(|| format!("failed to add column {column_name} on {table_name}"))?;

    Ok(())
}

fn upsert_docs(connection: &mut Connection, inventory: &PdfInventoryManifest) -> Result<usize> {
    let tx = connection.transaction()?;

    {
        let mut statement = tx.prepare(
            "
            INSERT INTO docs(doc_id, filename, sha256, part, year, title)
            VALUES(?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(doc_id) DO UPDATE SET
              filename=excluded.filename,
              sha256=excluded.sha256,
              part=excluded.part,
              year=excluded.year,
              title=excluded.title
            ",
        )?;

        for pdf in &inventory.pdfs {
            let doc_id = doc_id_for(pdf);
            let title = format!("ISO 26262-{}:{}", pdf.part, pdf.year);

            statement.execute(params![
                doc_id,
                &pdf.filename,
                &pdf.sha256,
                pdf.part,
                pdf.year,
                title
            ])?;
        }
    }

    tx.commit()?;
    Ok(inventory.pdfs.len())
}
