fn insert_chunks(
    connection: &mut Connection,
    cache_root: &Path,
    pdfs: &[PdfEntry],
    parser: &StructuredChunkParser,
    max_pages_per_doc: Option<usize>,
    seed_page_chunks: bool,
    target_parts: &[u32],
    ocr_mode: OcrMode,
    ocr_lang: &str,
    ocr_min_text_chars: usize,
) -> Result<ChunkInsertStats> {
    let target_set: HashSet<u32> = target_parts.iter().copied().collect();
    let tx = connection.transaction()?;
    let mut stats = ChunkInsertStats::default();
    let list_item_regex = Regex::new(
        r"^(?P<marker>(?:(?:\d+[A-Za-z]?|[A-Za-z])(?:[\.)])?|[-*•—–]))(?:\s+(?P<body>.+))?$",
    )
    .context("failed to compile list item regex")?;
    let note_item_regex = Regex::new(r"^(?i)(?P<marker>NOTE(?:\s+\d+)?)(?:\s+(?P<body>.+))?$")
        .context("failed to compile note item regex")?;
    let table_cell_split_regex =
        Regex::new(r"\t+|\s{2,}").context("failed to compile table cell split regex")?;
    let requirement_split_regex =
        Regex::new(r"[.;]\s+").context("failed to compile requirement split regex")?;
    let requirement_keyword_regex = Regex::new(r"(?i)\bshall(?:\s+not)?\b|\bshould\b")
        .context("failed to compile requirement keyword regex")?;

    {
        let mut chunk_statement = tx.prepare(
            "
            INSERT INTO chunks(
              chunk_id, doc_id, type, ref, ref_path, heading, chunk_seq,
              page_pdf_start, page_pdf_end, page_printed_start, page_printed_end,
              text, table_md, table_csv, source_hash,
              origin_node_id, leaf_node_type, ancestor_path,
              anchor_type, anchor_label_raw, anchor_label_norm, anchor_order, citation_anchor_id
            )
            VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23)
            ON CONFLICT(chunk_id) DO UPDATE SET
              doc_id=excluded.doc_id,
              type=excluded.type,
              ref=excluded.ref,
              ref_path=excluded.ref_path,
              heading=excluded.heading,
              chunk_seq=excluded.chunk_seq,
              page_pdf_start=excluded.page_pdf_start,
              page_pdf_end=excluded.page_pdf_end,
              page_printed_start=excluded.page_printed_start,
              page_printed_end=excluded.page_printed_end,
              text=excluded.text,
              table_md=excluded.table_md,
              table_csv=excluded.table_csv,
              source_hash=excluded.source_hash,
              origin_node_id=excluded.origin_node_id,
              leaf_node_type=excluded.leaf_node_type,
              ancestor_path=excluded.ancestor_path,
              anchor_type=excluded.anchor_type,
              anchor_label_raw=excluded.anchor_label_raw,
              anchor_label_norm=excluded.anchor_label_norm,
              anchor_order=excluded.anchor_order,
              citation_anchor_id=excluded.citation_anchor_id
            ",
        )?;

        let mut node_statement = tx.prepare(
            "
            INSERT INTO nodes(
              node_id, parent_node_id, doc_id, node_type, ref, ref_path, heading,
              order_index, page_pdf_start, page_pdf_end, text, source_hash, ancestor_path,
              anchor_type, anchor_label_raw, anchor_label_norm, anchor_order, citation_anchor_id,
              list_depth, list_marker_style, item_index,
              table_node_id, row_idx, col_idx, is_header, row_span, col_span
            )
            VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27)
            ON CONFLICT(node_id) DO UPDATE SET
              parent_node_id=excluded.parent_node_id,
              doc_id=excluded.doc_id,
              node_type=excluded.node_type,
              ref=excluded.ref,
              ref_path=excluded.ref_path,
              heading=excluded.heading,
              order_index=excluded.order_index,
              page_pdf_start=excluded.page_pdf_start,
              page_pdf_end=excluded.page_pdf_end,
              text=excluded.text,
              source_hash=excluded.source_hash,
              ancestor_path=excluded.ancestor_path,
              anchor_type=excluded.anchor_type,
              anchor_label_raw=excluded.anchor_label_raw,
              anchor_label_norm=excluded.anchor_label_norm,
              anchor_order=excluded.anchor_order,
              citation_anchor_id=excluded.citation_anchor_id,
              list_depth=excluded.list_depth,
              list_marker_style=excluded.list_marker_style,
              item_index=excluded.item_index,
              table_node_id=excluded.table_node_id,
              row_idx=excluded.row_idx,
              col_idx=excluded.col_idx,
              is_header=excluded.is_header,
              row_span=excluded.row_span,
              col_span=excluded.col_span
            ",
        )?;

        for pdf in pdfs {
            if !target_set.is_empty() && !target_set.contains(&pdf.part) {
                continue;
            }

            stats.processed_pdf_count += 1;
            if !stats.processed_parts.contains(&pdf.part) {
                stats.processed_parts.push(pdf.part);
                stats.processed_parts.sort_unstable();
            }

            let doc_id = doc_id_for(pdf);
            tx.execute("DELETE FROM chunks WHERE doc_id = ?1", [&doc_id])?;
            tx.execute("DELETE FROM nodes WHERE doc_id = ?1", [&doc_id])?;

            let pdf_path = cache_root.join(&pdf.filename);
            if !pdf_path.exists() {
                stats
                    .warnings
                    .push(format!("missing source PDF: {}", pdf_path.display()));
                continue;
            }

            let page_extraction = match extract_pages_with_backend(
                &pdf_path,
                &doc_id,
                max_pages_per_doc,
                ocr_mode,
                ocr_lang,
                ocr_min_text_chars,
            ) {
                Ok(extraction) => extraction,
                Err(err) => {
                    if matches!(ocr_mode, OcrMode::Force) {
                        return Err(err).with_context(|| {
                            format!("failed to extract text for {}", pdf_path.display())
                        });
                    }

                    let warning =
                        format!("failed to extract text for {}: {err}", pdf_path.display());
                    warn!(warning = %warning, "pdf extraction warning");
                    stats.warnings.push(warning);
                    continue;
                }
            };
            let page_printed_labels = page_extraction.page_printed_labels.clone();
            let pages = page_extraction.pages;
            stats.ocr_page_count += page_extraction.ocr_page_count;
            stats.text_layer_page_count += page_extraction.text_layer_page_count;
            stats.ocr_fallback_page_count += page_extraction.ocr_fallback_page_count;
            stats.empty_page_count += page_extraction.empty_page_count;
            stats.header_lines_removed += page_extraction.header_lines_removed;
            stats.footer_lines_removed += page_extraction.footer_lines_removed;
            stats.dehyphenation_merges += page_extraction.dehyphenation_merges;
            stats
                .page_provenance
                .extend(page_extraction.page_provenance);
            stats.warnings.extend(page_extraction.warnings);

            let section_headings = match extract_section_headings_with_pdftohtml(&pdf_path) {
                Ok(headings) => headings,
                Err(err) => {
                    let warning = format!(
                        "failed to extract outline headings for {}: {err}",
                        pdf_path.display()
                    );
                    warn!(warning = %warning, "pdf outline extraction warning");
                    stats.warnings.push(warning);
                    Vec::new()
                }
            };

            let document_node_id = format!("{}:node:document", doc_id);
            let document_path = format!("document:{}", doc_id);
            let page_count = pages.len() as i64;
            insert_node(
                &mut node_statement,
                &document_node_id,
                None,
                &doc_id,
                NodeType::Document,
                None,
                None,
                Some(&format!("ISO 26262 Part {}", pdf.part)),
                0,
                Some(1),
                Some(page_count),
                None,
                &pdf.sha256,
                &document_path,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )?;

            stats.nodes_total += 1;

            let mut node_paths = HashMap::<String, String>::new();
            node_paths.insert(document_node_id.clone(), document_path);

            let mut node_key_counts = HashMap::<String, i64>::new();
            let mut chunk_key_counts = HashMap::<String, i64>::new();
            let mut section_ref_to_node_id = HashMap::<String, String>::new();
            let mut clause_ref_to_node_id = HashMap::<String, String>::new();
            let mut last_clause_node_id: Option<String> = None;
            let mut node_order_index: i64 = 1;

            for section in section_headings {
                let section_node_id = format!(
                    "{}:node:section_heading:{}",
                    doc_id,
                    sanitize_ref_for_id(&section.reference)
                );

                let section_path = build_ancestor_path(
                    Some(&document_node_id),
                    &node_paths,
                    NodeType::SectionHeading,
                    &section.reference,
                    &section.heading,
                );
                let section_anchor_order = section.reference.parse::<i64>().ok();
                let section_anchor_id = build_citation_anchor_id(
                    &doc_id,
                    &section.reference,
                    "clause",
                    Some(&section.reference),
                    section_anchor_order,
                );

                insert_node(
                    &mut node_statement,
                    &section_node_id,
                    Some(&document_node_id),
                    &doc_id,
                    NodeType::SectionHeading,
                    Some(&section.reference),
                    Some(&section.reference),
                    Some(&section.heading),
                    node_order_index,
                    Some(section.page_pdf),
                    Some(section.page_pdf),
                    Some(&section.heading),
                    &pdf.sha256,
                    &section_path,
                    Some("clause"),
                    Some(&section.reference),
                    Some(&section.reference),
                    section_anchor_order,
                    Some(&section_anchor_id),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )?;

                node_paths.insert(section_node_id.clone(), section_path);
                section_ref_to_node_id.insert(section.reference, section_node_id);
                node_order_index += 1;
                stats.nodes_total += 1;
                increment_node_type_stat(&mut stats, NodeType::SectionHeading);
            }

            let structured_chunks = split_long_structured_chunks(parser.parse_pages(&pages));
            let mut chunk_seq_by_ref = HashMap::<String, i64>::new();

            for chunk in structured_chunks {
                let origin_node_type = chunk_origin_node_type(chunk.chunk_type, &chunk.reference);
                let parent_node_id = match chunk.chunk_type {
                    ChunkType::Table => last_clause_node_id
                        .clone()
                        .unwrap_or_else(|| document_node_id.clone()),
                    ChunkType::Clause => {
                        find_parent_clause_node_id(&chunk.reference, &clause_ref_to_node_id)
                            .or_else(|| {
                                find_section_node_id(&chunk.reference, &section_ref_to_node_id)
                            })
                            .unwrap_or_else(|| document_node_id.clone())
                    }
                    ChunkType::Annex => document_node_id.clone(),
                };

                let ref_key = sanitize_ref_for_id(&chunk.reference);
                let node_count = node_key_counts
                    .entry(format!("{}:{}", origin_node_type.as_str(), ref_key))
                    .and_modify(|value| *value += 1)
                    .or_insert(1);

                let origin_node_id = format!(
                    "{}:node:{}:{}:{:03}",
                    doc_id,
                    origin_node_type.as_str(),
                    ref_key,
                    node_count
                );

                let ancestor_path = build_ancestor_path(
                    Some(&parent_node_id),
                    &node_paths,
                    origin_node_type,
                    &chunk.reference,
                    &chunk.heading,
                );
                let structured_seq = {
                    let next = chunk_seq_by_ref
                        .entry(chunk.reference.clone())
                        .and_modify(|value| *value += 1)
                        .or_insert(1);
                    *next
                };
                let node_anchor_type = match origin_node_type {
                    NodeType::Clause | NodeType::Subclause | NodeType::Annex | NodeType::Table => {
                        Some("clause")
                    }
                    _ => None,
                };
                let node_anchor_order = node_anchor_type.map(|_| structured_seq);
                let node_anchor_id = node_anchor_type.map(|anchor_type| {
                    build_citation_anchor_id(
                        &doc_id,
                        &chunk.reference,
                        anchor_type,
                        Some(&chunk.reference),
                        node_anchor_order,
                    )
                });

                insert_node(
                    &mut node_statement,
                    &origin_node_id,
                    Some(&parent_node_id),
                    &doc_id,
                    origin_node_type,
                    Some(&chunk.reference),
                    Some(&chunk.ref_path),
                    Some(&chunk.heading),
                    node_order_index,
                    Some(chunk.page_start),
                    Some(chunk.page_end),
                    Some(&chunk.text),
                    &pdf.sha256,
                    &ancestor_path,
                    node_anchor_type,
                    node_anchor_type.map(|_| chunk.reference.as_str()),
                    node_anchor_type.map(|_| chunk.reference.as_str()),
                    node_anchor_order,
                    node_anchor_id.as_deref(),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )?;

                node_paths.insert(origin_node_id.clone(), ancestor_path.clone());
                node_order_index += 1;
                stats.nodes_total += 1;
                increment_node_type_stat(&mut stats, origin_node_type);

                if matches!(origin_node_type, NodeType::Clause | NodeType::Subclause) {
                    clause_ref_to_node_id.insert(chunk.reference.clone(), origin_node_id.clone());
                    last_clause_node_id = Some(origin_node_id.clone());
                }

                let (table_md, table_csv, parsed_table_rows) =
                    if chunk.chunk_type == ChunkType::Table {
                        let parsed =
                            parse_table_rows(&chunk.text, &chunk.heading, &table_cell_split_regex);
                        (parsed.markdown.clone(), parsed.csv.clone(), Some(parsed))
                    } else {
                        (None::<String>, None::<String>, None::<ParsedTableRows>)
                    };

                if parsed_table_rows
                    .as_ref()
                    .is_some_and(|parsed| parsed.used_fallback)
                {
                    stats.table_raw_fallback_count += 1;
                }

                if let Some(parsed) = parsed_table_rows.as_ref() {
                    stats.table_sparse_rows_count += parsed.quality.sparse_rows_count;
                    stats.table_overloaded_rows_count += parsed.quality.overloaded_rows_count;
                    stats.table_rows_with_markers_count += parsed.quality.rows_with_markers_count;
                    stats.table_rows_with_descriptions_count +=
                        parsed.quality.rows_with_descriptions_count;
                    stats.table_marker_expected_count += parsed.quality.marker_expected_count;
                    stats.table_marker_observed_count += parsed.quality.marker_observed_count;
                }

                let chunk_count = chunk_key_counts
                    .entry(format!("{}:{}", chunk.chunk_type.as_str(), ref_key))
                    .and_modify(|value| *value += 1)
                    .or_insert(1);

                let chunk_id = format!(
                    "{}:{}:{}:{:03}",
                    doc_id,
                    chunk.chunk_type.as_str(),
                    ref_key,
                    chunk_count
                );
                let chunk_anchor_type = Some("clause");
                let chunk_anchor_order = Some(structured_seq);
                let chunk_anchor_id = Some(build_citation_anchor_id(
                    &doc_id,
                    &chunk.reference,
                    "clause",
                    Some(&chunk.reference),
                    chunk_anchor_order,
                ));
                let (chunk_page_printed_start, chunk_page_printed_end) =
                    printed_page_labels_for_range(
                        &page_printed_labels,
                        chunk.page_start,
                        chunk.page_end,
                    );

                chunk_statement.execute(params![
                    chunk_id,
                    &doc_id,
                    chunk.chunk_type.as_str(),
                    &chunk.reference,
                    &chunk.ref_path,
                    &chunk.heading,
                    structured_seq,
                    chunk.page_start,
                    chunk.page_end,
                    &chunk_page_printed_start,
                    &chunk_page_printed_end,
                    &chunk.text,
                    &table_md,
                    &table_csv,
                    &pdf.sha256,
                    &origin_node_id,
                    origin_node_type.as_str(),
                    &ancestor_path,
                    chunk_anchor_type,
                    chunk_anchor_type.map(|_| chunk.reference.as_str()),
                    chunk_anchor_type.map(|_| chunk.reference.as_str()),
                    chunk_anchor_order,
                    chunk_anchor_id.as_deref()
                ])?;

                stats.structured_chunks_inserted += 1;
                match chunk.chunk_type {
                    ChunkType::Clause => stats.clause_chunks_inserted += 1,
                    ChunkType::Table => stats.table_chunks_inserted += 1,
                    ChunkType::Annex => stats.annex_chunks_inserted += 1,
                }

                if let Some(parsed) = parsed_table_rows {
                    insert_table_child_nodes(
                        &mut node_statement,
                        &doc_id,
                        &origin_node_id,
                        &ancestor_path,
                        &chunk.reference,
                        &parsed,
                        chunk.page_start,
                        chunk.page_end,
                        &pdf.sha256,
                        &mut node_order_index,
                        &mut stats,
                    )?;
                }

                if matches!(
                    origin_node_type,
                    NodeType::Clause | NodeType::Subclause | NodeType::Annex
                ) {
                    let paragraphs = parse_paragraphs(
                        &chunk.text,
                        &chunk.heading,
                        &list_item_regex,
                        &note_item_regex,
                    );
                    if !paragraphs.is_empty() {
                        insert_paragraph_nodes(
                            &mut node_statement,
                            &doc_id,
                            &origin_node_id,
                            &ancestor_path,
                            &chunk.reference,
                            &paragraphs,
                            chunk.page_start,
                            chunk.page_end,
                            &pdf.sha256,
                            &mut node_order_index,
                            &mut stats,
                        )?;
                    }

                    let note_items = parse_note_items(
                        &chunk.text,
                        &chunk.heading,
                        &note_item_regex,
                        &list_item_regex,
                    );
                    if !note_items.is_empty() {
                        insert_note_nodes(
                            &mut node_statement,
                            &doc_id,
                            &origin_node_id,
                            &ancestor_path,
                            &chunk.reference,
                            &note_items,
                            chunk.page_start,
                            chunk.page_end,
                            &pdf.sha256,
                            &mut node_order_index,
                            &mut stats,
                        )?;
                    }

                    let (list_items, list_fallback, had_list_candidates) = parse_list_items(
                        &chunk.text,
                        &chunk.heading,
                        &list_item_regex,
                        &note_item_regex,
                    );
                    if had_list_candidates {
                        stats.list_parse_candidate_count += 1;
                    }
                    if !list_items.is_empty() {
                        insert_list_nodes(
                            &mut node_statement,
                            &doc_id,
                            &origin_node_id,
                            &ancestor_path,
                            &chunk.reference,
                            &list_items,
                            chunk.page_start,
                            chunk.page_end,
                            &pdf.sha256,
                            &mut node_order_index,
                            &mut stats,
                        )?;
                    } else if list_fallback {
                        stats.list_parse_fallback_count += 1;
                    }

                    let requirement_atoms = parse_requirement_atoms(
                        &chunk.text,
                        &chunk.heading,
                        &requirement_split_regex,
                        &requirement_keyword_regex,
                    );
                    if !requirement_atoms.is_empty() {
                        insert_requirement_atom_nodes(
                            &mut node_statement,
                            &doc_id,
                            &origin_node_id,
                            &ancestor_path,
                            &chunk.reference,
                            &requirement_atoms,
                            chunk.page_start,
                            chunk.page_end,
                            &pdf.sha256,
                            &mut node_order_index,
                            &mut stats,
                        )?;
                    }
                }
            }

            if seed_page_chunks {
                for (index, page_text) in pages.into_iter().enumerate() {
                    let text = page_text.trim();
                    if text.is_empty() {
                        continue;
                    }

                    let page_number = (index + 1) as i64;
                    let chunk_id = format!("{}:page:{:04}", doc_id, page_number);
                    let page_ref = format!("PDF page {}", page_number);
                    let heading = format!("Page {}", page_number);
                    let page_printed_label =
                        printed_page_label_for(&page_printed_labels, page_number);
                    let page_node_id = format!("{}:node:page:{:04}", doc_id, page_number);
                    let page_ancestor_path = build_ancestor_path(
                        Some(&document_node_id),
                        &node_paths,
                        NodeType::Page,
                        &page_ref,
                        &heading,
                    );

                    insert_node(
                        &mut node_statement,
                        &page_node_id,
                        Some(&document_node_id),
                        &doc_id,
                        NodeType::Page,
                        Some(&page_ref),
                        Some(&page_ref),
                        Some(&heading),
                        node_order_index,
                        Some(page_number),
                        Some(page_number),
                        Some(text),
                        &pdf.sha256,
                        &page_ancestor_path,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    )?;
                    node_order_index += 1;
                    stats.nodes_total += 1;

                    chunk_statement.execute(params![
                        chunk_id,
                        &doc_id,
                        "page",
                        &page_ref,
                        &page_ref,
                        &heading,
                        page_number,
                        page_number,
                        page_number,
                        &page_printed_label,
                        &page_printed_label,
                        text,
                        Option::<String>::None,
                        Option::<String>::None,
                        &pdf.sha256,
                        &page_node_id,
                        NodeType::Page.as_str(),
                        &page_ancestor_path,
                        Option::<String>::None,
                        Option::<String>::None,
                        Option::<String>::None,
                        Option::<i64>::None,
                        Option::<String>::None
                    ])?;
                    stats.page_chunks_inserted += 1;
                }
            }
        }
    }

    tx.commit()?;
    Ok(stats)
}

