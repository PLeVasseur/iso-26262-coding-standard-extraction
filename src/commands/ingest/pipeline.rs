struct IngestRegexes {
    list_item_regex: Regex,
    note_item_regex: Regex,
    table_cell_split_regex: Regex,
    requirement_split_regex: Regex,
    requirement_keyword_regex: Regex,
}

impl IngestRegexes {
    fn build() -> Result<Self> {
        Ok(Self {
            list_item_regex: Regex::new(
                r"^(?P<marker>(?:(?:\d+[A-Za-z]?|[A-Za-z])(?:[\.)])?|[-*•—–]))(?:\s+(?P<body>.+))?$",
            )
            .context("failed to compile list item regex")?,
            note_item_regex: Regex::new(r"^(?i)(?P<marker>NOTE(?:\s+\d+)?)(?:\s+(?P<body>.+))?$")
                .context("failed to compile note item regex")?,
            table_cell_split_regex: Regex::new(r"\t+|\s{2,}")
                .context("failed to compile table cell split regex")?,
            requirement_split_regex: Regex::new(r"[.;]\s+")
                .context("failed to compile requirement split regex")?,
            requirement_keyword_regex: Regex::new(r"(?i)\bshall(?:\s+not)?\b|\bshould\b")
                .context("failed to compile requirement keyword regex")?,
        })
    }
}

#[derive(Debug)]
struct PdfNodeState {
    document_node_id: String,
    node_paths: HashMap<String, String>,
    section_ref_to_node_id: HashMap<String, String>,
    clause_ref_to_node_id: HashMap<String, String>,
    last_clause_node_id: Option<String>,
    node_order_index: i64,
}

impl PdfNodeState {
    fn new(document_node_id: String, document_path: String) -> Self {
        let mut node_paths = HashMap::<String, String>::new();
        node_paths.insert(document_node_id.clone(), document_path);

        Self {
            document_node_id,
            node_paths,
            section_ref_to_node_id: HashMap::new(),
            clause_ref_to_node_id: HashMap::new(),
            last_clause_node_id: None,
            node_order_index: 1,
        }
    }
}

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
    let regexes = IngestRegexes::build()?;

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

            track_processed_part(&mut stats, pdf.part);

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

            let Some(page_extraction) = extract_pages_for_pdf(
                &pdf_path,
                &doc_id,
                max_pages_per_doc,
                ocr_mode,
                ocr_lang,
                ocr_min_text_chars,
                &mut stats,
            )?
            else {
                continue;
            };

            accumulate_page_extraction_stats(&mut stats, &page_extraction);
            let page_printed_labels = page_extraction.page_printed_labels.clone();
            let pages = page_extraction.pages;

            let section_headings = extract_section_headings_for_pdf(&pdf_path, &mut stats);

            let mut state = initialize_document_node_state(
                &mut node_statement,
                &doc_id,
                pdf.part,
                &pdf.sha256,
                pages.len(),
                &mut stats,
            )?;

            insert_section_heading_nodes(
                &mut node_statement,
                &doc_id,
                &pdf.sha256,
                &section_headings,
                &mut state,
                &mut stats,
            )?;

            let structured_chunks = split_long_structured_chunks(parser.parse_pages(&pages));
            insert_structured_chunks_for_pdf(
                &mut chunk_statement,
                &mut node_statement,
                &doc_id,
                &pdf.sha256,
                &structured_chunks,
                &page_printed_labels,
                &regexes,
                &mut state,
                &mut stats,
            )?;

            if seed_page_chunks {
                seed_page_chunks_for_pdf(
                    &mut chunk_statement,
                    &mut node_statement,
                    &doc_id,
                    &pdf.sha256,
                    &pages,
                    &page_printed_labels,
                    &mut state,
                    &mut stats,
                )?;
            }
        }
    }

    tx.commit()?;
    Ok(stats)
}

fn track_processed_part(stats: &mut ChunkInsertStats, part: u32) {
    stats.processed_pdf_count += 1;
    if !stats.processed_parts.contains(&part) {
        stats.processed_parts.push(part);
        stats.processed_parts.sort_unstable();
    }
}

fn accumulate_page_extraction_stats(stats: &mut ChunkInsertStats, extraction: &ExtractedPages) {
    stats.ocr_page_count += extraction.ocr_page_count;
    stats.text_layer_page_count += extraction.text_layer_page_count;
    stats.ocr_fallback_page_count += extraction.ocr_fallback_page_count;
    stats.empty_page_count += extraction.empty_page_count;
    stats.header_lines_removed += extraction.header_lines_removed;
    stats.footer_lines_removed += extraction.footer_lines_removed;
    stats.dehyphenation_merges += extraction.dehyphenation_merges;
    stats
        .page_provenance
        .extend(extraction.page_provenance.clone());
    stats.warnings.extend(extraction.warnings.clone());
}

#[allow(clippy::too_many_arguments)]
fn extract_pages_for_pdf(
    pdf_path: &Path,
    doc_id: &str,
    max_pages_per_doc: Option<usize>,
    ocr_mode: OcrMode,
    ocr_lang: &str,
    ocr_min_text_chars: usize,
    stats: &mut ChunkInsertStats,
) -> Result<Option<ExtractedPages>> {
    match extract_pages_with_backend(
        pdf_path,
        doc_id,
        max_pages_per_doc,
        ocr_mode,
        ocr_lang,
        ocr_min_text_chars,
    ) {
        Ok(extraction) => Ok(Some(extraction)),
        Err(err) => {
            if matches!(ocr_mode, OcrMode::Force) {
                return Err(err)
                    .with_context(|| format!("failed to extract text for {}", pdf_path.display()));
            }

            let warning = format!("failed to extract text for {}: {err}", pdf_path.display());
            warn!(warning = %warning, "pdf extraction warning");
            stats.warnings.push(warning);
            Ok(None)
        }
    }
}

fn extract_section_headings_for_pdf(
    pdf_path: &Path,
    stats: &mut ChunkInsertStats,
) -> Vec<SectionHeadingDraft> {
    match extract_section_headings_with_pdftohtml(pdf_path) {
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
    }
}

fn initialize_document_node_state(
    node_statement: &mut rusqlite::Statement<'_>,
    doc_id: &str,
    part: u32,
    source_hash: &str,
    page_count: usize,
    stats: &mut ChunkInsertStats,
) -> Result<PdfNodeState> {
    let document_node_id = format!("{}:node:document", doc_id);
    let document_path = format!("document:{}", doc_id);

    insert_node(
        node_statement,
        &document_node_id,
        None,
        doc_id,
        NodeType::Document,
        None,
        None,
        Some(&format!("ISO 26262 Part {}", part)),
        0,
        Some(1),
        Some(page_count as i64),
        None,
        source_hash,
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
    Ok(PdfNodeState::new(document_node_id, document_path))
}
