#[derive(Debug, Default)]
struct ChunkInsertStats {
    processed_pdf_count: usize,
    processed_parts: Vec<u32>,
    ocr_page_count: usize,
    text_layer_page_count: usize,
    ocr_fallback_page_count: usize,
    empty_page_count: usize,
    header_lines_removed: usize,
    footer_lines_removed: usize,
    dehyphenation_merges: usize,
    structured_chunks_inserted: usize,
    clause_chunks_inserted: usize,
    table_chunks_inserted: usize,
    annex_chunks_inserted: usize,
    page_chunks_inserted: usize,
    nodes_total: i64,
    clause_nodes_inserted: usize,
    subclause_nodes_inserted: usize,
    annex_nodes_inserted: usize,
    table_nodes_inserted: usize,
    table_row_nodes_inserted: usize,
    table_cell_nodes_inserted: usize,
    list_nodes_inserted: usize,
    list_item_nodes_inserted: usize,
    note_nodes_inserted: usize,
    note_item_nodes_inserted: usize,
    paragraph_nodes_inserted: usize,
    requirement_atom_nodes_inserted: usize,
    table_raw_fallback_count: usize,
    list_parse_candidate_count: usize,
    list_parse_fallback_count: usize,
    table_sparse_rows_count: usize,
    table_overloaded_rows_count: usize,
    table_rows_with_markers_count: usize,
    table_rows_with_descriptions_count: usize,
    table_marker_expected_count: usize,
    table_marker_observed_count: usize,
    page_provenance: Vec<PageExtractionProvenance>,
    warnings: Vec<String>,
}

#[derive(Debug, Default)]
struct ExtractedPages {
    pages: Vec<String>,
    page_printed_labels: Vec<Option<String>>,
    ocr_page_count: usize,
    text_layer_page_count: usize,
    ocr_fallback_page_count: usize,
    empty_page_count: usize,
    header_lines_removed: usize,
    footer_lines_removed: usize,
    dehyphenation_merges: usize,
    page_provenance: Vec<PageExtractionProvenance>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct PageExtractionProvenance {
    doc_id: String,
    page_pdf: i64,
    backend: String,
    reason: String,
    text_char_count: usize,
    ocr_char_count: Option<usize>,
    printed_page_label: Option<String>,
    printed_page_status: String,
}

#[derive(Debug, Serialize)]
struct PageProvenanceManifest {
    manifest_version: u32,
    run_id: String,
    generated_at: String,
    entries: Vec<PageExtractionProvenance>,
}

#[derive(Debug, Clone)]
struct StructuredChunkDraft {
    chunk_type: ChunkType,
    reference: String,
    ref_path: String,
    heading: String,
    text: String,
    page_start: i64,
    page_end: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChunkType {
    Clause,
    Table,
    Annex,
}

impl ChunkType {
    fn as_str(self) -> &'static str {
        match self {
            ChunkType::Clause => "clause",
            ChunkType::Table => "table",
            ChunkType::Annex => "annex",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeType {
    Document,
    SectionHeading,
    Clause,
    Subclause,
    Annex,
    Paragraph,
    Table,
    TableRow,
    TableCell,
    List,
    ListItem,
    Note,
    NoteItem,
    RequirementAtom,
    Page,
}

impl NodeType {
    fn as_str(self) -> &'static str {
        match self {
            NodeType::Document => "document",
            NodeType::SectionHeading => "section_heading",
            NodeType::Clause => "clause",
            NodeType::Subclause => "subclause",
            NodeType::Annex => "annex",
            NodeType::Paragraph => "paragraph",
            NodeType::Table => "table",
            NodeType::TableRow => "table_row",
            NodeType::TableCell => "table_cell",
            NodeType::List => "list",
            NodeType::ListItem => "list_item",
            NodeType::Note => "note",
            NodeType::NoteItem => "note_item",
            NodeType::RequirementAtom => "requirement_atom",
            NodeType::Page => "page",
        }
    }
}

#[derive(Debug)]
struct ParsedTableRows {
    rows: Vec<Vec<String>>,
    markdown: Option<String>,
    csv: Option<String>,
    used_fallback: bool,
    quality: TableQualityCounters,
}

#[derive(Debug, Default)]
struct TableQualityCounters {
    sparse_rows_count: usize,
    overloaded_rows_count: usize,
    rows_with_markers_count: usize,
    rows_with_descriptions_count: usize,
    marker_expected_count: usize,
    marker_observed_count: usize,
}

#[derive(Debug)]
struct ListItemDraft {
    marker: String,
    marker_norm: String,
    marker_style: String,
    text: String,
    depth: i64,
}

#[derive(Debug)]
struct NoteItemDraft {
    marker: String,
    marker_norm: String,
    text: String,
}

#[derive(Debug, Clone)]
struct SectionHeadingDraft {
    reference: String,
    heading: String,
    page_pdf: i64,
}

#[derive(Debug)]
struct StructuredChunkParser {
    clause_heading: Regex,
    table_heading: Regex,
    annex_heading: Regex,
    toc_line: Regex,
}

impl StructuredChunkParser {
    fn new() -> Result<Self> {
        Ok(Self {
            clause_heading: Regex::new(r"^\s*(\d+(?:\.\d+)+)\s+(.+)$")
                .context("failed to compile clause heading regex")?,
            table_heading: Regex::new(r"^\s*(Table\s+\d+)\s*[-:–—]?\s*(.*)$")
                .context("failed to compile table heading regex")?,
            annex_heading: Regex::new(r"^\s*(Annex\s+[A-Z])(?:\s*\([^)]*\))?\s*[-:–—]?\s*(.*)$")
                .context("failed to compile annex heading regex")?,
            toc_line: Regex::new(r"\.{3,}\s*\d+\s*$")
                .context("failed to compile table-of-contents line regex")?,
        })
    }

    fn parse_pages(&self, pages: &[String]) -> Vec<StructuredChunkDraft> {
        #[derive(Debug)]
        struct ActiveChunk {
            chunk_type: ChunkType,
            reference: String,
            heading: String,
            page_start: i64,
            page_end: i64,
            body_lines: Vec<String>,
        }

        fn finalize(active: ActiveChunk) -> StructuredChunkDraft {
            let body = active.body_lines.join("\n").trim().to_string();
            let text = if body.is_empty() {
                active.heading.clone()
            } else {
                format!("{}\n\n{}", active.heading, body)
            };
            let ref_path = derive_ref_path(&active.reference, active.chunk_type);

            StructuredChunkDraft {
                chunk_type: active.chunk_type,
                reference: active.reference,
                ref_path,
                heading: active.heading,
                text,
                page_start: active.page_start,
                page_end: active.page_end,
            }
        }

        let mut chunks = Vec::new();
        let mut current: Option<ActiveChunk> = None;

        for (page_index, page_text) in pages.iter().enumerate() {
            let page_number = (page_index + 1) as i64;
            for raw_line in page_text.lines() {
                let line = normalize_line(raw_line);
                if line.is_empty() {
                    continue;
                }

                if let Some((chunk_type, reference, heading)) = self.detect_heading(line) {
                    if let Some(active) = current.take() {
                        chunks.push(finalize(active));
                    }

                    current = Some(ActiveChunk {
                        chunk_type,
                        reference,
                        heading,
                        page_start: page_number,
                        page_end: page_number,
                        body_lines: Vec::new(),
                    });
                    continue;
                }

                if let Some(active) = current.as_mut() {
                    active.page_end = page_number;
                    active.body_lines.push(line.to_string());
                }
            }
        }

        if let Some(active) = current.take() {
            chunks.push(finalize(active));
        }

        chunks
    }

    fn detect_heading(&self, line: &str) -> Option<(ChunkType, String, String)> {
        if self.toc_line.is_match(line) {
            return None;
        }

        if let Some(captures) = self.table_heading.captures(line) {
            let reference = captures.get(1).map(|m| m.as_str().trim().to_string())?;
            return Some((ChunkType::Table, reference, line.to_string()));
        }

        if let Some(captures) = self.annex_heading.captures(line) {
            let reference = captures.get(1).map(|m| m.as_str().trim().to_string())?;
            return Some((ChunkType::Annex, reference, line.to_string()));
        }

        if let Some(captures) = self.clause_heading.captures(line) {
            let reference = captures.get(1).map(|m| m.as_str().trim().to_string())?;
            let title = captures.get(2).map(|m| m.as_str().trim()).unwrap_or("");
            if title.is_empty() || title.len() > 140 {
                return None;
            }

            return Some((ChunkType::Clause, reference, line.to_string()));
        }

        None
    }
}

fn split_long_structured_chunks(chunks: Vec<StructuredChunkDraft>) -> Vec<StructuredChunkDraft> {
    let mut expanded = Vec::<StructuredChunkDraft>::new();

    for chunk in chunks {
        if !matches!(chunk.chunk_type, ChunkType::Clause | ChunkType::Annex) {
            expanded.push(chunk);
            continue;
        }

        let body = body_without_heading(&chunk.text, &chunk.heading);
        let body_word_count = body.split_whitespace().count();
        if body_word_count <= 900 {
            expanded.push(chunk);
            continue;
        }

        let heading_words = chunk.heading.split_whitespace().count();
        let max_segment_words = 900usize.saturating_sub(heading_words).max(300);
        let overlap_words = 75.min(max_segment_words.saturating_sub(1));

        for segment in split_words_with_overlap(&body, max_segment_words, overlap_words) {
            let text = format!("{}\n\n{}", chunk.heading, segment);
            expanded.push(StructuredChunkDraft {
                chunk_type: chunk.chunk_type,
                reference: chunk.reference.clone(),
                ref_path: chunk.ref_path.clone(),
                heading: chunk.heading.clone(),
                text,
                page_start: chunk.page_start,
                page_end: chunk.page_end,
            });
        }
    }

    expanded
}

fn body_without_heading(text: &str, heading: &str) -> String {
    let mut lines = text.lines().collect::<Vec<&str>>();
    if lines
        .first()
        .map(|line| line.trim() == heading.trim())
        .unwrap_or(false)
    {
        lines.remove(0);
    }

    lines.join(" ")
}

fn split_words_with_overlap(text: &str, max_words: usize, overlap_words: usize) -> Vec<String> {
    let words = text.split_whitespace().collect::<Vec<&str>>();
    if words.is_empty() {
        return vec![String::new()];
    }
    if words.len() <= max_words {
        return vec![words.join(" ")];
    }

    let mut segments = Vec::<String>::new();
    let mut start = 0usize;

    while start < words.len() {
        let end = (start + max_words).min(words.len());
        segments.push(words[start..end].join(" "));

        if end == words.len() {
            break;
        }

        let mut next_start = end.saturating_sub(overlap_words);
        if next_start <= start {
            next_start = end;
        }
        start = next_start;
    }

    segments
}
