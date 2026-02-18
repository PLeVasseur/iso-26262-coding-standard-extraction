use super::*;

#[derive(Debug, Default)]
pub struct ChunkInsertStats {
    pub processed_pdf_count: usize,
    pub processed_parts: Vec<u32>,
    pub ocr_page_count: usize,
    pub text_layer_page_count: usize,
    pub ocr_fallback_page_count: usize,
    pub empty_page_count: usize,
    pub header_lines_removed: usize,
    pub footer_lines_removed: usize,
    pub dehyphenation_merges: usize,
    pub structured_chunks_inserted: usize,
    pub clause_chunks_inserted: usize,
    pub table_chunks_inserted: usize,
    pub annex_chunks_inserted: usize,
    pub page_chunks_inserted: usize,
    pub nodes_total: i64,
    pub clause_nodes_inserted: usize,
    pub subclause_nodes_inserted: usize,
    pub annex_nodes_inserted: usize,
    pub table_nodes_inserted: usize,
    pub table_row_nodes_inserted: usize,
    pub table_cell_nodes_inserted: usize,
    pub list_nodes_inserted: usize,
    pub list_item_nodes_inserted: usize,
    pub note_nodes_inserted: usize,
    pub note_item_nodes_inserted: usize,
    pub paragraph_nodes_inserted: usize,
    pub requirement_atom_nodes_inserted: usize,
    pub table_raw_fallback_count: usize,
    pub list_parse_candidate_count: usize,
    pub list_parse_fallback_count: usize,
    pub table_sparse_rows_count: usize,
    pub table_overloaded_rows_count: usize,
    pub table_rows_with_markers_count: usize,
    pub table_rows_with_descriptions_count: usize,
    pub table_marker_expected_count: usize,
    pub table_marker_observed_count: usize,
    pub page_provenance: Vec<PageExtractionProvenance>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Default)]
pub struct ExtractedPages {
    pub pages: Vec<String>,
    pub page_printed_labels: Vec<Option<String>>,
    pub ocr_page_count: usize,
    pub text_layer_page_count: usize,
    pub ocr_fallback_page_count: usize,
    pub empty_page_count: usize,
    pub header_lines_removed: usize,
    pub footer_lines_removed: usize,
    pub dehyphenation_merges: usize,
    pub page_provenance: Vec<PageExtractionProvenance>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PageExtractionProvenance {
    pub doc_id: String,
    pub page_pdf: i64,
    pub backend: String,
    pub reason: String,
    pub text_char_count: usize,
    pub ocr_char_count: Option<usize>,
    pub printed_page_label: Option<String>,
    pub printed_page_status: String,
}

#[derive(Debug, Serialize)]
pub struct PageProvenanceManifest {
    pub manifest_version: u32,
    pub run_id: String,
    pub generated_at: String,
    pub entries: Vec<PageExtractionProvenance>,
}

#[derive(Debug, Clone)]
pub struct StructuredChunkDraft {
    pub chunk_type: ChunkType,
    pub reference: String,
    pub ref_path: String,
    pub heading: String,
    pub text: String,
    pub page_start: i64,
    pub page_end: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkType {
    Clause,
    Table,
    Annex,
}

impl ChunkType {
    pub fn as_str(self) -> &'static str {
        match self {
            ChunkType::Clause => "clause",
            ChunkType::Table => "table",
            ChunkType::Annex => "annex",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeType {
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
    pub fn as_str(self) -> &'static str {
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
pub struct ParsedTableRows {
    pub rows: Vec<Vec<String>>,
    pub markdown: Option<String>,
    pub csv: Option<String>,
    pub used_fallback: bool,
    pub quality: TableQualityCounters,
}

#[derive(Debug, Default)]
pub struct TableQualityCounters {
    pub sparse_rows_count: usize,
    pub overloaded_rows_count: usize,
    pub rows_with_markers_count: usize,
    pub rows_with_descriptions_count: usize,
    pub marker_expected_count: usize,
    pub marker_observed_count: usize,
}

#[derive(Debug)]
pub struct ListItemDraft {
    pub marker: String,
    pub marker_norm: String,
    pub marker_style: String,
    pub text: String,
    pub depth: i64,
}

#[derive(Debug)]
pub struct NoteItemDraft {
    pub marker: String,
    pub marker_norm: String,
    pub text: String,
}

#[derive(Debug, Clone)]
pub struct SectionHeadingDraft {
    pub reference: String,
    pub heading: String,
    pub page_pdf: i64,
}

#[derive(Debug)]
pub struct StructuredChunkParser {
    pub clause_heading: Regex,
    pub table_heading: Regex,
    pub annex_heading: Regex,
    pub toc_line: Regex,
}

impl StructuredChunkParser {
    pub fn new() -> Result<Self> {
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

    pub fn parse_pages(&self, pages: &[String]) -> Vec<StructuredChunkDraft> {
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

    pub fn detect_heading(&self, line: &str) -> Option<(ChunkType, String, String)> {
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

pub fn split_long_structured_chunks(
    chunks: Vec<StructuredChunkDraft>,
) -> Vec<StructuredChunkDraft> {
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

pub fn body_without_heading(text: &str, heading: &str) -> String {
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

pub fn split_words_with_overlap(text: &str, max_words: usize, overlap_words: usize) -> Vec<String> {
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
