fn extract_pages_with_backend(
    pdf_path: &Path,
    doc_id: &str,
    max_pages_per_doc: Option<usize>,
    ocr_mode: OcrMode,
    ocr_lang: &str,
    ocr_min_text_chars: usize,
) -> Result<ExtractedPages> {
    let pages = extract_pages_with_pdftotext(pdf_path, max_pages_per_doc)?;
    let mut extraction = ExtractedPages {
        page_printed_labels: vec![None; pages.len()],
        text_layer_page_count: pages.len(),
        empty_page_count: pages
            .iter()
            .filter(|page| non_whitespace_char_count(page) == 0)
            .count(),
        page_provenance: pages
            .iter()
            .enumerate()
            .map(|(index, page)| {
                let chars = non_whitespace_char_count(page);
                PageExtractionProvenance {
                    doc_id: doc_id.to_string(),
                    page_pdf: (index + 1) as i64,
                    backend: "text_layer".to_string(),
                    reason: if chars == 0 {
                        "text_layer_empty".to_string()
                    } else {
                        "text_layer_default".to_string()
                    },
                    text_char_count: chars,
                    ocr_char_count: None,
                    printed_page_label: None,
                    printed_page_status: "unknown".to_string(),
                }
            })
            .collect(),
        pages,
        ..ExtractedPages::default()
    };

    let candidate_pages = collect_ocr_candidates(&extraction.pages, ocr_mode, ocr_min_text_chars);
    if candidate_pages.is_empty() {
        refresh_printed_page_labels(&mut extraction);
        apply_page_normalization(&mut extraction);
        return Ok(extraction);
    }

    if !command_available("pdftoppm") || !command_available("tesseract") {
        let message = format!(
            "OCR mode '{}' requested for {} pages but pdftoppm/tesseract are unavailable",
            ocr_mode.as_str(),
            candidate_pages.len()
        );
        if matches!(ocr_mode, OcrMode::Force) {
            bail!(message);
        }

        for page_number in &candidate_pages {
            let page_index = page_number.saturating_sub(1);
            if let Some(entry) = extraction.page_provenance.get_mut(page_index) {
                entry.reason = "ocr_unavailable_text_layer_fallback".to_string();
            }
        }
        extraction.warnings.push(message);
        refresh_printed_page_labels(&mut extraction);
        apply_page_normalization(&mut extraction);
        return Ok(extraction);
    }

    for page_number in candidate_pages {
        let page_index = page_number.saturating_sub(1);
        let current_text = extraction
            .pages
            .get(page_index)
            .cloned()
            .unwrap_or_default();

        match extract_page_with_ocr(pdf_path, page_number, ocr_lang) {
            Ok(ocr_text) => {
                let ocr_char_count = non_whitespace_char_count(&ocr_text);
                if ocr_char_count == 0 && matches!(ocr_mode, OcrMode::Auto) {
                    extraction.warnings.push(format!(
                        "OCR text was empty for {} page {} in auto mode",
                        pdf_path.display(),
                        page_number
                    ));
                    if let Some(entry) = extraction.page_provenance.get_mut(page_index) {
                        entry.reason = "ocr_empty_text_layer_fallback".to_string();
                        entry.ocr_char_count = Some(0);
                    }
                    continue;
                }

                if let Some(page) = extraction.pages.get_mut(page_index) {
                    *page = ocr_text;
                }
                extraction.ocr_page_count += 1;
                extraction.text_layer_page_count =
                    extraction.text_layer_page_count.saturating_sub(1);
                if matches!(ocr_mode, OcrMode::Auto) {
                    extraction.ocr_fallback_page_count += 1;
                }

                let previous_chars = non_whitespace_char_count(&current_text);
                if previous_chars == 0 && ocr_char_count > 0 {
                    extraction.empty_page_count = extraction.empty_page_count.saturating_sub(1);
                } else if previous_chars > 0 && ocr_char_count == 0 {
                    extraction.empty_page_count += 1;
                }

                if let Some(entry) = extraction.page_provenance.get_mut(page_index) {
                    entry.backend = "ocr".to_string();
                    entry.reason = if matches!(ocr_mode, OcrMode::Force) {
                        "ocr_force_mode".to_string()
                    } else {
                        "ocr_auto_low_text".to_string()
                    };
                    entry.text_char_count = ocr_char_count;
                    entry.ocr_char_count = Some(ocr_char_count);
                }
            }
            Err(error) => {
                if matches!(ocr_mode, OcrMode::Force) {
                    return Err(error).with_context(|| {
                        format!(
                            "failed OCR extraction for {} page {}",
                            pdf_path.display(),
                            page_number
                        )
                    });
                }

                extraction.warnings.push(format!(
                    "OCR fallback failed for {} page {}: {}",
                    pdf_path.display(),
                    page_number,
                    error
                ));
                if let Some(page) = extraction.pages.get_mut(page_index) {
                    *page = current_text;
                }
                if let Some(entry) = extraction.page_provenance.get_mut(page_index) {
                    entry.reason = "ocr_failed_text_layer_fallback".to_string();
                }
            }
        }
    }

    refresh_printed_page_labels(&mut extraction);
    apply_page_normalization(&mut extraction);
    Ok(extraction)
}

fn apply_page_normalization(extraction: &mut ExtractedPages) {
    let header_candidates = detect_repeated_edge_lines(&extraction.pages, true);
    let footer_candidates = detect_repeated_edge_lines(&extraction.pages, false);

    let mut header_removed = 0usize;
    let mut footer_removed = 0usize;
    let mut dehyphen_merges = 0usize;

    for page in &mut extraction.pages {
        let mut lines = page
            .lines()
            .map(|line| line.to_string())
            .collect::<Vec<String>>();

        lines.retain(|line| !line_is_noise(line));

        if let Some(index) = first_nonempty_line_index(&lines) {
            let candidate = normalize_edge_line(&lines[index]);
            if !candidate.is_empty() && header_candidates.contains(&candidate) {
                lines.remove(index);
                header_removed += 1;
            }
        }

        if let Some(index) = last_nonempty_line_index(&lines) {
            let candidate = normalize_edge_line(&lines[index]);
            if !candidate.is_empty() && footer_candidates.contains(&candidate) {
                lines.remove(index);
                footer_removed += 1;
            }
        }

        let (normalized_lines, merges) = merge_hyphenated_lines(lines);
        dehyphen_merges += merges;
        *page = normalized_lines.join("\n");
    }

    extraction.header_lines_removed += header_removed;
    extraction.footer_lines_removed += footer_removed;
    extraction.dehyphenation_merges += dehyphen_merges;
    extraction.empty_page_count = extraction
        .pages
        .iter()
        .filter(|page| non_whitespace_char_count(page) == 0)
        .count();
}

fn refresh_printed_page_labels(extraction: &mut ExtractedPages) {
    extraction.page_printed_labels = extraction
        .pages
        .iter()
        .map(|page| detect_printed_page_label(page))
        .collect();

    for (index, label) in extraction.page_printed_labels.iter().enumerate() {
        if let Some(entry) = extraction.page_provenance.get_mut(index) {
            entry.printed_page_label = label.clone();
            if label.is_some() {
                entry.printed_page_status = "detected".to_string();
            } else {
                entry.printed_page_status = "missing".to_string();
            }
        }
    }
}

fn detect_printed_page_label(page_text: &str) -> Option<String> {
    let lines = page_text.lines().collect::<Vec<&str>>();

    let page_regex = Regex::new(r"(?i)^page\s+([0-9ivxlcdm]+)$").ok()?;
    let number_regex = Regex::new(r"^([0-9]{1,4})$").ok()?;
    let roman_regex = Regex::new(r"(?i)^([ivxlcdm]{1,8})$").ok()?;

    for line in lines.iter().rev().take(5).chain(lines.iter().take(2)) {
        let normalized = line
            .chars()
            .filter(|character| {
                character.is_ascii_alphanumeric() || character.is_ascii_whitespace()
            })
            .collect::<String>();
        let normalized = normalized.trim();
        if normalized.is_empty() {
            continue;
        }

        if let Some(captures) = page_regex.captures(normalized) {
            if let Some(value) = captures.get(1) {
                return Some(value.as_str().to_ascii_lowercase());
            }
        }

        if let Some(captures) = number_regex.captures(normalized) {
            if let Some(value) = captures.get(1) {
                return Some(value.as_str().to_string());
            }
        }

        if let Some(captures) = roman_regex.captures(normalized) {
            if let Some(value) = captures.get(1) {
                return Some(value.as_str().to_ascii_lowercase());
            }
        }
    }

    None
}

fn printed_page_label_for(labels: &[Option<String>], page_pdf: i64) -> Option<String> {
    if page_pdf <= 0 {
        return None;
    }

    labels
        .get((page_pdf - 1) as usize)
        .and_then(|label| label.clone())
}

fn printed_page_labels_for_range(
    labels: &[Option<String>],
    page_start: i64,
    page_end: i64,
) -> (Option<String>, Option<String>) {
    if labels.is_empty() || (page_start <= 0 && page_end <= 0) {
        return (None, None);
    }

    let mut start = if page_start > 0 { page_start } else { page_end };
    let mut end = if page_end > 0 { page_end } else { page_start };
    if start <= 0 || end <= 0 {
        return (None, None);
    }
    if start > end {
        std::mem::swap(&mut start, &mut end);
    }

    let first_index = (start - 1) as usize;
    if first_index >= labels.len() {
        return (None, None);
    }
    let last_index = ((end - 1) as usize).min(labels.len().saturating_sub(1));

    let mut first_detected = None;
    let mut last_detected = None;
    for label in labels[first_index..=last_index].iter().flatten() {
        let normalized = label.trim();
        if normalized.is_empty() {
            continue;
        }

        if first_detected.is_none() {
            first_detected = Some(normalized.to_string());
        }
        last_detected = Some(normalized.to_string());
    }

    (first_detected, last_detected)
}

fn detect_repeated_edge_lines(pages: &[String], header: bool) -> HashSet<String> {
    let mut counts = HashMap::<String, usize>::new();
    for page in pages {
        let lines = page.lines().map(str::trim).collect::<Vec<&str>>();
        let candidate = if header {
            lines.iter().copied().find(|line| !line.is_empty())
        } else {
            lines.iter().rev().copied().find(|line| !line.is_empty())
        };

        let Some(candidate) = candidate else {
            continue;
        };

        let normalized = normalize_edge_line(candidate);
        if normalized.is_empty() || normalized.len() > 120 {
            continue;
        }
        *counts.entry(normalized).or_insert(0) += 1;
    }

    counts
        .into_iter()
        .filter_map(|(candidate, count)| if count >= 3 { Some(candidate) } else { None })
        .collect()
}

fn normalize_edge_line(input: &str) -> String {
    input
        .split_whitespace()
        .collect::<Vec<&str>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn first_nonempty_line_index(lines: &[String]) -> Option<usize> {
    lines.iter().position(|line| !line.trim().is_empty())
}

fn last_nonempty_line_index(lines: &[String]) -> Option<usize> {
    lines.iter().rposition(|line| !line.trim().is_empty())
}

fn merge_hyphenated_lines(lines: Vec<String>) -> (Vec<String>, usize) {
    let mut merged = Vec::<String>::new();
    let mut merges = 0usize;
    let mut index = 0usize;

    while index < lines.len() {
        let current = lines[index].clone();
        if index + 1 < lines.len() {
            let next = lines[index + 1].clone();
            if should_merge_hyphenated_pair(&current, &next) {
                let joined = format!(
                    "{}{}",
                    current.trim_end().trim_end_matches('-'),
                    next.trim_start()
                );
                merged.push(joined);
                merges += 1;
                index += 2;
                continue;
            }
        }

        merged.push(current);
        index += 1;
    }

    (merged, merges)
}

fn should_merge_hyphenated_pair(current: &str, next: &str) -> bool {
    let left = current.trim_end();
    if !left.ends_with('-') {
        return false;
    }

    let right = next.trim_start();
    let starts_with_lowercase = right
        .chars()
        .next()
        .map(|character| character.is_ascii_lowercase())
        .unwrap_or(false);
    if !starts_with_lowercase {
        return false;
    }

    left.trim_end_matches('-')
        .chars()
        .last()
        .map(|character| character.is_ascii_alphabetic())
        .unwrap_or(false)
}

fn collect_ocr_candidates(
    pages: &[String],
    ocr_mode: OcrMode,
    min_text_chars: usize,
) -> Vec<usize> {
    match ocr_mode {
        OcrMode::Off => Vec::new(),
        OcrMode::Force => (1..=pages.len()).collect(),
        OcrMode::Auto => pages
            .iter()
            .enumerate()
            .filter_map(|(index, page)| {
                if non_whitespace_char_count(page) < min_text_chars {
                    Some(index + 1)
                } else {
                    None
                }
            })
            .collect(),
    }
}

