fn parse_table_rows(text: &str, heading: &str, cell_split_regex: &Regex) -> ParsedTableRows {
    let body_lines = extract_body_lines(text, heading);
    let mut rows = Vec::<Vec<String>>::new();

    for line in &body_lines {
        if line_is_noise(line) {
            continue;
        }

        let cells = split_table_cells(line, cell_split_regex);
        if !cells.is_empty() {
            rows.push(cells);
        }
    }

    normalize_table_rows_for_alignment(&mut rows);
    backfill_asil_marker_row_ratings(&mut rows, &body_lines);

    let mut structured = rows.len() >= 2 && rows.iter().any(|cells| cells.len() > 1);
    let reconstructed = reconstruct_table_rows_from_markers(&body_lines);
    if !reconstructed.is_empty() {
        let original_quality = analyze_table_rows(&rows);
        let reconstructed_quality = analyze_table_rows(&reconstructed);

        if prefer_reconstructed_rows(
            rows.len(),
            &original_quality,
            reconstructed.len(),
            &reconstructed_quality,
        ) {
            rows = reconstructed;
            normalize_table_rows_for_alignment(&mut rows);
            backfill_asil_marker_row_ratings(&mut rows, &body_lines);
            structured = rows.len() >= 2 && rows.iter().any(|cells| cells.len() > 1);
        } else if !structured
            && reconstructed.len() >= 2
            && reconstructed.iter().any(|cells| cells.len() > 1)
        {
            rows = reconstructed;
            normalize_table_rows_for_alignment(&mut rows);
            backfill_asil_marker_row_ratings(&mut rows, &body_lines);
            structured = true;
        }
    }

    let markdown = if rows.is_empty() {
        None
    } else {
        Some(table_to_markdown(&rows))
    };
    let csv = if rows.is_empty() {
        None
    } else {
        Some(table_to_csv(&rows))
    };
    let quality = analyze_table_rows(&rows);

    ParsedTableRows {
        rows,
        markdown,
        csv,
        used_fallback: !structured,
        quality,
    }
}

fn normalize_table_rows_for_alignment(rows: &mut Vec<Vec<String>>) {
    merge_single_cell_continuations(rows);
    split_marker_rows_with_trailing_ratings(rows);
    redistribute_dense_marker_ratings(rows);
}

fn merge_single_cell_continuations(rows: &mut Vec<Vec<String>>) {
    let mut merged = Vec::<Vec<String>>::new();

    for row in rows.drain(..) {
        if row.len() == 1 {
            let content = row[0].trim();
            if !content.is_empty()
                && parse_table_marker_token(content).is_none()
                && let Some(previous) = merged.last_mut()
                && previous
                    .first()
                    .map(|value| parse_table_marker_token(value).is_some())
                    .unwrap_or(false)
            {
                if previous.len() < 2 {
                    previous.push(content.to_string());
                } else if let Some(description) = previous.get_mut(1) {
                    if !description.is_empty() {
                        description.push(' ');
                    }
                    description.push_str(content);
                }
                continue;
            }
        }

        merged.push(row);
    }

    *rows = merged;
}

fn split_marker_rows_with_trailing_ratings(rows: &mut [Vec<String>]) {
    for row in rows {
        if row.len() != 2 {
            continue;
        }

        let is_marker_row = row
            .first()
            .map(|value| parse_table_marker_token(value).is_some())
            .unwrap_or(false);
        if !is_marker_row {
            continue;
        }

        let description = row.get(1).cloned().unwrap_or_default();
        let tokens = description
            .split_whitespace()
            .map(str::trim)
            .filter(|token| !token.is_empty())
            .collect::<Vec<&str>>();
        if tokens.len() < 3 {
            continue;
        }

        let mut trailing_ratings = Vec::<String>::new();
        for token in tokens.iter().rev() {
            let normalized = token.trim_matches(['(', ')', '.', ':', ';', ',']);
            if is_table_rating_token(normalized) {
                trailing_ratings.push(normalized.to_string());
            } else {
                break;
            }
        }

        if trailing_ratings.len() < 2 {
            continue;
        }

        trailing_ratings.reverse();
        let split_at = tokens.len().saturating_sub(trailing_ratings.len());
        let merged_description = tokens[..split_at].join(" ");
        if merged_description.is_empty() {
            continue;
        }

        row[1] = merged_description;
        row.extend(trailing_ratings);
    }
}

fn redistribute_dense_marker_ratings(rows: &mut [Vec<String>]) {
    for row_index in 0..rows.len() {
        let Some(first_cell) = rows[row_index].first() else {
            continue;
        };
        if parse_table_marker_token(first_cell).is_none() {
            continue;
        }

        let rating_pool = rows[row_index]
            .iter()
            .skip(2)
            .map(|cell| cell.trim_matches(['(', ')', '.', ':', ';', ',']))
            .filter(|cell| is_table_rating_token(cell))
            .map(|cell| cell.to_string())
            .collect::<Vec<String>>();
        if rating_pool.len() < 8 {
            continue;
        }

        let mut marker_block = vec![row_index];
        let mut cursor = row_index;
        while cursor > 0 {
            let previous = cursor - 1;
            let is_marker = rows[previous]
                .first()
                .map(|value| parse_table_marker_token(value).is_some())
                .unwrap_or(false);
            if !is_marker {
                break;
            }

            marker_block.push(previous);
            cursor = previous;
        }
        marker_block.reverse();
        if marker_block.len() < 2 {
            continue;
        }

        rows[row_index].truncate(2);
        let mut assignment_index = 0usize;
        for rating in rating_pool {
            while assignment_index < marker_block.len()
                && rows[marker_block[assignment_index]].len() >= 6
            {
                assignment_index += 1;
            }

            if assignment_index >= marker_block.len() {
                rows[row_index].push(rating);
                continue;
            }

            rows[marker_block[assignment_index]].push(rating);
        }
    }
}

fn is_table_rating_token(token: &str) -> bool {
    matches!(token, "+" | "++" | "-" | "--" | "+/-" | "+/−" | "−/+" | "o")
}

fn is_footnote_marker_line(line: &str) -> bool {
    let trimmed = line.trim_matches(['(', ')', '.', ':', ';', ',']).trim();
    trimmed.len() == 1
        && trimmed
            .chars()
            .next()
            .map(|ch| ch.is_ascii_lowercase())
            .unwrap_or(false)
}

fn backfill_asil_marker_row_ratings(rows: &mut [Vec<String>], body_lines: &[&str]) {
    if !looks_like_asil_matrix(body_lines) {
        return;
    }

    let mut rating_pool = body_lines
        .iter()
        .flat_map(|line| line.split_whitespace())
        .map(|token| token.trim_matches(['(', ')', '.', ':', ';', ',']))
        .filter(|token| is_table_rating_token(token))
        .map(|token| token.to_string())
        .collect::<Vec<String>>();

    if rating_pool.is_empty() {
        return;
    }

    for row in rows {
        let is_marker_row = row
            .first()
            .map(|value| parse_table_marker_token(value).is_some())
            .unwrap_or(false);
        if !is_marker_row {
            continue;
        }

        let has_ratings = row.iter().skip(2).any(|cell| {
            cell.split_whitespace()
                .map(|token| token.trim_matches(['(', ')', '.', ':', ';', ',']))
                .any(is_table_rating_token)
        });
        if has_ratings {
            continue;
        }

        let Some(rating) = rating_pool.pop() else {
            break;
        };
        row.push(rating);
    }
}

fn looks_like_asil_matrix(body_lines: &[&str]) -> bool {
    let has_asil_header = body_lines
        .iter()
        .any(|line| line.to_ascii_uppercase().contains("ASIL"));
    if !has_asil_header {
        return false;
    }

    let mut columns = HashSet::<char>::new();
    for line in body_lines {
        let trimmed = line.trim();
        if trimmed.len() != 1 {
            continue;
        }

        if let Some(ch) = trimmed.chars().next().map(|value| value.to_ascii_uppercase())
            && matches!(ch, 'A' | 'B' | 'C' | 'D')
        {
            columns.insert(ch);
        }
    }

    columns.len() == 4
}

fn analyze_table_rows(rows: &[Vec<String>]) -> TableQualityCounters {
    let mut counters = TableQualityCounters::default();
    let mut observed_markers = HashSet::<(i64, Option<char>)>::new();

    for row in rows {
        let first_cell = row.first().map(|value| value.as_str()).unwrap_or_default();
        let row_marker = parse_table_marker_token(first_cell);
        let row_marker_count = count_row_marker_tokens(row);

        if let Some(marker) = row_marker {
            counters.rows_with_markers_count += 1;
            observed_markers.insert(marker);

            if has_row_description(row) {
                counters.rows_with_descriptions_count += 1;
            } else {
                counters.sparse_rows_count += 1;
            }
        }

        if row_marker_count > 1 {
            counters.overloaded_rows_count += 1;
        }
    }

    counters.marker_observed_count = observed_markers.len();
    counters.marker_expected_count = estimate_expected_marker_count(&observed_markers);
    counters
}

fn parse_table_marker_token(value: &str) -> Option<(i64, Option<char>)> {
    let marker = normalize_marker_label(value);
    parse_numeric_alpha_marker(&marker)
}
