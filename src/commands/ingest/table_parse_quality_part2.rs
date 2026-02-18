fn infer_table_header_rows(rows: &[Vec<String>]) -> usize {
    let Some(first_row) = rows.first() else {
        return 0;
    };

    let first_cell = first_row
        .first()
        .map(|value| value.as_str())
        .unwrap_or_default();
    let first_row_has_marker = parse_table_marker_token(first_cell).is_some();
    if first_row_has_marker {
        return 0;
    }

    let non_empty_cells = first_row
        .iter()
        .map(|cell| cell.trim())
        .filter(|cell| !cell.is_empty())
        .count();

    if non_empty_cells >= 2 {
        1
    } else {
        0
    }
}

fn count_row_marker_tokens(row: &[String]) -> usize {
    let mut marker_tokens = HashSet::<(i64, Option<char>)>::new();

    for cell in row {
        for token in cell.split_whitespace() {
            let trimmed = token.trim_matches(['(', ')', '.', ':', ';', ',']);
            if let Some(marker) = parse_table_marker_token(trimmed) {
                marker_tokens.insert(marker);
            }
        }
    }

    marker_tokens.len()
}

fn has_row_description(row: &[String]) -> bool {
    if row.len() < 2 {
        return false;
    }

    let description = row[1].trim();
    !description.is_empty() && description.chars().any(|value| value.is_ascii_alphabetic())
}

fn estimate_expected_marker_count(observed_markers: &HashSet<(i64, Option<char>)>) -> usize {
    let mut grouped = HashMap::<i64, Vec<Option<char>>>::new();

    for (number, suffix) in observed_markers {
        grouped.entry(*number).or_default().push(*suffix);
    }

    let mut expected = 0usize;
    for suffixes in grouped.values() {
        let with_suffix = suffixes
            .iter()
            .filter_map(|suffix| *suffix)
            .collect::<Vec<char>>();

        if with_suffix.is_empty() {
            expected += suffixes.len().max(1);
            continue;
        }

        let min_index = with_suffix
            .iter()
            .map(|suffix| (*suffix as u8).saturating_sub(b'a') as usize)
            .min()
            .unwrap_or(0);
        let max_index = with_suffix
            .iter()
            .map(|suffix| (*suffix as u8).saturating_sub(b'a') as usize)
            .max()
            .unwrap_or(min_index);

        expected += (max_index.saturating_sub(min_index) + 1).max(with_suffix.len());
    }

    expected
}

fn reconstruct_table_rows_from_markers(lines: &[&str]) -> Vec<Vec<String>> {
    let marker_with_body_regex = Regex::new(r"^(?P<marker>\d+[A-Za-z]?)[\.)]?\s+(?P<body>.+)$")
        .expect("valid marker with body regex");
    let marker_only_regex =
        Regex::new(r"^(?P<marker>\d+[A-Za-z]?)[\.)]?$").expect("valid marker only regex");
    let marker_list_regex = Regex::new(r"^(?P<list>(?:\d+[A-Za-z]?\s+){1,}\d+[A-Za-z]?)$")
        .expect("valid marker list regex");
    let plus_regex = Regex::new(r"^\+{1,2}$").expect("valid plus regex");

    let mut rows = Vec::<Vec<String>>::new();
    let mut current_row: Option<Vec<String>> = None;
    let mut pending_markers = Vec::<String>::new();

    let flush_current = |rows: &mut Vec<Vec<String>>, current_row: &mut Option<Vec<String>>| {
        if let Some(row) = current_row.take() {
            if row.len() > 1 {
                rows.push(row);
            }
        }
    };

    for raw_line in lines {
        let line = raw_line.trim();
        if line.is_empty() || line_is_noise(line) {
            continue;
        }

        if let Some(captures) = marker_list_regex.captures(line) {
            flush_current(&mut rows, &mut current_row);

            let marker_list = captures
                .name("list")
                .map(|value| value.as_str())
                .unwrap_or("");
            pending_markers = marker_list
                .split_whitespace()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect();
            continue;
        }

        if let Some(captures) = marker_with_body_regex.captures(line) {
            flush_current(&mut rows, &mut current_row);

            let marker = captures
                .name("marker")
                .map(|value| value.as_str().trim().to_string())
                .unwrap_or_default();
            let body = captures
                .name("body")
                .map(|value| value.as_str().trim().to_string())
                .unwrap_or_default();

            let mut row = vec![marker];
            row.push(body);
            current_row = Some(row);
            continue;
        }

        if let Some(captures) = marker_only_regex.captures(line) {
            flush_current(&mut rows, &mut current_row);
            let marker = captures
                .name("marker")
                .map(|value| value.as_str().trim().to_string())
                .unwrap_or_default();
            if !marker.is_empty() {
                pending_markers.push(marker);
            }
            continue;
        }

        if plus_regex.is_match(line) {
            if let Some(row) = current_row.as_mut() {
                row.push(line.to_string());
            }
            continue;
        }

        if !pending_markers.is_empty() && is_footnote_marker_line(line) {
            continue;
        }

        if !pending_markers.is_empty() {
            flush_current(&mut rows, &mut current_row);
            let marker = pending_markers.remove(0);
            current_row = Some(vec![marker, line.to_string()]);
            continue;
        }

        let Some(row) = current_row.as_mut() else {
            continue;
        };

        if row.len() <= 1 {
            row.push(line.to_string());
            continue;
        }

        let description = row.get_mut(1).expect("description slot exists");
        if !description.is_empty() {
            description.push(' ');
        }
        description.push_str(line);
    }

    if let Some(row) = current_row.take() {
        if row.len() > 1 {
            rows.push(row);
        }
    }

    for marker in pending_markers {
        rows.push(vec![marker, String::new()]);
    }

    rows
}

fn prefer_reconstructed_rows(
    original_rows_count: usize,
    original_quality: &TableQualityCounters,
    reconstructed_rows_count: usize,
    reconstructed_quality: &TableQualityCounters,
) -> bool {
    if reconstructed_rows_count < 2 {
        return false;
    }

    if reconstructed_quality.rows_with_markers_count == 0 {
        return false;
    }

    let original_sparse_ratio = if original_rows_count == 0 {
        1.0
    } else {
        original_quality.sparse_rows_count as f64 / original_rows_count as f64
    };
    let reconstructed_sparse_ratio =
        reconstructed_quality.sparse_rows_count as f64 / reconstructed_rows_count as f64;

    let original_description_coverage = ratio_usize(
        original_quality.rows_with_descriptions_count,
        original_quality.rows_with_markers_count,
    )
    .unwrap_or(0.0);
    let reconstructed_description_coverage = ratio_usize(
        reconstructed_quality.rows_with_descriptions_count,
        reconstructed_quality.rows_with_markers_count,
    )
    .unwrap_or(0.0);

    (reconstructed_sparse_ratio + 0.05) < original_sparse_ratio
        || (reconstructed_description_coverage > original_description_coverage + 0.10)
        || (reconstructed_quality.sparse_rows_count < original_quality.sparse_rows_count
            && reconstructed_quality.rows_with_descriptions_count
                >= original_quality.rows_with_descriptions_count)
}

fn ratio_usize(numerator: usize, denominator: usize) -> Option<f64> {
    if denominator == 0 {
        None
    } else {
        Some(numerator as f64 / denominator as f64)
    }
}

fn extract_body_lines<'a>(text: &'a str, heading: &str) -> Vec<&'a str> {
    let mut lines = text.lines().collect::<Vec<&str>>();
    if let Some(first) = lines.first() {
        if first.trim() == heading.trim() {
            lines.remove(0);
        }
    }
    lines
        .into_iter()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect()
}

fn extract_body_lines_preserve_blanks<'a>(text: &'a str, heading: &str) -> Vec<&'a str> {
    let mut lines = text.lines().collect::<Vec<&str>>();
    if let Some(first) = lines.first() {
        if first.trim() == heading.trim() {
            lines.remove(0);
        }
    }

    lines
}

fn line_is_noise(line: &str) -> bool {
    contains_iso_watermark_noise(line)
}

fn contains_iso_watermark_noise(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    let has_store_download = lower.contains("iso store order") && lower.contains("downloaded:");
    let has_single_user_notice = (lower.contains("single user licence only")
        || lower.contains("single user license only"))
        && lower.contains("networking prohibited");
    let has_license_banner = lower.contains("licensed to")
        && lower.contains("license #")
        && lower.contains("downloaded:");

    has_store_download || has_single_user_notice || has_license_banner
}

fn split_table_cells(line: &str, cell_split_regex: &Regex) -> Vec<String> {
    let mut cells = cell_split_regex
        .split(line)
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<String>>();

    if cells.len() <= 1 && line.contains('|') {
        cells = line
            .split('|')
            .map(str::trim)
            .filter(|segment| !segment.is_empty())
            .map(ToOwned::to_owned)
            .collect();
    }

    if cells.is_empty() {
        vec![line.trim().to_string()]
    } else {
        cells
    }
}

fn table_to_markdown(rows: &[Vec<String>]) -> String {
    let col_count = rows.iter().map(|row| row.len()).max().unwrap_or(1).max(1);
    let mut padded_rows = rows
        .iter()
        .map(|row| {
            let mut current = row.clone();
            while current.len() < col_count {
                current.push(String::new());
            }
            current
        })
        .collect::<Vec<Vec<String>>>();

    if padded_rows.is_empty() {
        padded_rows.push(vec![String::new(); col_count]);
    }

    let header = padded_rows.first().cloned().unwrap_or_default();
    let mut lines = Vec::<String>::new();
    lines.push(format!("| {} |", header.join(" | ")));
    lines.push(format!(
        "| {} |",
        (0..col_count)
            .map(|_| "---")
            .collect::<Vec<&str>>()
            .join(" | ")
    ));

    for row in padded_rows.iter().skip(1) {
        lines.push(format!("| {} |", row.join(" | ")));
    }

    lines.join("\n")
}

fn table_to_csv(rows: &[Vec<String>]) -> String {
    rows.iter()
        .map(|row| {
            row.iter()
                .map(|cell| escape_csv_cell(cell))
                .collect::<Vec<String>>()
                .join(",")
        })
        .collect::<Vec<String>>()
        .join("\n")
}

fn escape_csv_cell(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}
