fn parse_paragraphs(
    text: &str,
    heading: &str,
    list_item_regex: &Regex,
    note_item_regex: &Regex,
) -> Vec<String> {
    let body_lines = extract_body_lines_preserve_blanks(text, heading);
    let mut paragraphs = Vec::<String>::new();
    let mut current = String::new();

    for raw_line in body_lines {
        if line_is_noise(raw_line) {
            continue;
        }

        let line = raw_line.trim();
        if line.is_empty() {
            if !current.is_empty() {
                paragraphs.push(current.trim().to_string());
                current.clear();
            }
            continue;
        }

        if current.is_empty() {
            current.push_str(line);
            continue;
        }

        let starts_new_marker = list_item_regex.is_match(line) || note_item_regex.is_match(line);
        let previous_ends_sentence = current.ends_with('.') || current.ends_with(';');
        let starts_with_lowercase = line
            .chars()
            .next()
            .map(|value| value.is_lowercase())
            .unwrap_or(false);

        if starts_new_marker || (previous_ends_sentence && !starts_with_lowercase) {
            paragraphs.push(current.trim().to_string());
            current.clear();
            current.push_str(line);
            continue;
        }

        current.push(' ');
        current.push_str(line);
    }

    if !current.is_empty() {
        paragraphs.push(current.trim().to_string());
    }

    paragraphs
}

#[allow(clippy::too_many_arguments)]
fn insert_paragraph_nodes(
    node_statement: &mut rusqlite::Statement<'_>,
    doc_id: &str,
    parent_node_id: &str,
    parent_path: &str,
    reference: &str,
    paragraphs: &[String],
    page_start: i64,
    page_end: i64,
    source_hash: &str,
    node_order_index: &mut i64,
    stats: &mut ChunkInsertStats,
) -> Result<()> {
    for (index, paragraph) in paragraphs.iter().enumerate() {
        let paragraph_node_id = format!("{}:paragraph:{:03}", parent_node_id, index + 1);
        let paragraph_ref = format!("{} para {}", reference, index + 1);
        let paragraph_heading = format!("{} paragraph {}", reference, index + 1);
        let paragraph_path = format!("{} > paragraph:{}", parent_path, index + 1);
        let paragraph_order = (index + 1) as i64;
        let paragraph_label = paragraph_order.to_string();
        let paragraph_anchor_id = build_citation_anchor_id(
            doc_id,
            reference,
            "paragraph",
            Some(&paragraph_label),
            Some(paragraph_order),
        );

        insert_node(
            node_statement,
            &paragraph_node_id,
            Some(parent_node_id),
            doc_id,
            NodeType::Paragraph,
            Some(&paragraph_ref),
            Some(&paragraph_ref),
            Some(&paragraph_heading),
            *node_order_index,
            Some(page_start),
            Some(page_end),
            Some(paragraph),
            source_hash,
            &paragraph_path,
            Some("paragraph"),
            None,
            Some(&paragraph_label),
            Some(paragraph_order),
            Some(&paragraph_anchor_id),
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

        *node_order_index += 1;
        stats.nodes_total += 1;
        increment_node_type_stat(stats, NodeType::Paragraph);
    }

    Ok(())
}

fn parse_list_items(
    text: &str,
    heading: &str,
    list_item_regex: &Regex,
    note_item_regex: &Regex,
) -> (Vec<ListItemDraft>, bool, bool) {
    let body_lines = extract_body_lines_preserve_blanks(text, heading);
    let mut items = Vec::<ListItemDraft>::new();
    let mut list_marker_candidates = 0usize;
    let mut active_item: Option<ListItemDraft> = None;

    for raw_line in body_lines {
        if line_is_noise(raw_line) {
            continue;
        }

        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        let list_capture = list_item_regex.captures(line);
        let note_capture = note_item_regex.captures(line);

        if list_capture.is_some() {
            list_marker_candidates += 1;
        }

        if let Some(captures) = list_capture {
            if let Some(item) = active_item.take() {
                if !item.text.trim().is_empty() {
                    items.push(item);
                }
            }

            let marker = captures
                .name("marker")
                .map(|value| value.as_str().to_string())
                .unwrap_or_else(|| "-".to_string());
            let marker_norm = normalize_marker_label(&marker);
            let marker_style = classify_list_marker_style(&marker_norm).to_string();
            let body = captures
                .name("body")
                .map(|value| value.as_str().trim().to_string())
                .unwrap_or_default();
            let mut depth = infer_list_depth(raw_line);
            if depth == 1 {
                if let Some(previous) = items.last() {
                    depth = infer_depth_from_marker_transition(previous, &marker_style, depth);
                }
            }

            active_item = Some(ListItemDraft {
                marker,
                marker_norm,
                marker_style,
                text: body,
                depth,
            });
            continue;
        }

        if note_capture.is_some() {
            if let Some(item) = active_item.take() {
                if !item.text.trim().is_empty() {
                    items.push(item);
                }
            }
            continue;
        }

        if let Some(item) = active_item.as_mut() {
            if !item.text.is_empty() {
                item.text.push(' ');
            }
            item.text.push_str(line);
        }
    }

    if let Some(item) = active_item.take() {
        if !item.text.trim().is_empty() {
            items.push(item);
        }
    }

    reorder_list_items_for_marker_sequence(&mut items);

    let had_list_candidates = list_marker_candidates > 0;
    let used_fallback = had_list_candidates && items.is_empty();
    (items, used_fallback, had_list_candidates)
}

fn parse_note_items(
    text: &str,
    heading: &str,
    note_item_regex: &Regex,
    list_item_regex: &Regex,
) -> Vec<NoteItemDraft> {
    let body_lines = extract_body_lines(text, heading);
    let mut items = Vec::<NoteItemDraft>::new();
    let mut active_item: Option<NoteItemDraft> = None;

    for raw_line in body_lines {
        if line_is_noise(raw_line) {
            continue;
        }

        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        if let Some(captures) = note_item_regex.captures(line) {
            if let Some(item) = active_item.take() {
                if !item.text.trim().is_empty() {
                    items.push(item);
                }
            }

            let marker = captures
                .name("marker")
                .map(|value| value.as_str().to_string())
                .unwrap_or_else(|| "NOTE".to_string());
            let marker_norm = normalize_marker_label(&marker);
            let body = captures
                .name("body")
                .map(|value| value.as_str().trim().to_string())
                .unwrap_or_default();

            active_item = Some(NoteItemDraft {
                marker,
                marker_norm,
                text: body,
            });
            continue;
        }

        if list_item_regex.is_match(line) {
            if let Some(item) = active_item.take() {
                if !item.text.trim().is_empty() {
                    items.push(item);
                }
            }
            continue;
        }

        if let Some(item) = active_item.as_mut() {
            if !item.text.is_empty() {
                item.text.push(' ');
            }
            item.text.push_str(line);
        }
    }

    if let Some(item) = active_item.take() {
        if !item.text.trim().is_empty() {
            items.push(item);
        }
    }

    items
}

fn reorder_list_items_for_marker_sequence(items: &mut Vec<ListItemDraft>) {
    if items.len() < 3 {
        return;
    }

    if items.iter().all(|item| {
        item.marker_norm.len() == 1 && item.marker_norm.chars().all(|ch| ch.is_ascii_lowercase())
    }) {
        items.sort_by(|left, right| left.marker_norm.cmp(&right.marker_norm));
        return;
    }

    if items
        .iter()
        .all(|item| parse_numeric_alpha_marker(&item.marker_norm).is_some())
    {
        items.sort_by(|left, right| {
            let (left_num, left_suffix) =
                parse_numeric_alpha_marker(&left.marker_norm).unwrap_or((i64::MAX, None));
            let (right_num, right_suffix) =
                parse_numeric_alpha_marker(&right.marker_norm).unwrap_or((i64::MAX, None));

            left_num
                .cmp(&right_num)
                .then(left_suffix.unwrap_or('~').cmp(&right_suffix.unwrap_or('~')))
        });
    }
}

fn infer_list_depth(raw_line: &str) -> i64 {
    let indent_units = raw_line
        .chars()
        .take_while(|ch| ch.is_whitespace())
        .map(|ch| if ch == '\t' { 4usize } else { 1usize })
        .sum::<usize>();

    let normalized = (indent_units / 2).min(5);
    (normalized as i64) + 1
}

fn classify_list_marker_style(marker_norm: &str) -> &'static str {
    if marker_norm == "-" {
        return "bullet";
    }

    if marker_norm.chars().all(|ch| ch.is_ascii_digit()) {
        return "numeric";
    }

    if marker_norm.len() == 1 && marker_norm.chars().all(|ch| ch.is_ascii_lowercase()) {
        return "alpha";
    }

    if is_roman_marker(marker_norm) {
        return "roman";
    }

    if parse_numeric_alpha_marker(marker_norm).is_some() {
        return "alnum";
    }

    "symbol"
}

fn infer_depth_from_marker_transition(
    previous: &ListItemDraft,
    marker_style: &str,
    fallback_depth: i64,
) -> i64 {
    match (previous.marker_style.as_str(), marker_style) {
        ("numeric", "alpha") | ("numeric", "roman") | ("alpha", "bullet") => {
            (previous.depth + 1).min(6)
        }
        _ => fallback_depth,
    }
}

fn is_roman_marker(value: &str) -> bool {
    value.len() >= 2
        && value
            .chars()
            .all(|ch| matches!(ch, 'i' | 'v' | 'x' | 'l' | 'c' | 'd' | 'm'))
}

fn parse_numeric_alpha_marker(value: &str) -> Option<(i64, Option<char>)> {
    let mut digits = String::new();
    let mut suffix: Option<char> = None;

    for ch in value.chars() {
        if ch.is_ascii_digit() {
            if suffix.is_some() {
                return None;
            }
            digits.push(ch);
            continue;
        }

        if ch.is_ascii_lowercase() {
            if suffix.is_some() {
                return None;
            }
            suffix = Some(ch);
            continue;
        }

        return None;
    }

    if digits.is_empty() {
        return None;
    }

    let number = digits.parse::<i64>().ok()?;
    Some((number, suffix))
}
