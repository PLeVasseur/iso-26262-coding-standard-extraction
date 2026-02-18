use std::path::PathBuf;

use super::*;

#[test]
fn normalize_marker_label_handles_common_forms() {
    assert_eq!(normalize_marker_label("b)"), "b");
    assert_eq!(normalize_marker_label("NOTE 2"), "NOTE 2");
    assert_eq!(normalize_marker_label("—"), "-");
}

#[test]
fn reconstruct_table_rows_assigns_marker_list_to_following_lines() {
    let lines = vec![
        "1a 1b 1c",
        "First requirement description",
        "++",
        "Second requirement description",
        "+",
        "Third requirement description",
        "++",
    ];

    let rows = reconstruct_table_rows_from_markers(&lines);
    assert_eq!(rows.len(), 3);

    assert_eq!(rows[0][0], "1a");
    assert!(rows[0][1].contains("First requirement"));
    assert!(rows[0].iter().any(|cell| cell == "++"));

    assert_eq!(rows[1][0], "1b");
    assert!(rows[1][1].contains("Second requirement"));

    assert_eq!(rows[2][0], "1c");
    assert!(rows[2][1].contains("Third requirement"));
}

#[test]
fn reconstruct_table_rows_skips_footnote_markers_before_descriptions() {
    let lines = vec!["1b 1c", "a", "b", "Interface test", "Fault injection test"];

    let rows = reconstruct_table_rows_from_markers(&lines);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "1b");
    assert_eq!(rows[0][1], "Interface test");
    assert_eq!(rows[1][0], "1c");
    assert_eq!(rows[1][1], "Fault injection test");
}

#[test]
fn analyze_table_rows_tracks_sparse_and_overloaded_patterns() {
    let rows = vec![
        vec![
            "1a".to_string(),
            "Valid description".to_string(),
            "++".to_string(),
        ],
        vec!["1b".to_string(), String::new(), "+".to_string()],
        vec![
            "1c".to_string(),
            "Contains merged marker 1d text".to_string(),
            "++".to_string(),
        ],
    ];

    let counters = analyze_table_rows(&rows);
    assert_eq!(counters.rows_with_markers_count, 3);
    assert_eq!(counters.rows_with_descriptions_count, 2);
    assert_eq!(counters.sparse_rows_count, 1);
    assert_eq!(counters.overloaded_rows_count, 1);
    assert_eq!(counters.marker_observed_count, 3);
    assert_eq!(counters.marker_expected_count, 3);
}

#[test]
fn prefer_reconstructed_rows_when_quality_improves() {
    let original = TableQualityCounters {
        sparse_rows_count: 4,
        overloaded_rows_count: 1,
        rows_with_markers_count: 5,
        rows_with_descriptions_count: 1,
        marker_expected_count: 5,
        marker_observed_count: 5,
    };
    let reconstructed = TableQualityCounters {
        sparse_rows_count: 1,
        overloaded_rows_count: 1,
        rows_with_markers_count: 5,
        rows_with_descriptions_count: 4,
        marker_expected_count: 5,
        marker_observed_count: 5,
    };

    let preferred = prefer_reconstructed_rows(5, &original, 5, &reconstructed);
    assert!(preferred);
}

#[test]
fn merge_single_cell_continuations_appends_to_previous_marker_row() {
    let mut rows = vec![
        vec!["1a".to_string(), "Requirement text".to_string()],
        vec!["continued tail".to_string()],
        vec!["1b".to_string(), "Second requirement".to_string()],
    ];

    merge_single_cell_continuations(&mut rows);

    assert_eq!(rows.len(), 2);
    assert!(rows[0][1].contains("Requirement text"));
    assert!(rows[0][1].contains("continued tail"));
}

#[test]
fn split_marker_rows_with_trailing_ratings_extracts_rating_columns() {
    let mut rows = vec![vec!["1a".to_string(), "Description ++ + o -".to_string()]];

    split_marker_rows_with_trailing_ratings(&mut rows);

    assert_eq!(rows[0][1], "Description");
    assert_eq!(rows[0][2], "++");
    assert_eq!(rows[0][3], "+");
    assert_eq!(rows[0][4], "o");
    assert_eq!(rows[0][5], "-");
}

#[test]
fn redistribute_dense_marker_ratings_spreads_tokens_to_previous_rows() {
    let mut rows = vec![
        vec!["1a".to_string(), "First".to_string(), "++".to_string()],
        vec!["1b".to_string(), "Second".to_string()],
        vec!["1c".to_string(), "Third".to_string()],
        vec![
            "1d".to_string(),
            "Fourth".to_string(),
            "++".to_string(),
            "+".to_string(),
            "++".to_string(),
            "+".to_string(),
            "++".to_string(),
            "+".to_string(),
            "++".to_string(),
            "+".to_string(),
        ],
    ];

    redistribute_dense_marker_ratings(&mut rows);

    assert!(rows[1].len() > 2);
    assert!(rows[2].len() > 2);
    assert!(rows[3].len() <= 6);
}

#[test]
fn backfill_asil_marker_row_ratings_assigns_missing_marker_ratings() {
    let mut rows = vec![
        vec!["1a".to_string(), "First principle".to_string()],
        vec!["1b".to_string(), "Second principle".to_string()],
    ];
    let body_lines = vec!["ASIL", "A", "B", "C", "D", "++", "+", "++", "+"];

    backfill_asil_marker_row_ratings(&mut rows, &body_lines);

    assert_eq!(rows[0].len(), 3);
    assert_eq!(rows[1].len(), 3);
}

#[test]
fn infer_table_header_rows_detects_first_non_marker_row() {
    let rows = vec![
        vec!["Requirement".to_string(), "ASIL A".to_string()],
        vec!["1a".to_string(), "description".to_string()],
    ];

    assert_eq!(infer_table_header_rows(&rows), 1);
}

#[test]
fn parse_list_items_excludes_note_markers() {
    let list_item_regex = Regex::new(
        r"^(?P<marker>(?:(?:\d+[A-Za-z]?|[A-Za-z])(?:[\.)])?|[-*•—–]))(?:\s+(?P<body>.+))?$",
    )
    .expect("list regex compiles");
    let note_item_regex = Regex::new(r"^(?i)(?P<marker>NOTE(?:\s+\d+)?)(?:\s+(?P<body>.+))?$")
        .expect("note regex compiles");

    let text = "9.4.2 Heading\nNOTE 1 software safety requirements include implementation constraints\na) retain traceability\nb) verify assumptions";
    let (list_items, fallback, had_candidates) =
        parse_list_items(text, "9.4.2 Heading", &list_item_regex, &note_item_regex);

    assert!(!fallback);
    assert!(had_candidates);
    assert_eq!(list_items.len(), 2);
    assert_eq!(list_items[0].marker_norm, "a");
    assert_eq!(list_items[1].marker_norm, "b");
}

#[test]
fn parse_list_items_captures_depth_and_marker_style() {
    let list_item_regex = Regex::new(
        r"^(?P<marker>(?:(?:\d+[A-Za-z]?|[A-Za-z])(?:[\.)])?|[-*•—–]))(?:\s+(?P<body>.+))?$",
    )
    .expect("list regex compiles");
    let note_item_regex = Regex::new(r"^(?i)(?P<marker>NOTE(?:\s+\d+)?)(?:\s+(?P<body>.+))?$")
        .expect("note regex compiles");

    let text = "6.4 Heading\n1) top-level item\n  a) nested alpha item\n    - nested bullet item";
    let (list_items, fallback, had_candidates) =
        parse_list_items(text, "6.4 Heading", &list_item_regex, &note_item_regex);

    assert!(!fallback);
    assert!(had_candidates);
    assert_eq!(list_items.len(), 3);
    assert_eq!(list_items[0].marker_style, "numeric");
    assert_eq!(list_items[0].depth, 1);
    assert_eq!(list_items[1].marker_style, "alpha");
    assert_eq!(list_items[1].depth, 2);
    assert_eq!(list_items[2].marker_style, "bullet");
    assert_eq!(list_items[2].depth, 3);
}

#[test]
fn parse_list_items_note_only_text_is_not_counted_as_candidate() {
    let list_item_regex = Regex::new(
        r"^(?P<marker>(?:(?:\d+[A-Za-z]?|[A-Za-z])(?:[\.)])?|[-*•—–]))(?:\s+(?P<body>.+))?$",
    )
    .expect("list regex compiles");
    let note_item_regex = Regex::new(r"^(?i)(?P<marker>NOTE(?:\s+\d+)?)(?:\s+(?P<body>.+))?$")
        .expect("note regex compiles");

    let text = "6.4 Heading\nNOTE 1 This is informative guidance\nNOTE 2 Another informative note";
    let (list_items, fallback, had_candidates) =
        parse_list_items(text, "6.4 Heading", &list_item_regex, &note_item_regex);

    assert!(list_items.is_empty());
    assert!(!had_candidates);
    assert!(!fallback);
}

#[test]
fn parse_note_items_extracts_note_markers() {
    let list_item_regex = Regex::new(
        r"^(?P<marker>(?:(?:\d+[A-Za-z]?|[A-Za-z])(?:[\.)])?|[-*•—–]))(?:\s+(?P<body>.+))?$",
    )
    .expect("list regex compiles");
    let note_item_regex = Regex::new(r"^(?i)(?P<marker>NOTE(?:\s+\d+)?)(?:\s+(?P<body>.+))?$")
        .expect("note regex compiles");

    let text = "5.2 Heading\nNOTE 1 Development approaches can be suitable\nNOTE 2 Safety activities remain required\na) This line belongs to list";
    let note_items = parse_note_items(text, "5.2 Heading", &note_item_regex, &list_item_regex);

    assert_eq!(note_items.len(), 2);
    assert_eq!(note_items[0].marker_norm, "NOTE 1");
    assert_eq!(note_items[1].marker_norm, "NOTE 2");
    assert!(note_items[0].text.contains("Development approaches"));
}

#[test]
fn parse_paragraphs_splits_on_blank_lines() {
    let list_item_regex = Regex::new(
        r"^(?P<marker>(?:(?:\d+[A-Za-z]?|[A-Za-z])(?:[\.)])?|[-*•—–]))(?:\s+(?P<body>.+))?$",
    )
    .expect("list regex compiles");
    let note_item_regex = Regex::new(r"^(?i)(?P<marker>NOTE(?:\s+\d+)?)(?:\s+(?P<body>.+))?$")
        .expect("note regex compiles");

    let text = "9.3 Heading\nThe software unit should satisfy the first property.\n\nThis second paragraph starts after a blank line.";
    let paragraphs = parse_paragraphs(text, "9.3 Heading", &list_item_regex, &note_item_regex);

    assert_eq!(paragraphs.len(), 2);
    assert!(paragraphs[0].contains("first property"));
    assert!(paragraphs[1].contains("second paragraph"));
}

#[test]
fn parse_paragraphs_splits_before_marker_transitions() {
    let list_item_regex = Regex::new(
        r"^(?P<marker>(?:(?:\d+[A-Za-z]?|[A-Za-z])(?:[\.)])?|[-*•—–]))(?:\s+(?P<body>.+))?$",
    )
    .expect("list regex compiles");
    let note_item_regex = Regex::new(r"^(?i)(?P<marker>NOTE(?:\s+\d+)?)(?:\s+(?P<body>.+))?$")
        .expect("note regex compiles");

    let text = "8.4.5 Heading\nThe following principles apply to software units.\nNOTE 1 This is informative guidance.\na) first normative bullet";
    let paragraphs = parse_paragraphs(text, "8.4.5 Heading", &list_item_regex, &note_item_regex);

    assert_eq!(paragraphs.len(), 3);
    assert_eq!(paragraphs[1], "NOTE 1 This is informative guidance.");
    assert_eq!(paragraphs[2], "a) first normative bullet");
}

#[test]
fn collect_ocr_candidates_auto_uses_text_threshold() {
    let pages = vec![
        "minimal".to_string(),
        "this page has substantially more extractable text content".to_string(),
        "tiny".to_string(),
    ];

    let candidates = collect_ocr_candidates(&pages, OcrMode::Auto, 10);
    assert_eq!(candidates, vec![1, 3]);
}

#[test]
fn render_ingest_command_includes_ocr_flags_when_enabled() {
    let args = IngestArgs {
        cache_root: PathBuf::from(".cache/iso26262"),
        inventory_manifest_path: None,
        ingest_manifest_path: None,
        db_path: None,
        refresh_inventory: false,
        seed_page_chunks: false,
        target_parts: vec![6],
        max_pages_per_doc: Some(5),
        ocr_mode: OcrMode::Auto,
        ocr_lang: "eng".to_string(),
        ocr_min_text_chars: 200,
    };

    let command = render_ingest_command(&args);
    assert!(command.contains("--ocr-mode auto"));
    assert!(command.contains("--ocr-lang eng"));
    assert!(command.contains("--ocr-min-text-chars 200"));
}

#[test]
fn apply_page_normalization_strips_repeated_headers_and_footers() {
    let mut extraction = ExtractedPages {
        pages: vec![
            "ISO 26262 Part 6\nRequirement intro\nLicensed copy".to_string(),
            "ISO 26262 Part 6\nAnother requirement\nLicensed copy".to_string(),
            "ISO 26262 Part 6\nFinal requirement\nLicensed copy".to_string(),
        ],
        ..ExtractedPages::default()
    };

    apply_page_normalization(&mut extraction);

    assert_eq!(extraction.header_lines_removed, 3);
    assert_eq!(extraction.footer_lines_removed, 3);
    assert!(extraction
        .pages
        .iter()
        .all(|page| !page.contains("ISO 26262 Part 6")));
    assert!(extraction
        .pages
        .iter()
        .all(|page| !page.contains("Licensed copy")));
}

#[test]
fn apply_page_normalization_merges_hyphenated_line_wraps() {
    let mut extraction = ExtractedPages {
        pages: vec!["soft-\nware unit".to_string()],
        ..ExtractedPages::default()
    };

    apply_page_normalization(&mut extraction);

    assert_eq!(extraction.dehyphenation_merges, 1);
    assert_eq!(extraction.pages[0], "software unit");
}

#[test]
fn apply_page_normalization_removes_iso_watermark_lines() {
    let mut extraction = ExtractedPages {
            pages: vec![
                "ISO Store Order: OP-1022919 license #1/ Downloaded: 2026-02-14\nSingle user licence only, copying and networking prohibited.\nFunctional safety requirement"
                    .to_string(),
            ],
            ..ExtractedPages::default()
        };

    apply_page_normalization(&mut extraction);

    assert_eq!(extraction.pages[0], "Functional safety requirement");
}

#[test]
fn detect_printed_page_label_supports_numeric_and_roman_labels() {
    let numeric_page = "Some line\nPage 12\n";
    let roman_page = "Some line\nii\n";

    assert_eq!(
        detect_printed_page_label(numeric_page),
        Some("12".to_string())
    );
    assert_eq!(
        detect_printed_page_label(roman_page),
        Some("ii".to_string())
    );
}

#[test]
fn printed_page_labels_for_range_uses_detected_labels_inside_chunk_range() {
    let labels = vec![None, Some("25".to_string()), None, Some("27".to_string())];

    let (start, end) = printed_page_labels_for_range(&labels, 1, 4);
    assert_eq!(start.as_deref(), Some("25"));
    assert_eq!(end.as_deref(), Some("27"));

    let (single_start, single_end) = printed_page_labels_for_range(&labels, 1, 3);
    assert_eq!(single_start.as_deref(), Some("25"));
    assert_eq!(single_end.as_deref(), Some("25"));
}

#[test]
fn split_words_with_overlap_limits_chunk_size() {
    let text = (0..1200)
        .map(|index| format!("w{index}"))
        .collect::<Vec<String>>()
        .join(" ");

    let segments = split_words_with_overlap(&text, 900, 75);
    assert!(segments.len() >= 2);
    assert!(segments
        .iter()
        .all(|segment| segment.split_whitespace().count() <= 900));
}

#[test]
fn split_long_structured_chunks_preserves_reference_and_heading() {
    let body = (0..1200)
        .map(|index| format!("word{index}"))
        .collect::<Vec<String>>()
        .join(" ");
    let input = StructuredChunkDraft {
        chunk_type: ChunkType::Clause,
        reference: "5.2".to_string(),
        ref_path: "5 > 2".to_string(),
        heading: "5.2 Software safety".to_string(),
        text: format!("5.2 Software safety\n\n{body}"),
        page_start: 10,
        page_end: 12,
    };

    let expanded = split_long_structured_chunks(vec![input]);
    assert!(expanded.len() >= 2);
    assert!(expanded.iter().all(|chunk| chunk.reference == "5.2"));
    assert!(expanded
        .iter()
        .all(|chunk| chunk.heading == "5.2 Software safety"));
    assert!(expanded
        .iter()
        .all(|chunk| chunk.page_start == 10 && chunk.page_end == 12));
}
