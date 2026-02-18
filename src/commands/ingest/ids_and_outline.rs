fn derive_ref_path(reference: &str, chunk_type: ChunkType) -> String {
    match chunk_type {
        ChunkType::Clause => reference.split('.').collect::<Vec<&str>>().join(" > "),
        ChunkType::Table | ChunkType::Annex => reference.to_string(),
    }
}

fn normalize_line(input: &str) -> &str {
    input.trim()
}

fn sanitize_ref_for_id(reference: &str) -> String {
    let mut out = String::with_capacity(reference.len());
    for ch in reference.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }

    while out.contains("__") {
        out = out.replace("__", "_");
    }

    out.trim_matches('_').to_string()
}

fn normalize_marker_label(marker: &str) -> String {
    let trimmed = marker.trim();
    if trimmed.is_empty() {
        return "-".to_string();
    }

    let without_suffix = trimmed.trim_end_matches([')', '.', ':', ';']);
    let canonical_bullet = without_suffix.replace('–', "-").replace('—', "-");
    if canonical_bullet == "-" || canonical_bullet == "*" || canonical_bullet == "•" {
        return "-".to_string();
    }

    let upper = canonical_bullet.to_ascii_uppercase();
    if upper == "NOTE" {
        return "NOTE".to_string();
    }

    if let Some(rest) = upper.strip_prefix("NOTE ") {
        let normalized_rest = rest.trim();
        if !normalized_rest.is_empty() && normalized_rest.chars().all(|ch| ch.is_ascii_digit()) {
            return format!("NOTE {}", normalized_rest);
        }
    }

    canonical_bullet.to_ascii_lowercase()
}

fn build_citation_anchor_id(
    doc_id: &str,
    parent_ref: &str,
    anchor_type: &str,
    anchor_label_norm: Option<&str>,
    anchor_order: Option<i64>,
) -> String {
    let parent_key = sanitize_ref_for_id(parent_ref);
    let label_key = anchor_label_norm
        .map(sanitize_ref_for_id)
        .filter(|value| !value.is_empty())
        .or_else(|| anchor_order.map(|value| value.to_string()))
        .unwrap_or_else(|| "root".to_string());

    format!(
        "{}:{}:{}:{}",
        doc_id,
        parent_key,
        sanitize_ref_for_id(anchor_type),
        label_key
    )
}

fn extract_section_headings_with_pdftohtml(pdf_path: &Path) -> Result<Vec<SectionHeadingDraft>> {
    let output = Command::new("pdftohtml")
        .arg("-xml")
        .arg("-f")
        .arg("1")
        .arg("-l")
        .arg("1")
        .arg(pdf_path)
        .arg("-stdout")
        .output()
        .with_context(|| format!("failed to execute pdftohtml for {}", pdf_path.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "pdftohtml returned non-zero exit status for {}: {}",
            pdf_path.display(),
            stderr.trim()
        );
    }

    let xml = String::from_utf8_lossy(&output.stdout);
    let item_regex = Regex::new(r#"<item page="(\d+)">(.*?)</item>"#)
        .context("failed to compile outline item regex")?;
    let section_heading_regex =
        Regex::new(r"^\s*(\d+)\s+(.+)$").context("failed to compile section heading regex")?;

    let mut section_headings = Vec::<SectionHeadingDraft>::new();
    let mut seen_refs = HashSet::<String>::new();

    for captures in item_regex.captures_iter(&xml) {
        let page_pdf = captures
            .get(1)
            .and_then(|value| value.as_str().parse::<i64>().ok())
            .unwrap_or(1);

        let raw_label = captures.get(2).map(|value| value.as_str()).unwrap_or("");
        let normalized_label = normalize_outline_label(raw_label);

        let Some(section_captures) = section_heading_regex.captures(&normalized_label) else {
            continue;
        };

        let reference = section_captures
            .get(1)
            .map(|value| value.as_str().trim())
            .unwrap_or_default();
        let title = section_captures
            .get(2)
            .map(|value| value.as_str().trim())
            .unwrap_or_default();

        if reference.is_empty() || title.is_empty() {
            continue;
        }
        if !seen_refs.insert(reference.to_string()) {
            continue;
        }

        section_headings.push(SectionHeadingDraft {
            reference: reference.to_string(),
            heading: normalized_label,
            page_pdf,
        });
    }

    Ok(section_headings)
}

fn normalize_outline_label(raw_label: &str) -> String {
    raw_label
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace('\u{00a0}', " ")
        .split_whitespace()
        .collect::<Vec<&str>>()
        .join(" ")
}

