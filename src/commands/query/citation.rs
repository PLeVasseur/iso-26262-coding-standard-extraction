use super::run::QueryCandidate;

pub(super) fn format_page_range(start: Option<i64>, end: Option<i64>) -> String {
    match (start, end) {
        (Some(start), Some(end)) if start == end => start.to_string(),
        (Some(start), Some(end)) => format!("{start}-{end}"),
        (Some(start), None) => start.to_string(),
        (None, Some(end)) => end.to_string(),
        (None, None) => "unknown".to_string(),
    }
}

pub(super) fn render_citation(candidate: &QueryCandidate) -> String {
    let reference = if candidate.reference.is_empty() {
        "(unreferenced chunk)".to_string()
    } else {
        candidate.reference.clone()
    };

    let reference_with_anchor = match (
        candidate.anchor_type.as_deref(),
        candidate.anchor_label_norm.as_deref(),
    ) {
        (Some("marker"), Some(label)) if !label.is_empty() => {
            let base = marker_base_reference(&reference);
            if label.starts_with("NOTE") {
                format!("{base}, {label}")
            } else {
                format!("{base}({label})")
            }
        }
        (Some("paragraph"), Some(label)) if !label.is_empty() => {
            let base = marker_base_reference(&reference);
            format!("{base}, para {label}")
        }
        _ => reference,
    };

    format!(
        "ISO 26262-{}:{}, {}, PDF pages {}",
        candidate.part,
        candidate.year,
        reference_with_anchor,
        format_page_range(candidate.page_pdf_start, candidate.page_pdf_end)
    )
}

fn marker_base_reference(reference: &str) -> String {
    if let Some((base, _)) = reference.split_once(" item ") {
        return base.to_string();
    }

    if let Some((base, _)) = reference.split_once(" note ") {
        return base.to_string();
    }

    if let Some((base, _)) = reference.split_once(" para ") {
        return base.to_string();
    }

    if let Some((base, _)) = reference.split_once(" row ") {
        return base.to_string();
    }

    reference.to_string()
}
