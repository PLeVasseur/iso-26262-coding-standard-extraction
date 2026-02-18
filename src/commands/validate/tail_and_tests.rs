fn format_page_range(start: Option<i64>, end: Option<i64>) -> String {
    match (start, end) {
        (Some(start), Some(end)) if start == end => start.to_string(),
        (Some(start), Some(end)) => format!("{start}-{end}"),
        (Some(start), None) => start.to_string(),
        (None, Some(end)) => end.to_string(),
        (None, None) => "unknown".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ensure_citation_baseline_metadata_only, parse_citation_baseline_mode,
        parse_citation_baseline_path, parse_target_parts_from_command, resolve_processed_parts,
        CitationBaselineMode, GoldReference, IngestRunSnapshot,
    };

    #[test]
    fn gold_reference_deserializes_without_wp1_optional_fields() {
        let raw = r#"
        {
          "id": "G-legacy",
          "doc_id": "ISO26262-6-2018",
          "ref": "8.4.5",
          "expected_page_pattern": "26-27",
          "must_match_terms": ["source code"],
          "status": "pass"
        }
        "#;

        let reference: GoldReference =
            serde_json::from_str(raw).expect("legacy gold row should deserialize");
        assert_eq!(reference.reference, "8.4.5");
        assert!(reference.target_id.is_none());
        assert!(reference.target_ref_raw.is_none());
        assert!(reference.canonical_ref.is_none());
        assert!(reference.ref_resolution_mode.is_none());
    }

    #[test]
    fn parse_target_parts_from_command_extracts_and_deduplicates_values() {
        let command = "iso26262 ingest --cache-root .cache/iso26262 --target-part 8 --target-part 2 --target-part 8";
        let parts = parse_target_parts_from_command(command);
        assert_eq!(parts, vec![2, 8]);
    }

    #[test]
    fn resolve_processed_parts_falls_back_to_required_parts_when_missing() {
        let snapshot = IngestRunSnapshot::default();
        let parts = resolve_processed_parts(&snapshot, &[2, 6, 8, 9]);
        assert_eq!(parts, vec![2, 6, 8, 9]);
    }

    #[test]
    fn parse_citation_baseline_mode_supports_bootstrap_aliases() {
        assert_eq!(
            parse_citation_baseline_mode(Some("bootstrap")),
            CitationBaselineMode::Bootstrap
        );
        assert_eq!(
            parse_citation_baseline_mode(Some("RoTaTe")),
            CitationBaselineMode::Bootstrap
        );
        assert_eq!(
            parse_citation_baseline_mode(Some("verify")),
            CitationBaselineMode::Verify
        );
        assert_eq!(parse_citation_baseline_mode(None), CitationBaselineMode::Verify);
    }

    #[test]
    fn parse_citation_baseline_path_defaults_to_repo_lockfile() {
        let path = parse_citation_baseline_path(None);
        assert_eq!(
            path,
            std::path::PathBuf::from("manifests/citation_parity_baseline.lock.json")
        );

        let custom = parse_citation_baseline_path(Some("/tmp/custom.lock.json"));
        assert_eq!(custom, std::path::PathBuf::from("/tmp/custom.lock.json"));
    }

    #[test]
    fn citation_baseline_schema_guard_rejects_text_payload_fields() {
        let payload = serde_json::json!({
            "manifest_version": 1,
            "entries": [
                {
                    "target_id": "t1",
                    "text": "forbidden"
                }
            ]
        });

        let error = ensure_citation_baseline_metadata_only(&payload)
            .expect_err("schema guard should reject text-bearing fields");
        assert!(
            error.to_string().contains("forbidden text-bearing key"),
            "unexpected error: {}",
            error
        );
    }
}
