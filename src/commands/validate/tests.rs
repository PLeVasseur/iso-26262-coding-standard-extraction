#[cfg(test)]
use super::*;

#[cfg(test)]
mod tests {
    use super::{
        bootstrap_confidence_interval_95, ensure_citation_baseline_metadata_only,
        fill_missing_judged_chunk_ids, parse_citation_baseline_mode, parse_citation_baseline_path,
        parse_semantic_baseline_mode, parse_semantic_baseline_path,
        parse_target_parts_from_command, resolve_processed_parts, sign_test_two_sided_p_value,
        CitationBaselineMode, GoldReference, IngestRunSnapshot, SemanticBaselineMode,
        SemanticEvalManifest, SemanticEvalQuery,
    };
    use rusqlite::Connection;

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
        assert_eq!(
            parse_citation_baseline_mode(None),
            CitationBaselineMode::Verify
        );
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

    #[test]
    fn sign_test_reports_small_p_for_positive_shift() {
        let deltas = vec![0.10, 0.08, 0.07, 0.06, 0.09];
        let p_value =
            sign_test_two_sided_p_value(&deltas).expect("sign test should produce p-value");
        assert!(p_value < 0.10, "unexpected p-value: {p_value}");
    }

    #[test]
    fn bootstrap_ci_is_deterministic_with_fixed_seed() {
        let deltas = vec![0.30, 0.25, 0.28, 0.22, 0.27, 0.24];
        let first = bootstrap_confidence_interval_95(&deltas, 500, 0xC0FFEE_u64)
            .expect("ci should be computed");
        let second = bootstrap_confidence_interval_95(&deltas, 500, 0xC0FFEE_u64)
            .expect("ci should be computed");
        assert_eq!(
            first, second,
            "bootstrap CI should be deterministic for fixed seed"
        );

        let (low, high) = first;
        let low = low.expect("low bound should be present");
        let high = high.expect("high bound should be present");
        assert!(low <= high, "invalid CI bounds: low={low}, high={high}");
    }

    #[test]
    fn fill_missing_judged_chunk_ids_enriches_empty_labels() {
        let connection = Connection::open_in_memory().expect("in-memory DB should open");
        connection
            .execute_batch(
                "
                CREATE TABLE chunks (
                  chunk_id TEXT PRIMARY KEY,
                  doc_id TEXT NOT NULL,
                  page_pdf_start INTEGER
                );
                INSERT INTO chunks (chunk_id, doc_id, page_pdf_start) VALUES
                  ('c-001', 'doc-1', 10),
                  ('c-002', 'doc-1', 11),
                  ('c-003', 'doc-1', 14);
                ",
            )
            .expect("seed rows should insert");

        let mut manifest = SemanticEvalManifest {
            manifest_version: 1,
            generated_at: "now".to_string(),
            source: "test".to_string(),
            queries: vec![SemanticEvalQuery {
                query_id: "q-1".to_string(),
                query_text: "8.4.5".to_string(),
                intent: "exact_ref".to_string(),
                expected_chunk_ids: vec!["c-002".to_string()],
                judged_chunk_ids: vec![],
                expected_refs: vec![],
                must_hit_top1: true,
                part_filter: None,
                chunk_type_filter: None,
                notes: None,
            }],
        };

        let changed = fill_missing_judged_chunk_ids(&connection, &mut manifest)
            .expect("enrichment should succeed");
        assert!(changed, "manifest should be marked as changed");

        let judged = &manifest.queries[0].judged_chunk_ids;
        assert!(!judged.is_empty(), "judged ids should be backfilled");
        assert!(
            judged.contains(&"c-002".to_string()),
            "expected chunk should be included in judged set"
        );
    }

    #[test]
    fn parse_semantic_baseline_mode_supports_bootstrap_aliases() {
        assert_eq!(
            parse_semantic_baseline_mode(Some("bootstrap")),
            SemanticBaselineMode::Bootstrap
        );
        assert_eq!(
            parse_semantic_baseline_mode(Some("RoTaTe")),
            SemanticBaselineMode::Bootstrap
        );
        assert_eq!(
            parse_semantic_baseline_mode(Some("verify")),
            SemanticBaselineMode::Verify
        );
        assert_eq!(
            parse_semantic_baseline_mode(None),
            SemanticBaselineMode::Verify
        );
    }

    #[test]
    fn parse_semantic_baseline_path_defaults_to_repo_lockfile() {
        let path = parse_semantic_baseline_path(None);
        assert_eq!(
            path,
            std::path::PathBuf::from("manifests/semantic_retrieval_baseline.lock.json")
        );

        let custom = parse_semantic_baseline_path(Some("/tmp/semantic.lock.json"));
        assert_eq!(custom, std::path::PathBuf::from("/tmp/semantic.lock.json"));
    }
}
