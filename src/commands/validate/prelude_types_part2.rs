#[derive(Debug, Clone, Serialize, Deserialize)]
struct CitationParityBaseline {
    manifest_version: u32,
    run_id: String,
    generated_at: String,
    db_schema_version: Option<String>,
    #[serde(default)]
    decision_id: Option<String>,
    #[serde(default)]
    change_reason: Option<String>,
    target_linked_count: usize,
    query_options: String,
    checksum: String,
    entries: Vec<CitationParityEntry>,
}

#[derive(Debug, Clone, Serialize)]
struct CitationParityArtifact {
    manifest_version: u32,
    run_id: String,
    generated_at: String,
    baseline_path: String,
    baseline_mode: String,
    baseline_checksum: Option<String>,
    baseline_missing: bool,
    target_linked_count: usize,
    comparable_count: usize,
    top1_parity: Option<f64>,
    top3_containment: Option<f64>,
    page_range_parity: Option<f64>,
    baseline_created: bool,
    entries: Vec<CitationParityComparisonEntry>,
}

#[derive(Debug, Clone, Serialize)]
struct CitationParityComparisonEntry {
    target_id: String,
    top1_match: bool,
    top3_contains_baseline: bool,
    page_range_match: bool,
}

#[derive(Debug)]
struct Wp2Assessment {
    checks: Vec<QualityCheck>,
    extraction_fidelity: ExtractionFidelityReport,
    hierarchy_semantics: HierarchySemanticsReport,
    table_semantics: TableSemanticsReport,
    citation_parity: CitationParitySummaryReport,
    recommendations: Vec<String>,
}

#[derive(Debug, Clone)]
struct NamedIngestRunSnapshot {
    manifest_name: String,
    snapshot: IngestRunSnapshot,
}

#[derive(Debug, Deserialize)]
struct TargetSectionsManifest {
    #[serde(default)]
    target_count: Option<usize>,
    targets: Vec<TargetSectionReference>,
}

#[derive(Debug, Deserialize)]
struct TargetSectionReference {
    id: String,
    part: u32,
}

