use std::fs;

use anyhow::{Context, Result};
use rusqlite::Connection;
use tracing::{info, warn};

use crate::cli::StatusArgs;
use crate::model::{PdfInventoryManifest, RunStateManifest};

pub fn run(args: StatusArgs) -> Result<()> {
    let manifest_dir = args.cache_root.join("manifests");
    let run_state_path = manifest_dir.join("run_state.json");
    let inventory_path = manifest_dir.join("pdf_inventory.json");
    let db_path = args.cache_root.join("iso26262_index.sqlite");

    info!(cache_root = %args.cache_root.display(), "status requested");

    if run_state_path.exists() {
        let raw = fs::read(&run_state_path)
            .with_context(|| format!("failed to read {}", run_state_path.display()))?;
        let state: RunStateManifest = serde_json::from_slice(&raw)
            .with_context(|| format!("failed to parse {}", run_state_path.display()))?;

        info!(
            run_id = %state.active_run_id.unwrap_or_default(),
            phase = %state.current_phase.unwrap_or_default(),
            phase_id = %state.phase_id.unwrap_or_default(),
            step = %state.current_step.unwrap_or_default(),
            status = %state.status.unwrap_or_default(),
            base_branch = %state.base_branch.unwrap_or_default(),
            active_branch = %state.active_branch.unwrap_or_default(),
            commit_mode = %state.commit_mode.unwrap_or_default(),
            last_commit = %state.last_commit.unwrap_or_default(),
            started_at = %state.started_at.unwrap_or_default(),
            failed_step = %state.failed_step.unwrap_or_default(),
            failure_reason = %state.failure_reason.unwrap_or_default(),
            resume_from_step = %state.resume_from_step.unwrap_or_default(),
            updated_at = %state.updated_at.unwrap_or_default(),
            last_successful_command = %state.last_successful_command.unwrap_or_default(),
            next_planned_command = %state.next_planned_command.unwrap_or_default(),
            last_successful_artifact = %state.last_successful_artifact.unwrap_or_default(),
            compatibility_status = %state.compatibility.as_ref().and_then(|value| value.status.as_ref()).cloned().unwrap_or_default(),
            compatibility_reason = %state.compatibility.as_ref().and_then(|value| value.reason.as_ref()).cloned().unwrap_or_default(),
            compatibility_runbook_version = %state.compatibility.as_ref().and_then(|value| value.runbook_version.as_ref()).cloned().unwrap_or_default(),
            compatibility_engine_version = %state.compatibility.as_ref().and_then(|value| value.engine_version.as_ref()).cloned().unwrap_or_default(),
            compatibility_db_schema_version = %state.compatibility.as_ref().and_then(|value| value.db_schema_version.as_ref()).cloned().unwrap_or_default(),
            "loaded run-state manifest"
        );
    } else {
        warn!(path = %run_state_path.display(), "run-state manifest missing");
    }

    if inventory_path.exists() {
        let raw = fs::read(&inventory_path)
            .with_context(|| format!("failed to read {}", inventory_path.display()))?;
        let inventory: PdfInventoryManifest = serde_json::from_slice(&raw)
            .with_context(|| format!("failed to parse {}", inventory_path.display()))?;

        info!(
            generated_at = %inventory.generated_at,
            pdf_count = inventory.pdf_count,
            "loaded inventory manifest"
        );
    } else {
        warn!(path = %inventory_path.display(), "inventory manifest missing");
    }

    if db_path.exists() {
        let conn = Connection::open(&db_path)
            .with_context(|| format!("failed to open {}", db_path.display()))?;
        let docs_count = query_count(&conn, "SELECT COUNT(*) FROM docs").unwrap_or(0);
        let chunks_count = query_count(&conn, "SELECT COUNT(*) FROM chunks").unwrap_or(0);

        info!(
            path = %db_path.display(),
            docs = docs_count,
            chunks = chunks_count,
            "database status"
        );
    } else {
        warn!(path = %db_path.display(), "database file missing");
    }

    Ok(())
}

fn query_count(conn: &Connection, sql: &str) -> Result<i64> {
    let count = conn.query_row(sql, [], |row| row.get(0))?;
    Ok(count)
}
