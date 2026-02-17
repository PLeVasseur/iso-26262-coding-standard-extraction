# Smoke Tests

Use `scripts/smoke_part6.sh` as a quick pass/fail sanity check for hierarchy extraction and citation output.

## Default run

```bash
scripts/smoke_part6.sh
```

## Optional environment overrides

- `CACHE_ROOT` (default `.cache/iso26262`)
- `PART` (default `6`)
- `MAX_PAGES` (default `60`)
- `SMOKE_DETERMINISM=1` to rerun validate and assert stable report/query outputs
- `SMOKE_IDEMPOTENCE=1` to run a second ingest and compare selected counters

Example:

```bash
SMOKE_DETERMINISM=1 SMOKE_IDEMPOTENCE=1 scripts/smoke_part6.sh
```

## Refresh Local Quality Artifacts

Use `scripts/refresh_quality_artifacts.sh` to run the phase gate bundle and refresh local run-state artifacts in `.cache/iso26262/manifests/`.

The script now enforces a deterministic runbook flow:

- `R00` branch preflight (mainline mode expects `main`)
- `R01` config-path check (`OPENCODE_CONFIG_DIR`)
- `R04` target register refresh (`target_sections.json` + `target_sections.csv`)
- `R05` ingest
- `R06` query + validate quality gate
- `R07` traceability matrix presence/build check
- `R08` quality report verification
- `R09` run-state finalization + decision-log append

Resume behavior is strict:

- `run_state.status == running` resumes from `current_step`
- `run_state.status == failed` resumes from `failed_step`/`current_step`
- `run_state.status == blocked` stops until compatibility issue is resolved
- compatibility mismatches hard-block by default (engine/schema drift)

```bash
scripts/refresh_quality_artifacts.sh
```

Optional environment overrides:

- `CACHE_ROOT` (default `.cache/iso26262`)
- `PART` (default `6`)
- `MAX_PAGES` (default `60`)
- `PHASE_ID` (default `phase-8`)
- `PHASE_NAME` (default `Phase 8 - Deterministic runbook and crash recovery`)
- `BASE_BRANCH` (default `main`)
- `UPDATE_DECISIONS=0` to skip appending `decisions_log.jsonl`
- `RUNBOOK_VERSION` (default `1.0`)
- `EXPECTED_DB_SCHEMA_VERSION` (default `0.3.0`)
- `REBUILD_ON_COMPAT_MISMATCH=1` to archive DB and rebuild instead of hard-blocking
- `ALLOW_BLOCKED_RESUME=1` to explicitly clear a blocked run-state and restart from `R04`
