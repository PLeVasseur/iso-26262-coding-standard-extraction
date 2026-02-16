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

```bash
scripts/refresh_quality_artifacts.sh
```

Optional environment overrides:

- `CACHE_ROOT` (default `.cache/iso26262`)
- `PART` (default `6`)
- `MAX_PAGES` (default `60`)
- `PHASE_ID` (default `phase-7`)
- `PHASE_NAME` (default `Phase 7 - Regression and determinism`)
- `BASE_BRANCH` (default `main`)
- `UPDATE_DECISIONS=0` to skip appending `decisions_log.jsonl`
