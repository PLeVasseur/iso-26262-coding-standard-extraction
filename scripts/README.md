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

Script preflight dependencies:

- `cargo`
- `jq`
- `git`

Resume behavior is strict:

- `run_state.status == running` resumes from `current_step`
- `run_state.status == failed` resumes from `failed_step`/`current_step`
- `run_state.status == blocked` stops until compatibility issue is resolved
- compatibility mismatches hard-block by default (engine/schema drift)

```bash
scripts/refresh_quality_artifacts.sh
```

### Quick parser iteration mode (Part 6)

Default behavior is optimized for Part 6 parser iteration:

```bash
PART=6 MAX_PAGES=60 scripts/refresh_quality_artifacts.sh
```

Quick mode tolerates `Q-022` freshness failures (expected when only Part 6 is refreshed) but still requires all other checks to pass.

### WP2 fidelity gate stage

`validate` supports stage-aware WP2 gate enforcement via `WP2_GATE_STAGE`:

- `WP2_GATE_STAGE=A` (default): instrumentation mode.
  - Stage B-only WP2 thresholds (`Q-023`..`Q-030`) are emitted as warnings in report sections.
  - Hard-fail conditions still fail immediately.
- `WP2_GATE_STAGE=B`: hard gate mode.
  - Stage B-only WP2 thresholds are enforced as check failures.

Citation parity lockfile policy (`Q-030`):

- Canonical lockfile path defaults to `manifests/citation_parity_baseline.lock.json` (repo-tracked).
- Lockfile content is metadata-only (target ids, references, anchor identities, and page ranges), not extracted text/snippets.
- Stage B hard-fails when this lockfile is missing.
- Standard `validate` runs do not auto-create the lockfile.
- Explicit bootstrap/rotation is required:

```bash
WP2_CITATION_BASELINE_MODE=bootstrap WP2_GATE_STAGE=A cargo run -- validate --cache-root .cache/iso26262
```

When rotating an existing lockfile, provide rationale metadata:

```bash
WP2_CITATION_BASELINE_MODE=bootstrap \
WP2_CITATION_BASELINE_DECISION_ID=D-0123 \
WP2_CITATION_BASELINE_REASON="retargeted corpus after approved source update" \
WP2_GATE_STAGE=A cargo run -- validate --cache-root .cache/iso26262
```

Examples:

```bash
WP2_GATE_STAGE=A FULL_TARGET_SET=1 TARGET_PARTS="2 6 8 9" scripts/refresh_quality_artifacts.sh
```

```bash
WP2_GATE_STAGE=B cargo run -- validate --cache-root .cache/iso26262
```

### Full-target freshness mode (WP1)

Use full-target mode to refresh all target parts in one ingest cycle (Parts 2, 6, 8, 9):

```bash
FULL_TARGET_SET=1 TARGET_PARTS="2 6 8 9" scripts/refresh_quality_artifacts.sh
```

Optional full-mode page cap:

```bash
FULL_TARGET_SET=1 TARGET_PARTS="2 6 8 9" FULL_MAX_PAGES=120 scripts/refresh_quality_artifacts.sh
```

Optional environment overrides:

- `CACHE_ROOT` (default `.cache/iso26262`)
- `PART` (default `6`)
- `MAX_PAGES` (default `60`)
- `FULL_TARGET_SET=1` to enable full-target ingest mode
- `TARGET_PARTS` (default `2 6 8 9`) used when `FULL_TARGET_SET=1`
- `FULL_MAX_PAGES` (default `0`) optional page cap for full-target mode (`0` means no cap)
- `PHASE_ID` (default `phase-8`)
- `PHASE_NAME` (default `Phase 8 - Deterministic runbook and crash recovery`)
- `BASE_BRANCH` (default `main`)
- `UPDATE_DECISIONS=0` to skip appending `decisions_log.jsonl`
- `RUNBOOK_VERSION` (default `1.0`)
- `EXPECTED_DB_SCHEMA_VERSION` (default `0.3.0`)
- `REBUILD_ON_COMPAT_MISMATCH=1` to archive DB and rebuild instead of hard-blocking
- `ALLOW_BLOCKED_RESUME=1` to explicitly clear a blocked run-state and restart from `R04`
- `WP2_CITATION_BASELINE_MODE` (`verify` default, `bootstrap` to create/rotate lockfile)
- `WP2_CITATION_BASELINE_PATH` to override lockfile location (default `manifests/citation_parity_baseline.lock.json`)
- `WP2_CITATION_BASELINE_DECISION_ID` required for lockfile rotation in bootstrap mode
- `WP2_CITATION_BASELINE_REASON` required for lockfile rotation in bootstrap mode

### New-session bootstrap

When starting a fresh terminal/session, bootstrap in this order:

1. `export OPENCODE_CONFIG_DIR=...` (must exist; `R01` blocks when unset/missing).
2. Confirm branch is `main` (`R00` mainline check).
3. Run quick mode first for parser iteration:

```bash
PART=6 MAX_PAGES=60 WP2_GATE_STAGE=A scripts/refresh_quality_artifacts.sh
```

4. Run full-target refresh before promotion evidence:

```bash
FULL_TARGET_SET=1 TARGET_PARTS="2 6 8 9" WP2_GATE_STAGE=A scripts/refresh_quality_artifacts.sh
```

5. Run Stage B validate to inspect hard-gate readiness:

```bash
WP2_GATE_STAGE=B cargo run -- validate --cache-root .cache/iso26262
```

If Stage B reports missing citation parity lockfile, bootstrap once in Stage A:

```bash
WP2_CITATION_BASELINE_MODE=bootstrap WP2_GATE_STAGE=A cargo run -- validate --cache-root .cache/iso26262
```
