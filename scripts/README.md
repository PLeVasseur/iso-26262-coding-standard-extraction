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
- `SMOKE_IDEMPOTENCE=1` to run a second ingest and compare selected counters

Example:

```bash
SMOKE_IDEMPOTENCE=1 scripts/smoke_part6.sh
```
