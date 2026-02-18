#!/usr/bin/env bash
set -euo pipefail

COMMAND=""
MODE="full"
RUN_ID=""
SOURCE_CACHE_ROOT=".cache/iso26262"
OUTPUT_ROOT=".cache/iso26262/regression"
THRESHOLDS_PATH="scripts/lib/regression/thresholds.json"
FORCE="0"

usage() {
  cat <<'EOF'
Usage:
  scripts/regression_gate.sh before  --run-id <id> [options]
  scripts/regression_gate.sh after   --run-id <id> [options]
  scripts/regression_gate.sh compare --run-id <id> [options]

Options:
  --mode <lite|full>                Default: full
  --source-cache-root <path>        Default: .cache/iso26262 (before/after only)
  --output-root <path>              Default: .cache/iso26262/regression
  --thresholds <path>               Default: scripts/lib/regression/thresholds.json (compare only)
  --force                           Overwrite existing phase folder (before/after only)
  -h, --help                        Show this help message
EOF
}

log() {
  printf '[reg-gate] %s\n' "$*"
}

fail() {
  printf '[reg-gate][FAIL] %s\n' "$*" >&2
  exit 1
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

if [[ "$1" == "-h" || "$1" == "--help" ]]; then
  usage
  exit 0
fi

COMMAND="$1"
shift

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-id)
      RUN_ID="$2"
      shift 2
      ;;
    --mode)
      MODE="$2"
      shift 2
      ;;
    --source-cache-root)
      SOURCE_CACHE_ROOT="$2"
      shift 2
      ;;
    --output-root)
      OUTPUT_ROOT="$2"
      shift 2
      ;;
    --thresholds)
      THRESHOLDS_PATH="$2"
      shift 2
      ;;
    --force)
      FORCE="1"
      shift 1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      fail "unknown argument: $1"
      ;;
  esac
done

if [[ -z "$RUN_ID" ]]; then
  fail "--run-id is required"
fi

case "$MODE" in
  lite|full) ;;
  *) fail "--mode must be one of: lite, full" ;;
esac

case "$COMMAND" in
  before)
    args=(
      scripts/regression_capture.sh
      --run-id "$RUN_ID"
      --phase before
      --mode "$MODE"
      --source-cache-root "$SOURCE_CACHE_ROOT"
      --output-root "$OUTPUT_ROOT"
    )
    if [[ "$FORCE" == "1" ]]; then
      args+=(--force)
    fi
    log "Running before capture"
    "${args[@]}"
    ;;
  after)
    args=(
      scripts/regression_capture.sh
      --run-id "$RUN_ID"
      --phase after
      --mode "$MODE"
      --source-cache-root "$SOURCE_CACHE_ROOT"
      --output-root "$OUTPUT_ROOT"
    )
    if [[ "$FORCE" == "1" ]]; then
      args+=(--force)
    fi
    log "Running after capture"
    "${args[@]}"
    ;;
  compare)
    log "Running compare"
    scripts/regression_compare.sh \
      --run-id "$RUN_ID" \
      --mode "$MODE" \
      --output-root "$OUTPUT_ROOT" \
      --thresholds "$THRESHOLDS_PATH"
    ;;
  *)
    fail "unknown command: $COMMAND (expected before, after, or compare)"
    ;;
esac
