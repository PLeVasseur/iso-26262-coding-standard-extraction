#!/usr/bin/env bash
set -euo pipefail

RUST_MAX="${RUST_MAX:-500}"
SHELL_MAX="${SHELL_MAX:-500}"
MODE="${MODE:-warn}"
EXCEPTIONS_FILE="${EXCEPTIONS_FILE:-${OPENCODE_CONFIG_DIR:-}/plans/wp3-modularization-exceptions.md}"

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  cat <<'EOF'
Usage: scripts/check_file_size_budget.sh

Advisory-first file-size review for tracked Rust and shell files.

Environment overrides:
  RUST_MAX         Max lines for .rs files (default: 500)
  SHELL_MAX        Max lines for .sh files (default: 500)
  MODE             warn (default, advisory) or enforce (fails on breaches)
  EXCEPTIONS_FILE  Optional exemptions list

Examples:
  scripts/check_file_size_budget.sh
  MODE=enforce scripts/check_file_size_budget.sh
EOF
  exit 0
fi

if [[ "${MODE}" != "warn" && "${MODE}" != "enforce" ]]; then
  printf '[size-budget][FAIL] MODE must be warn or enforce (got: %s)\n' "${MODE}" >&2
  exit 2
fi

declare -A EXCEPTIONS=()

if [[ -n "${EXCEPTIONS_FILE}" && -f "${EXCEPTIONS_FILE}" ]]; then
  while IFS= read -r line; do
    if [[ "${line}" =~ ^[[:space:]]*-[[:space:]]*path:[[:space:]]*(.+)[[:space:]]*$ ]]; then
      path="${BASH_REMATCH[1]}"
      path="${path#\"}"
      path="${path%\"}"
      EXCEPTIONS["${path}"]=1
    fi
  done < "${EXCEPTIONS_FILE}"
fi

declare -a BREACHES=()

check_file() {
  local path="$1"
  local max="$2"
  local kind="$3"
  local line_count

  if [[ ! -f "${path}" ]]; then
    return
  fi

  line_count="$(wc -l < "${path}")"
  line_count="${line_count//[[:space:]]/}"

  if (( line_count > max )); then
    if [[ -n "${EXCEPTIONS[${path}]:-}" ]]; then
      printf '[size-budget][EXEMPT] %s %s has %s lines (limit %s)\n' "${kind}" "${path}" "${line_count}" "${max}"
      return
    fi

    BREACHES+=("${kind}|${path}|${line_count}|${max}")
  fi
}

mapfile -t RUST_FILES < <(git ls-files --cached --others --exclude-standard -- '*.rs')
mapfile -t SHELL_FILES < <(git ls-files --cached --others --exclude-standard -- '*.sh')

for path in "${RUST_FILES[@]}"; do
  check_file "${path}" "${RUST_MAX}" "rust"
done

for path in "${SHELL_FILES[@]}"; do
  check_file "${path}" "${SHELL_MAX}" "shell"
done

if (( ${#BREACHES[@]} == 0 )); then
  printf '[size-budget][ADVISORY] no non-exempt .rs/.sh files exceed configured review thresholds\n'
  exit 0
fi

printf '[size-budget][ADVISORY] found %s review item(s) above line thresholds:\n' "${#BREACHES[@]}"
for breach in "${BREACHES[@]}"; do
  IFS='|' read -r kind path lines limit <<< "${breach}"
  printf '  - [%s] %s has %s lines (limit %s)\n' "${kind}" "${path}" "${lines}" "${limit}"
done

if [[ "${MODE}" == "enforce" ]]; then
  printf '[size-budget][FAIL] mode=enforce and non-exempt review items were found\n' >&2
  exit 1
fi

printf '[size-budget][WARN] mode=warn (advisory); continuing without blocking\n'
