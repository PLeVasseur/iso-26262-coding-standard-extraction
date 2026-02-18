#!/usr/bin/env bash
set -euo pipefail

RUST_MAX="${RUST_MAX:-500}"
SHELL_MAX="${SHELL_MAX:-500}"
MODE="${MODE:-warn}"
EXCEPTIONS_FILE="${EXCEPTIONS_FILE:-${OPENCODE_CONFIG_DIR:-}/plans/wp3-modularization-exceptions.md}"

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
  printf '[size-budget] PASS: all tracked .rs/.sh files are within configured limits\n'
  exit 0
fi

printf '[size-budget] Found %s file-size breach(es):\n' "${#BREACHES[@]}"
for breach in "${BREACHES[@]}"; do
  IFS='|' read -r kind path lines limit <<< "${breach}"
  printf '  - [%s] %s has %s lines (limit %s)\n' "${kind}" "${path}" "${lines}" "${limit}"
done

if [[ "${MODE}" == "enforce" ]]; then
  printf '[size-budget][FAIL] mode=enforce and non-exempt breaches were found\n' >&2
  exit 1
fi

printf '[size-budget][WARN] mode=warn; continuing despite breaches\n'
