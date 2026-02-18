# shellcheck shell=bash

log() {
  printf '[refresh] %s\n' "$*"
}


fail() {
  printf '[refresh][FAIL] %s\n' "$*" >&2
  exit 1
}


require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    fail "required command not found: $1"
  fi
}


utc_now() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}


current_branch() {
  git rev-parse --abbrev-ref HEAD 2>/dev/null || printf 'main'
}


step_rank() {
  case "$1" in
    R00-*) printf '0\n' ;;
    R01-*) printf '1\n' ;;
    R02-*) printf '2\n' ;;
    R03-*) printf '3\n' ;;
    R04-*) printf '4\n' ;;
    R05-*) printf '5\n' ;;
    R06-*) printf '6\n' ;;
    R07-*) printf '7\n' ;;
    R08-*) printf '8\n' ;;
    R09-*) printf '9\n' ;;
    *) printf '4\n' ;;
  esac
}


normalize_step() {
  case "$1" in
    R04-*|R05-*|R06-*|R07-*|R08-*|R09-*) printf '%s\n' "$1" ;;
    *) printf 'R04-TARGET-REFRESH\n' ;;
  esac
}


should_run_step() {
  local step="$1"
  local step_num start_num
  step_num="$(step_rank "$step")"
  start_num="$(step_rank "$START_STEP")"
  [[ "$step_num" -ge "$start_num" ]]
}


ensure_mainline_branch() {
  local active_branch
  active_branch="$(current_branch)"
  if [[ "$active_branch" != "$BASE_BRANCH" ]]; then
    fail "R00 branch check failed: expected '${BASE_BRANCH}', found '${active_branch}'"
  fi

  if [[ "$BASE_BRANCH" == "main" && "$active_branch" != "main" ]]; then
    fail "R00 mainline mode requires active branch 'main'"
  fi
}


ensure_config_path() {
  if [[ -z "${OPENCODE_CONFIG_DIR:-}" ]]; then
    fail "R01 config check failed: OPENCODE_CONFIG_DIR is not set"
  fi

  if [[ ! -d "${OPENCODE_CONFIG_DIR}" ]]; then
    fail "R01 config check failed: OPENCODE_CONFIG_DIR does not exist: ${OPENCODE_CONFIG_DIR}"
  fi
}
