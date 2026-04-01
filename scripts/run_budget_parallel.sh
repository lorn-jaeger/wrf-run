#!/usr/bin/env bash
set -u
set -o pipefail

MAX_DAYS=${MAX_DAYS:-10}
MAX_JOBS=${MAX_JOBS:-500}
SLEEP_SECS=${SLEEP_SECS:-60}
RUN_ARGS=()
DAYS=()
PIDS=()
PID_DAYS=()
CLEANUP_MODE="none"
CLEANUP_ARGS=()

usage() {
  cat <<'EOF'
Usage: scripts/run_budget_parallel.sh [options] DAY_INDEX [DAY_INDEX ...]

Options:
  --max-days N        Max concurrent run_budget_day processes (default: 10)
  --max-jobs N        Max total PBS jobs before throttling (default: 500)
  --sleep SECS        Sleep between checks (default: 60)
  --runs-file PATH    Pass through to run_budget_day.py
  --dry-run           Pass through to run_budget_day.py
  --start-fire N      Pass through to run_budget_day.py
  --wrfout-threshold N  Pass through to run_budget_day.py
  --no-auto-stages    Pass through to run_budget_day.py
  --auto-stages       Pass through to run_budget_day.py
  --cleanup           Run fires/cleanup_day.py --execute after a day completes successfully
  --cleanup-dry-run   Run fires/cleanup_day.py without --execute after success
  --cleanup-delete-hrrr  Include --delete-hrrr when running cleanup
  --cleanup-force     Include --force when running cleanup (require all done)
  --                 Pass remaining args directly to run_budget_day.py

Example:
  scripts/run_budget_parallel.sh --max-days 10 --max-jobs 400 668 669 670
EOF
}

count_jobs() {
  if ! command -v qstat >/dev/null 2>&1; then
    echo 0
    return
  fi
  qstat -u "${USER}" 2>/dev/null | awk 'NR>5{c++} END{print c+0}'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --max-days)
      MAX_DAYS="$2"; shift 2 ;;
    --max-jobs)
      MAX_JOBS="$2"; shift 2 ;;
    --sleep)
      SLEEP_SECS="$2"; shift 2 ;;
    --runs-file)
      RUN_ARGS+=(--runs-file "$2"); shift 2 ;;
    --start-fire)
      RUN_ARGS+=(--start-fire "$2"); shift 2 ;;
    --wrfout-threshold)
      RUN_ARGS+=(--wrfout-threshold "$2"); shift 2 ;;
    --cleanup)
      CLEANUP_MODE="execute"; shift ;;
    --cleanup-dry-run)
      CLEANUP_MODE="dry-run"; shift ;;
    --cleanup-delete-hrrr)
      CLEANUP_ARGS+=(--delete-hrrr); shift ;;
    --cleanup-force)
      CLEANUP_ARGS+=(--force); shift ;;
    --dry-run|--no-auto-stages|--auto-stages)
      RUN_ARGS+=("$1"); shift ;;
    --)
      shift
      RUN_ARGS+=("$@")
      break ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      DAYS+=("$1"); shift ;;
  esac
done

if [[ ${#DAYS[@]} -eq 0 ]]; then
  usage
  exit 1
fi

echo "[INFO] max days: ${MAX_DAYS}, max jobs: ${MAX_JOBS}, sleep: ${SLEEP_SECS}s"
echo "[INFO] days: ${DAYS[*]}"

for day in "${DAYS[@]}"; do
  while [[ "$(count_jobs)" -ge "${MAX_JOBS}" ]]; do
    echo "[WAIT] job cap reached ($(count_jobs)/${MAX_JOBS}); sleeping ${SLEEP_SECS}s"
    sleep "${SLEEP_SECS}"
  done

  while [[ "$(jobs -rp | wc -l)" -ge "${MAX_DAYS}" ]]; do
    echo "[WAIT] day cap reached ($(jobs -rp | wc -l)/${MAX_DAYS}); sleeping ${SLEEP_SECS}s"
    sleep "${SLEEP_SECS}"
  done

  echo "[LAUNCH] day-index ${day}"
  python fires/run_budget_day.py --day-index "${day}" "${RUN_ARGS[@]}" &
  PIDS+=("$!")
  PID_DAYS+=("${day}")
done

fail=0
for i in "${!PIDS[@]}"; do
  pid="${PIDS[$i]}"
  day="${PID_DAYS[$i]}"
  if ! wait "${pid}"; then
    echo "[WARN] day-index ${day} (pid ${pid}) failed"
    fail=1
  fi
  if [[ "${CLEANUP_MODE}" != "none" ]]; then
    echo "[CLEANUP] day-index ${day} (${CLEANUP_MODE})"
    if [[ "${CLEANUP_MODE}" == "execute" ]]; then
      python fires/cleanup_day.py --day-index "${day}" --execute "${CLEANUP_ARGS[@]}"
    else
      python fires/cleanup_day.py --day-index "${day}" "${CLEANUP_ARGS[@]}"
    fi
  fi
done
if [[ "${fail}" -ne 0 ]]; then
  echo "[WARN] One or more day runs failed; continuing."
fi
