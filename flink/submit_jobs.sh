#!/bin/bash
set -euo pipefail

# ── Load .env from project root ───────────────────────────────────────────────
ENV_FILE="$(dirname "$0")/../.env"
if [ -f "$ENV_FILE" ]; then
  export $(grep -v '^#' "$ENV_FILE" | grep -v '^$' | xargs)
fi

CONTAINER="flink-jobmanager"
SCRIPT="/opt/flink/jobs/submit_job.py"

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

# ── Check Docker is running ───────────────────────────────────────────────────
if ! docker info > /dev/null 2>&1; then
  log_error "Docker is not running. Start Docker Desktop first."
  exit 1
fi

# ── Check container is up ─────────────────────────────────────────────────────
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  log_error "Container '${CONTAINER}' is not running."
  log_warn  "Start it with:  docker compose up -d"
  exit 1
fi

# ── Check script exists inside container ─────────────────────────────────────
if ! docker exec "${CONTAINER}" test -f "${SCRIPT}"; then
  log_error "Script not found inside container: ${SCRIPT}"
  exit 1
fi

# ── Optional env overrides via args ──────────────────────────────────────────
#   Usage:  ./submit_jobs.sh [POLL_INTERVAL=5] [MAX_RETRIES=3]
ENV_ARGS=""
for arg in "$@"; do
  case "$arg" in
    POLL_INTERVAL=*|MAX_RETRIES=*|RETRY_DELAY=*|FLINK_REST_URL=*|JOB_SCRIPTS=*)
      ENV_ARGS="${ENV_ARGS} -e ${arg}"
      log_info "Override: ${arg}"
      ;;
    *)
      log_warn "Unknown argument ignored: ${arg}"
      ;;
  esac
done

# ── Submit ────────────────────────────────────────────────────────────────────
log_info "Submitting Flink jobs via container '${CONTAINER}'..."
log_info "Script: ${SCRIPT}"
echo "──────────────────────────────────────────────"

docker exec ${ENV_ARGS} \
  -e JOB_SCRIPTS="${JOB_SCRIPTS:-/opt/flink/jobs/healthcare_job.py}" \
  -e FLINK_REST_URL="${FLINK_REST_URL:-http://localhost:8081}" \
  -e MAX_RETRIES="${MAX_RETRIES:-5}" \
  -e RETRY_DELAY="${RETRY_DELAY:-30}" \
  -e POLL_INTERVAL="${POLL_INTERVAL:-10}" \
  "${CONTAINER}" python "${SCRIPT}"

EXIT_CODE=$?
echo "──────────────────────────────────────────────"

if [ ${EXIT_CODE} -eq 0 ]; then
  log_info "All jobs submitted and completed successfully."
else
  log_error "One or more jobs failed. Check logs above."
fi

exit ${EXIT_CODE}
