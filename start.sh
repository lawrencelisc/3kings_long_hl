#!/usr/bin/env bash
# =============================================================================
#  start.sh  –  Process guardian for algo_3kings_long_hl.py
# =============================================================================
#  Why this exists:
#    Hyperliquid has NO exchange-native SL.  The bot's trailing stop is 100%
#    software-side.  If the process crashes or hangs, open positions have no
#    protection.  This guardian:
#      1. Auto-restarts the bot after any crash (exit code or exception).
#      2. Watches the heartbeat file; if it goes stale > HANG_TIMEOUT seconds
#         the process is killed and restarted (handles infinite-loop hangs).
#      3. On restart, sync_positions_on_startup() re-attaches to live positions
#         so trailing-SL tracking resumes immediately.
#
#  Usage:
#    chmod +x start.sh
#    ./start.sh             # foreground (tmux / screen recommended)
#    nohup ./start.sh &     # detached background
#
#  Environment:
#    PYTHON      – python binary to use   (default: python3)
#    SCRIPT      – bot script path        (default: same dir as start.sh)
#    LOG_DIR     – log directory          (default: ./logs)
#    RESTART_DELAY – seconds between restart attempts (default: 5)
#    HANG_TIMEOUT  – seconds before stale heartbeat triggers kill (default: 120)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON="${PYTHON:-python3}"
SCRIPT="${SCRIPT:-${SCRIPT_DIR}/algo_3kings_long_hl.py}"
LOG_DIR="${LOG_DIR:-${SCRIPT_DIR}/logs}"
RESTART_DELAY="${RESTART_DELAY:-5}"
HANG_TIMEOUT="${HANG_TIMEOUT:-120}"
HEARTBEAT_FILE="${HEARTBEAT_FILE:-/tmp/algo_3kings_hl.heartbeat}"

mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/guardian_$(date +%Y%m%d).log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"; }

log "============================================================"
log "Guardian started  (PID $$)"
log "Script   : ${SCRIPT}"
log "Python   : ${PYTHON}"
log "Heartbeat: ${HEARTBEAT_FILE}  (hang_timeout=${HANG_TIMEOUT}s)"
log "============================================================"

# Remove stale heartbeat so the watchdog doesn't see an old timestamp
rm -f "${HEARTBEAT_FILE}"

while true; do
    log "▶ Starting bot..."
    "${PYTHON}" "${SCRIPT}" >> "${LOG_FILE}" 2>&1 &
    BOT_PID=$!
    log "  Bot PID=${BOT_PID}"

    # Monitor: check heartbeat freshness while bot is running
    while kill -0 "${BOT_PID}" 2>/dev/null; do
        sleep 10
        if [[ -f "${HEARTBEAT_FILE}" ]]; then
            MTIME=$(date -r "${HEARTBEAT_FILE}" +%s 2>/dev/null || echo 0)
            NOW=$(date +%s)
            AGE=$(( NOW - MTIME ))
            if (( AGE > HANG_TIMEOUT )); then
                log "⚠️  Heartbeat stale for ${AGE}s (>${HANG_TIMEOUT}s) — killing PID ${BOT_PID}"
                kill -9 "${BOT_PID}" 2>/dev/null || true
                break
            fi
        fi
    done

    wait "${BOT_PID}" 2>/dev/null || true
    EXIT_CODE=$?
    log "◼ Bot exited (code=${EXIT_CODE}). Restarting in ${RESTART_DELAY}s..."
    sleep "${RESTART_DELAY}"
done
