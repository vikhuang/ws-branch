#!/bin/bash
# ws-branch daily pipeline
# 排程：週一~六 22:30+ (pull-evening 之後)
# 前提：ws-admin pull 已完成
#
# Cron (local launchd):
#   ws-admin ready 通過後自動觸發，或手動 ./run.sh

set -uo pipefail

cd "$(dirname "$0")"

# ─── 環境 ────────────────────────────────────────────────────────

# 載入共用環境（Telegram token 等）
if [ -f "$HOME/r20/wp/.ws-env" ]; then
    set -a; source "$HOME/r20/wp/.ws-env"; set +a
fi
if [ -f "$HOME/r20/wp/ws-admin/.env" ]; then
    set -a; source "$HOME/r20/wp/ws-admin/.env"; set +a
fi

export PATH="$HOME/.local/bin:$PATH"
LOG_FILE="$HOME/r20/data/.logs/ws-branch-$(date +%Y%m%d-%H%M%S).log"
mkdir -p "$(dirname "$LOG_FILE")"

# ─── 通知 ────────────────────────────────────────────────────────

notify() {
    local message="$1"
    if [ -n "${TELEGRAM_BOT_TOKEN:-}" ] && [ -n "${TELEGRAM_ADMIN_ID:-}" ]; then
        curl -s -X POST \
            "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
            -d "chat_id=${TELEGRAM_ADMIN_ID}" \
            -d "text=${message}" \
            -d "parse_mode=HTML" > /dev/null 2>&1 || true
    fi
}

# ─── Preflight ───────────────────────────────────────────────────

echo "=== ws-branch pipeline $(date) ===" | tee "$LOG_FILE"

# 檢查資料是否就緒
if ! uv run --project "$HOME/r20/wp/ws-admin" ws-admin ready 2>&1 | tee -a "$LOG_FILE"; then
    echo "Data not ready, skipping." | tee -a "$LOG_FILE"
    exit 0
fi

# ─── Pipeline ────────────────────────────────────────────────────

FAILED=0

echo "" | tee -a "$LOG_FILE"
echo "[1/4] ETL..." | tee -a "$LOG_FILE"
if ! uv run python etl.py --incr >> "$LOG_FILE" 2>&1; then
    echo "ETL FAILED" | tee -a "$LOG_FILE"
    FAILED=1
fi

if [ "$FAILED" -eq 0 ]; then
    echo "[2/4] PNL Engine..." | tee -a "$LOG_FILE"
    if ! uv run python pnl_engine.py --incr >> "$LOG_FILE" 2>&1; then
        echo "PNL Engine FAILED" | tee -a "$LOG_FILE"
        FAILED=1
    fi
fi

if [ "$FAILED" -eq 0 ]; then
    echo "[3/4] Generate XLSX..." | tee -a "$LOG_FILE"
    if ! uv run python tmp/gen_xlsx.py >> "$LOG_FILE" 2>&1; then
        echo "gen_xlsx FAILED" | tee -a "$LOG_FILE"
        FAILED=1
    fi
fi

if [ "$FAILED" -eq 0 ]; then
    echo "[4/4] Export CSV..." | tee -a "$LOG_FILE"
    # Auto-detect latest broker_tx date
    LATEST_DATE=$(ls "$HOME/r20/data/fugle/broker_tx/" | tail -1 | grep -o '[0-9]\{8\}')
    if [ -n "$LATEST_DATE" ]; then
        uv run python tmp/export_broker_tx.py "$LATEST_DATE" >> "$LOG_FILE" 2>&1 || true
    fi
fi

# ─── 結果通知 ─────────────────────────────────────────────────────

echo "" | tee -a "$LOG_FILE"
if [ "$FAILED" -eq 0 ]; then
    echo "=== Done $(date) ===" | tee -a "$LOG_FILE"
    notify "✅ <b>ws-branch pipeline 完成</b>
$(TZ=Asia/Taipei date '+%Y-%m-%d %H:%M')
請檢查結果並交付客戶"
else
    echo "=== FAILED $(date) ===" | tee -a "$LOG_FILE"
    TAIL_LOG=$(tail -10 "$LOG_FILE" | head -c 500 | sed 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g')
    notify "🚨 <b>ws-branch pipeline 失敗</b>
$(TZ=Asia/Taipei date '+%Y-%m-%d %H:%M')

<pre>${TAIL_LOG}</pre>"
    exit 1
fi
