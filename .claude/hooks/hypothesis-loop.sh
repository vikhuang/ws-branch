#!/bin/bash
# Stop hook: 策略探索自動循環
#
# 讀取 data/reports/hypothesis_progress.json，找到下一個未完成策略，
# 用 exit 2 餵回指令讓 Claude 繼續。
#
# 進度檔格式：{"done": ["large_trade_scar", "contrarian_broker"], "current": null}
# Claude 完成一個策略後應更新此檔。
#
# 啟用方式：在 .claude/settings.json 加入此 hook
# 停用方式：刪除進度檔或把 hook 從 settings 移除

PROGRESS_FILE="data/reports/hypothesis_progress.json"

# Guard: 沒有進度檔 = 沒在跑探索循環，跳過
if [ ! -f "$PROGRESS_FILE" ]; then
  exit 0
fi

# 所有策略（順序 = 執行順序）
ALL_STRATEGIES=(
  "large_trade_scar"
  "contrarian_broker"
  "dual_window"
  "conviction"
  "exodus"
  "cross_stock"
  "ta_regime"
  "contrarian_smart"
  "concentration"
  "herding"
)

# 讀取已完成清單
DONE=$(python3 -c "
import json, sys
try:
    data = json.load(open('$PROGRESS_FILE'))
    for s in data.get('done', []):
        print(s)
except:
    pass
")

# 找下一個未完成策略
NEXT=""
for s in "${ALL_STRATEGIES[@]}"; do
  if ! echo "$DONE" | grep -qx "$s"; then
    NEXT="$s"
    break
  fi
done

# 全部完成
if [ -z "$NEXT" ]; then
  echo "所有 10 個策略已完成探索。請生成最終比較報告並更新 ROADMAP.md。"
  # 自動清除進度檔，結束循環
  rm -f "$PROGRESS_FILE"
  exit 2
fi

# 計算進度
DONE_COUNT=$(echo "$DONE" | grep -c . 2>/dev/null || echo 0)
TOTAL=${#ALL_STRATEGIES[@]}

echo "【策略探索循環 ${DONE_COUNT}/${TOTAL}】繼續下一個策略：${NEXT}"
echo ""
echo "請執行以下流程："
echo "1. 全市場掃描：uv run python -m broker_analytics hypothesis --scan -s ${NEXT}"
echo "2. 解讀結果（顯著率、FDR、effect size 方向）"
echo "3. 如有需要，調參數重跑（用 --params key=value）"
echo "4. 記錄結論到 data/reports/${NEXT}.md"
echo "5. 更新進度檔：把 '${NEXT}' 加入 done 清單"
echo "6. 更新 CLAUDE.md 策略表和 ROADMAP.md"
exit 2
