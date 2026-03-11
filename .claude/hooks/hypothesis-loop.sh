#!/bin/bash
# Stop hook: 策略探索自動循環
#
# 讀取 data/reports/hypothesis_progress.json，找到下一個未完成策略，
# 用 exit 2 餵回指令讓 Claude 繼續。
#
# Claude 完成一個策略迭代後應更新此檔。
# 啟用：建立 data/reports/hypothesis_progress.json
# 停用：刪除進度檔

PROGRESS_FILE="data/reports/hypothesis_progress.json"

# Guard: 沒有進度檔 = 沒在跑探索循環
if [ ! -f "$PROGRESS_FILE" ]; then
  exit 0
fi

# 用 python 解析進度檔並決定下一步
python3 -c "
import json, sys

with open('$PROGRESS_FILE') as f:
    data = json.load(f)

strategies = data.get('strategies', {})
order = data.get('order', [])
max_iter = data.get('max_iterations', 50)

# 找當前或下一個 exploring 策略
current = None
for name in order:
    s = strategies.get(name, {})
    status = s.get('status', 'pending')
    if status in ('pending', 'exploring'):
        current = name
        break

if current is None:
    # 全部完成
    print('所有 10 個策略探索完成。請生成最終比較報告到 data/reports/hypothesis_final.md，然後更新 ROADMAP.md。')
    sys.exit(0)

s = strategies[current]
status = s.get('status', 'pending')
iters = s.get('iterations', 0)
best = s.get('best_result', '—')

print(f'【策略探索循環】{current}（第 {iters}/{max_iter} 次迭代）')
print(f'  最佳結果：{best}')
print()

if status == 'pending':
    print(f'開始探索策略 {current}：')
    print(f'1. 先閱讀 registry.py 理解策略精神')
    print(f'2. 全市場掃描：uv run python -m broker_analytics hypothesis --scan -s {current}')
    print(f'3. 解讀結果（顯著率 vs 5%、FDR 股票數 ≥10、方向一致性 >60%）')
    print(f'4. 記錄到 data/reports/{current}.md')
    print(f'5. 更新 data/reports/hypothesis_progress.json')
else:
    print(f'繼續探索策略 {current}：')
    print(f'1. 閱讀上次的失敗原因（data/reports/{current}.md）')
    print(f'2. 深入思考：改 selector/filter 邏輯，不只是調參數')
    print(f'3. 修改代碼 → 全市場掃描 → 解讀結果')
    print(f'4. 更新 data/reports/{current}.md 和進度檔')

print()
print('通過標準：顯著率>5% AND FDR股票≥10 AND 方向一致性>60%')
print(f'剩餘迭代：{max_iter - iters} 次')
" 2>&1

EXIT_CODE=$?
if [ $EXIT_CODE -eq 0 ]; then
  exit 2  # 有工作要做，餵回指令
fi
exit 0
