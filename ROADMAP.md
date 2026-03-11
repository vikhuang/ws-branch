# ws-branch

```
████ ████ ████ ████ ████ ████ ████ ████ ████ ░░░░ ░░░░
管線 分析 大單 加速 整合 預設 時區 增量 增量 共現 集中
                    ws核 合併      ETL  PNL

───────────────────────────────────────

ws-branch
├── data-pipeline ✓
│   ├── etl.py — broker_tx → daily_summary
│   ├── pnl_engine.py — FIFO PNL（prices via ws-core）
│   └── generate_merge_map.py — 券商合併對照表
│
├── broker_analytics ✓
│   ├── domain/ — 純邏輯，零 I/O
│   ├── infrastructure/ — I/O + 外部依賴
│   ├── application/services/ — 業務流程
│   └── interfaces/cli.py — 10 subcommands
│
└── (future)
    ├── strategy-0 — 假說不成立 ✓
    ├── hypothesis-scan — ~8x 加速 ✓
    ├── ws-core 整合 ✓
    │   sync_prices.py 刪除，改用 ws_core.prices()
    ├── pipeline-daily ✓
    │   ├── merged 設為預設 ✓
    │   ├── timezone fix ✓
    │   ├── ETL 增量 ✓
    │   │   etl.py --incr：只處理新日期的 broker_tx
    │   └── PNL 增量 ✓
    │       pnl_engine.py --incr：從 fifo_state 恢復，只算新日期
    ├── cluster-discovery ← HERE
    │   ├── 券商共現分析 ○
    │   │   daily_summary/*.parquet → co_occurrence.parquet
    │   ├── 產業知識驗證 ○
    │   │   blocked: 券商共現分析
    │   │   co_occurrence.parquet → clusters.json
    │   └── cross_stock 整合 ○
    │       blocked: 產業知識驗證
    │       clusters.json → cross_stock filter 自動讀取
    ├── concentration-timeseries ○
    │   目前用 snapshot HHI，需時序版偵測「突然集中」
    │
    └── parking
        └── (ideas go here)

████ 完成  ▓▓▓▓ 進行中  ░░░░ 未開始
🟢 正在編輯  🟡 未合併
```

## 符號

- `✓` 完成
- `◐` 進行中
- `○` 未開始
- `← HERE` 當前焦點
- `🟢 branch-name` 正在編輯的 worktree
- `🟡 branch-name` branch 存在但未合併
- `blocked: task` 依賴關係
- `input → output` I/O 契約
- `~~text~~（原因）` 已放棄
