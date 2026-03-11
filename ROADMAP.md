# ws-branch

```
████ ████ ████ ████ ░░░░ ░░░░ ░░░░ ░░░░
管線 分析 大單 加速 共現 驗證 整合 集中

───────────────────────────────────────

ws-branch
├── data-pipeline ✓
│   ├── etl.py — 原始分點資料 streaming 聚合
│   ├── sync_prices.py — BigQuery 收盤價同步
│   ├── pnl_engine.py — FIFO PNL 計算引擎
│   └── generate_merge_map.py — 券商合併對照表
│
├── broker_analytics ✓
│   ├── domain/ — 純邏輯，零 I/O
│   ├── infrastructure/ — I/O + 外部依賴
│   ├── application/services/ — 業務流程
│   └── interfaces/cli.py — 10 subcommands
│
└── (future) ← HERE
    ├── strategy-0 — 假說不成立 ✓
    ├── hypothesis-scan — ~8x 加速 ✓
    ├── cluster-discovery
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
