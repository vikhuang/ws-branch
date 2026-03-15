# ws-branch

```
████ ████ ████ ████ ████ ████ ████ ████ ████ ████ ████ ████ ░░░░ ░░░░
管線 分析 大單 加速 整合 預設 時區 增量 增量 掃描 探索 滾動 共現 集中
                    ws核 合併      ETL  PNL       CV  修正

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
├── hypothesis-exploration ✓
│   3 rounds, 7/10 CV passed (4-5 independent signals)
│   ├── ✓ conviction — CV 5/5, sig=14-21%, dir=75-90%（最強，行為金融基礎）
│   ├── ✓ contrarian_smart — CV 5/5, sig=5.7-9.6%, dir=64-87%（最廣覆蓋）
│   ├── ✓ herding — CV 5/5, sig=5.8-10.2%, dir=60-68%（v3 rolling+percentile）
│   ├── ✓ concentration — CV 5/5, sig=6-46%, dir=61-100%（覆蓋少品質極高）
│   ├── ✓ contrarian_broker — CV 5/5, sig=10-16%, dir=71-86%（*共用 conviction filter）
│   ├── ✓ dual_window — CV 5/5, sig=11-26%, dir=64-87%（*共用 conviction filter）
│   ├── ✓ exodus — CV 3/5, sig=20-34%, dir=53-80%（v3 price-context direction）
│   ├── ✗ large_trade_scar — 假說不成立（regression to mean）
│   ├── ✗ ta_regime — 事件太稀疏 + 計算太慢
│   └── ⏭ cross_stock — 需 cluster 定義
│   詳見 docs/hypothesis_exploration_guide.md
│
├── hypothesis-scan ✓
│   ├── HypothesisConfig.requires — 策略宣告資料依賴，懶載入
│   ├── --scan 全市場模式 — 進度輸出 + BH-FDR 校正
│   └── --scan --cv 5-fold 滾動窗口交叉驗證
│
├── ws-core 整合 ✓
│   sync_prices.py 刪除，改用 ws_core.prices()
│
├── pipeline-daily ✓
│   ├── merged 設為預設 ✓
│   ├── timezone fix ✓
│   ├── ETL 增量 ✓
│   │   etl.py --incr：只處理新日期的 broker_tx
│   └── PNL 增量 ✓
│       pnl_engine.py --incr：從 fifo_state 恢復，只算新日期
│
├── rolling-window-fix ✓
│   unrealized_pnl 是快照非流量，窗口 PNL 須減 baseline
│   ├── rolling_ranking.py — realized.sum() + (unrealized[end] − unrealized[start])
│   └── tmp/gen_xlsx.py, gen_5d_xlsx.py — 同步修正
│
└── (future) ← HERE
    ├── cluster-discovery ○
    │   ├── 券商共現分析 ○
    │   │   daily_summary/*.parquet → co_occurrence.parquet
    │   ├── 產業知識驗證 ○
    │   │   blocked: 券商共現分析
    │   │   co_occurrence.parquet → clusters.json
    │   └── cross_stock 整合 ○
    │       blocked: 產業知識驗證
    │       clusters.json → cross_stock filter 自動讀取
    ├── backtest-e2e ○
    │   交易成本、滑價、持倉限制的端到端回測
    │   用 domain/backtest.py 的 run_backtest()
    ├── signal-combination ○
    │   conviction + contrarian_smart 同時觸發時的組合效果
    │
    └── parking
        └── concentration-timeseries — snapshot HHI → 時序版偵測「突然集中」

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
