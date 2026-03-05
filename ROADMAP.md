# ws-branch

```
ws-branch
├── data-pipeline
│   ├── etl.py — 原始分點資料 streaming 聚合 ✓
│   ├── sync_prices.py — BigQuery 收盤價同步 ✓
│   ├── pnl_engine.py — FIFO PNL 計算引擎 ✓
│   │   ├── Layer 1.5 每日明細 + FIFO checkpoint ✓
│   │   ├── Layer 3a 個股維度聚合 ✓
│   │   ├── Layer 3b 全市場券商排名 ✓
│   │   └── --merged 合併停用券商 ✓
│   └── generate_merge_map.py — 券商合併對照表 ✓
│
├── broker_analytics (Clean Architecture package)
│   ├── domain — 純邏輯，零 I/O
│   │   ├── timing_alpha.py — 正規化擇時能力 ✓
│   │   ├── large_trade.py — per-broker 2σ 大單偵測 ✓
│   │   ├── statistics.py — Welch t-test / BH-FDR / permutation ✓
│   │   ├── backtest.py — open-to-close 回測引擎 ✓
│   │   ├── fifo.py — FIFO 持倉追蹤 (Lot/FIFOAccount) ✓
│   │   ├── event_detection.py — 事件偵測 ✓
│   │   ├── forward_returns.py — 前瞻報酬計算 ✓
│   │   └── hypothesis/ — 可組合假說檢定框架 ✓ ← HERE
│   │       ├── types.py — 型別契約 (SymbolData/HypothesisConfig/Result)
│   │       ├── position.py — Plan A 持倉推導
│   │       ├── selectors.py — Step 1: 券商篩選 (8 函數)
│   │       ├── filters.py — Step 2: 事件過濾 (7 函數)
│   │       ├── outcomes.py — Step 3: 報酬衡量 (2 函數)
│   │       ├── baselines.py — Step 4: 基準報酬 (3 函數)
│   │       ├── stat_tests.py — Step 5: 統計檢定 (2 函數)
│   │       └── registry.py — 9 策略組合註冊
│   │
│   ├── infrastructure — I/O + 外部依賴
│   │   ├── bigquery.py — 統一 BigQuery client ✓
│   │   ├── config.py — DataPaths / AnalysisConfig ✓
│   │   └── repositories/ — trade / pnl / broker / price ✓
│   │
│   ├── application/services — 業務流程
│   │   ├── ranking.py — 全市場券商排名 ✓
│   │   ├── broker_analysis.py — 單一券商績效 ✓
│   │   ├── symbol_analysis.py — Smart Money Signal ✓
│   │   ├── rolling_ranking.py — 滾動窗口排名 ✓
│   │   ├── event_study.py — 事件研究 ✓
│   │   ├── signal_report.py — 個股大單信號分析 ✓
│   │   ├── market_scan.py — 全市場掃描 + BH-FDR ✓
│   │   ├── signal_export.py — 信號 CSV 匯出 ✓
│   │   └── hypothesis_runner.py — 假說檢定編排器 ✓
│   │
│   └── interfaces/cli.py — 10 subcommands ✓
│       ├── ranking / query / symbol / verify / rolling ✓
│       ├── event-study ✓
│       ├── signal / scan / export ✓
│       └── hypothesis ✓
│
├── (future)
│   ├── ○
│   ├── ○
│   └── ○
│
└── parking
    └── (ideas go here)
```

## 符號

- `✓` 完成
- `◐` 進行中
- `○` 未開始
- `← HERE` 當前位置
- `~~text~~` 已放棄（附原因）
