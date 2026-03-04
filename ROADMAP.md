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
│   │   └── forward_returns.py — 前瞻報酬計算 ✓
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
│   │   └── signal_export.py — 信號 CSV 匯出 ✓
│   │
│   └── interfaces/cli.py — 9 subcommands ✓
│       ├── ranking / query / symbol / verify / rolling ✓
│       ├── event-study ✓
│       └── signal / scan / export ✓
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
