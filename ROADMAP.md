# ws-branch

```
████ ████ ████ ████ ████ ████ ████ ████ ████ ████ ████ ████ ████ ░░░░ ░░░░ ░░░░ ░░░░
管線 分析 大單 加速 整合 預設 時區 增量 增量 掃描 探索 滾動 偏差 信號 回測 共現 集中
                    ws核 合併      ETL  PNL       CV  修正 強度 品質

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
│   3 rounds → bias fix → 6/10 CV passed (3 independent + exodus 邊緣)
│   ├── ✓ conviction — CV 4/5, sig=13-20%, dir=63-81%（最強）
│   ├── ✓ contrarian_smart — CV 5/5, sig=5.6-9.6%, dir=60-88%（最穩健、最廣覆蓋）
│   ├── ✓ concentration — CV 4/5, sig=7-51%, dir=50-100%（覆蓋少品質極高）
│   ├── ✓ contrarian_broker — CV 5/5, sig=10-15%, dir=70-89%（*共用 conviction filter）
│   ├── ✓ dual_window — CV 4/5, sig=15-23%, dir=51-86%（*共用 conviction filter）
│   ├── ✓ exodus — CV 3/5, sig=20-33%, dir=54-75%（v3 price-context direction）
│   ├── ✗ herding — CV 1/5（bias 修正後崩掉，dir% ~56-60%）
│   ├── ✗ large_trade_scar — 假說不成立（regression to mean）
│   ├── ✗ ta_regime — 事件太稀疏 + 計算太慢
│   └── ⏭ cross_stock — 需 cluster 定義
│   詳見 docs/harshreview.md
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
├── harshreview-fixes ← HERE
│   docs/harshreview.md — 9 個系統性問題的修復
│   ├── bias-fix ✓
│   │   ├── !1+!4 selector look-ahead ✓ — selectors 改用 rolling ranking + train_end_date
│   │   ├── !10 helper unrealized baseline ✓ — _rolling_top_k, _rolling_ranking_to_date 修正
│   │   ├── CV rerun ✓ — 6/7 存活，herding 1/5 失效
│   │   ├── !2 export 暖身期過濾 ✓ — _WARMUP_CUTOFF = 2023-01-01
│   │   └── !3 export significance windowing ✓ — inject test_start_date 排除暖身期
│   ├── backtest-quality ✓
│   │   ├── !8 beta 分離 ✓ — domain/beta_analysis.py + analyze CLI
│   │   ├── !7 重疊持倉去重 ✓ — domain/event_dedup.py + --hold-days flag
│   │   ├── 去重+beta 回測 ✓ — 真 alpha: conviction, concentration, contrarian_broker
│   │   └── !5 signal_value ◐ — 改回 1.0，count 作為 metadata，待 quintile 驗證
│   └── cross-project ○
│       blocked: bias-fix
│       ├── !6 Signal Contract v2 metadata ○
│       └── !9 策略相關性標註 ○
│
├── signal-strength-analysis ← HERE
│   驗證「信號強度 → 更好 return？」假設
│   ├── quintile framework ✓
│   │   ├── filters: signal_value=1.0 + signal_count metadata ✓
│   │   ├── domain/signal_strength.py: quintile analysis 純函數 ✓
│   │   ├── hypothesis_runner.run_strength_analysis() ✓
│   │   └── n_conviction quintile: ρ=0.056 太弱，維持 uniform ✓
│   │   ⚠ 方法論待修：stock effect 未控制、用 raw return 非 excess
│   │
│   ├── churn ratio ✓ — 反向！高 churn = 逆勢加碼 = return 更好
│   │   domain/churn.py — daily + rolling churn 純函數
│   │   conviction: count ρ=+0.056 vs churn ρ=-0.022 → count wins（但都太弱）
│   │   concentration: count ρ=+0.176 vs churn ρ=-0.052 → count wins
│   │   結論：signal_value = 1.0（uniform），不加權
│   │
│   └── 方法論修正 ○ — 下一階段必做
│       ├── quintile 基礎修正 ○
│       │   ├── winsorize churn_ratio（或 log 轉換，排除 outlier）
│       │   ├── per-stock z-score 正規化（控制 stock volatility effect）
│       │   └── 用 excess return（扣大盤）取代 raw return
│       ├── churn 獨立性驗證 ○
│       │   ├── partial correlation：控制 n_conviction 後 churn 殘差
│       │   └── conviction broker vs 全體 broker 的 churn（真正 vs-market）
│       └── signal_strength.py 修正 ○
│           ├── analyze_strength 加 winsorize 參數
│           └── invert 顯示邏輯改清楚（目前 group label 易誤讀）
│
└── (future)
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
