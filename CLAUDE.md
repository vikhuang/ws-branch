# ws-branch

## Build & Run

```bash
uv run python etl.py                        # Step 1: ETL full rebuild
uv run python etl.py --incr                 # Step 1: ETL incremental (new dates only)
uv run python pnl_engine.py                  # Step 2: FIFO PNL full (merged, prices via ws-core)
uv run python pnl_engine.py --incr           # Step 2: FIFO PNL incremental (from checkpoint)
uv run python pnl_engine.py --no-merge       # Step 2b: non-merged variant (archived)
uv run python -m broker_analytics <subcommand>  # Analytics CLI
uv run pytest                                # Tests
```

IMPORTANT: Always use `uv run python`, never bare `python`.

## CLI Commands

```bash
uv run python -m broker_analytics ranking          # Broker ranking
uv run python -m broker_analytics query 1440       # Single broker
uv run python -m broker_analytics symbol 2330      # Smart money flow
uv run python -m broker_analytics rolling          # Rolling PNL ranking
uv run python -m broker_analytics event-study 6285 # Event study
uv run python -m broker_analytics signal 2330      # Signal analysis
uv run python -m broker_analytics scan             # Market-wide screening
uv run python -m broker_analytics export           # Export signals CSV
uv run python -m broker_analytics verify           # Data integrity
uv run python -m broker_analytics hypothesis --list                    # List 10 strategies
uv run python -m broker_analytics hypothesis 2330 -s contrarian_broker # Single hypothesis
uv run python -m broker_analytics hypothesis 2330 --all                # All 10 strategies
uv run python -m broker_analytics hypothesis --batch 2330,2454 -s exodus --workers 4
uv run python -m broker_analytics hypothesis --scan -s conviction        # Full market scan + FDR
uv run python -m broker_analytics hypothesis --scan --cv -s conviction   # 5-fold rolling CV (recommended)
uv run python -m broker_analytics hypothesis --scan --cv -s conviction --min-folds 4  # Stricter CV
uv run python -m broker_analytics hypothesis --export -s conviction                  # Export Signal Contract CSV
uv run python -m broker_analytics hypothesis --export -s "conviction,herding,exodus"  # Export multiple strategies
```

## Architecture

Clean Architecture — violations break separation of concerns:

| Layer | Path | Rule |
|-------|------|------|
| domain | `broker_analytics/domain/` | Pure functions only. NO I/O, NO side effects, NO imports from other layers |
| infrastructure | `broker_analytics/infrastructure/` | I/O and external deps (config, repositories) |
| application | `broker_analytics/application/` | Use cases combining domain + infrastructure |
| interfaces | `broker_analytics/interfaces/` | CLI entry points only |

Top-level scripts (`etl.py`, `pnl_engine.py`) are the data pipeline. Both support `--incr` for incremental updates.
Prices come from ws-core (reads `~/r20/data/tej/prices.parquet`). Merged mode is the default (use `--no-merge` for raw).
`signal_report.py`, `market_scan.py`, `export_signals.py` are thin wrappers around `broker_analytics.application.services`.
**⚠️ signal_report/market_scan/export_signals are ARCHIVED** — T+1 intraday alpha invalidated after timezone fix (see `docs/information_fragmentation_alpha.md`).

### Domain Modules (shared, no duplication)

| Module | Provides |
|--------|----------|
| `domain/fifo.py` | `Lot`, `FIFOAccount`, `BrokerResult` |
| `domain/timing_alpha.py` | `compute_timing_alpha()` — normalized by `std(net_buy)` |
| `domain/large_trade.py` | `flag_large_trades()` — vectorized polars, per-broker 2σ |
| `domain/statistics.py` | Welch t-test, Cohen's d, permutation test, BH-FDR |
| `domain/backtest.py` | `run_backtest()` → `BacktestResult` (open→close, configurable cost) |
| `domain/event_detection.py` | Rolling PNL ranking + large trade detection |
| `domain/forward_returns.py` | Forward return computation for event study |
| `domain/hypothesis/` | Composable 5-step hypothesis pipeline (10 strategies) |

### Infrastructure

| Module | Provides |
|--------|----------|
| `infrastructure/config.py` | `DataPaths`, `AnalysisConfig` |
| `infrastructure/repositories/` | `TradeRepository`, `RankingRepository`, `BrokerRepository`, `PriceRepository` |

## Code Style

- **Polars only** — NEVER use pandas
- **No scipy** — statistics hand-implemented with `math.erfc` (see @docs/statistics.md)
- **Frozen dataclass + `__slots__`** for all result/value types: `@dataclass(frozen=True, slots=True)`
- **Chinese UI, English code** — display strings in Chinese (做多/做空/億), all identifiers and docstrings in English
- **Top-level functions for multiprocessing** — `ProcessPoolExecutor` workers must be module-level functions (pickle constraint)
- **Workers write files directly** — in-worker parquet writes to avoid IPC overhead

## Key Analytics Concepts

### Rolling PNL Ranking vs Smart Money Signal — 容易混淆，務必區分

| | Rolling PNL Ranking (`rolling`) | Smart Money Signal (`symbol`) |
|---|---|---|
| **問什麼** | 這個券商最近 N 年全市場賺多少？ | 這支股票上賺過錢的人現在在買還是賣？ |
| **資料來源** | `pnl_daily/{symbol}.parquet`（全市場聚合） | `daily_summary/{symbol}.parquet`（淨買超） + `pnl/{symbol}.parquet`（個股排名） |
| **範圍** | 跨所有股票 | 單一股票 |
| **輸出** | 券商全市場排名 | 買方/賣方力道分數 |

### `rolling --years N` vs `symbol --years N`

兩者都對 `pnl_daily` 做滾動窗口排名，邏輯相同，差別在範圍：
- `rolling --years 3`：聚合**全市場**所有股票的 pnl_daily → 全局券商排名
- `symbol 2330 --years 3`：只用 **2330** 的 pnl_daily → 該股票的券商排名（用於 smart money 計算）

### `symbol --detail N` ≠ N-day rolling PNL

- `--detail 5`：顯示近 5 日各券商的**淨買超明細**（來自 `daily_summary`，是交易量資料）
- 5-day rolling PNL：5 個交易日窗口的**損益排名**（來自 `pnl_daily`，是績效資料）
- 兩者資料來源不同、意義不同、完全獨立

## Critical Guardrails

These mistakes have been made before. Do NOT repeat them:

1. **Broker merge MUST precede FIFO** — merging after FIFO gives wrong cost basis and realized PNL
2. **Smart Money Signal uses per-stock PNL ranking** (`data/pnl/{symbol}.parquet`), NOT global `broker_ranking.parquet` — global ranking biases toward large-cap activity
3. **Timing alpha MUST normalize by `std(net_buy)`** — without normalization, high-volume brokers are automatically overrated
4. **`symbol --detail N` 是淨買超明細，不是 N-day rolling PNL** — 資料來源和意義完全不同
5. **Rolling window PNL = realized.sum() + (unrealized[end] − unrealized[start])** — `unrealized_pnl` 是存量快照，不是流量；少減 baseline 會把累計持倉損益灌入窗口排名
6. **Selector 必須用 rolling PNL ranking（pnl_daily_df + train_end_date）** — 不得用全期間 pnl_df 選 broker，否則引入 look-ahead bias（harshreview !1+!4）
7. **Bug 修復必須傳播到所有同 pattern 的程式碼路徑** — rolling_ranking.py 修了 unrealized baseline 但 selectors 的 helpers 沒修（harshreview !10）
8. **FIFO 的 net_shares < 0 不等於主動做空** — 台股分點資料中 broker 帳面空頭多為出貨效果。short conviction 2023+ return = -55 bps@10d（反向），已驗證失敗

## Data Conventions

- FIFO accumulates from 2021-01, performance measured from 2023-01
- `DataPaths(variant="merged")` → output paths get `_merged` suffix, input paths shared
- Significance requires BOTH `p < 0.05` AND `|Cohen's d| >= 0.2`
- Statistical method details: @docs/statistics.md
- `--export` outputs Signal Contract v1 CSV to `data/signals/{strategy}.csv` (for ws-quant `backtest-external-daily`)

## Git Conventions

[Conventional Commits](https://www.conventionalcommits.org/):

```
<type>: <concise description>
```

Types: `feat`, `fix`, `refactor`, `docs`, `test`, `perf`, `chore`

- Subject line under 72 characters, imperative mood
- Body optional, explain "why" not "what"

## Hypothesis Strategy Design

11 strategies follow a 5-step pipeline: Selector → Filter → Outcome → Baseline → StatTest.
Validated via 5-fold rolling CV (≥3/5 folds pass: sig>5%, FDR≥10, dir>60%).
**Stocks-only**: ETF/warrant/REIT excluded via tickers_tw（2,305 上市櫃股票，排除 564 非股票）。

| # | Strategy | Selector | Filter | CV | Core Logic |
|---|----------|----------|--------|----|------------|
| 10 | **momentum_conviction** | ranking momentum | 浮盈>20% 且加碼 | **4/5** | 排名躍升 broker 的 conviction（excess Sharpe 5.12@10d） |
| 3 | **conviction** | rolling top_k | 浮盈>20% 且加碼 | **4/5** | 對抗 disposition effect（excess Sharpe 3.31@10d） |
| 8 | **concentration** | HHI>8% | 集中券商加倉日 | **4/5** | 高信心（excess Sharpe 3.22@10d） |
| 1 | **contrarian_broker** | contrast score | conviction signals | **5/5** | 局部強+全局弱（excess Sharpe 3.36@10d，*共用 conviction filter） |
| 2 | **dual_window** | 1yr ∩ 3yr rolling top-K | conviction signals | **4/5** | 持續贏家加碼（excess Sharpe 2.51@10d，*共用 conviction filter） |
| 7 | ~~contrarian_smart~~ | rolling top_k | 恐慌日逆勢買 | 5/5 | CV 通過但去重後 excess Sharpe -0.20（alpha 消失） |
| 4 | ~~exodus~~ | rolling top_k | price-context 撤退 | 3/5 | 波動率信號，非方向性（excess -0.15） |
| 9 | ~~herding~~ | ~~rolling top+bottom~~ | ~~滾動群聯分歧百分位~~ | ❌ 1/5 | selector bias 修正後崩掉 |
| 0 | ~~large_trade_scar~~ | training SCAR top-K | test window 2σ | ❌ | 假說不成立 |
| 6 | ~~ta_regime~~ | TA z-score | large_trades | ❌ | 事件太稀疏 |
| 5 | cross_stock | top_k | cluster 內≥2股同時大單 | ⏭ | 需 cluster 定義 |

**獨立 alpha 來源**（deduped + beta-separated 10d excess Sharpe）：
- **momentum_conviction**（5.12）：排名躍升 broker × conviction filter。Broker Jaccard 0.03 vs conviction（幾乎不重疊）。⚠ 樣本小（260 trades deduped）
- **conviction**（3.31）：歷史績優 broker × conviction filter
- **concentration**（3.22）：HHI 集中 broker × 加倉
- contrarian_broker（3.36）有 alpha 但共用 conviction filter
詳見 `docs/harshreview.md` 和 `data/reports/round2_report.md`。

### Beta + 去重分析結果（2026-03-16, post-bias-fix）

10d Excess Sharpe（扣大盤 + 去重疊持倉）是判斷真 alpha 的最終指標。

| Strategy | 10d Total | 10d Excess (base) | 10d Excess (dedup) | Alpha? |
|----------|----------|-------------------|-------------------|--------|
| momentum_conviction | 6.36 | — | **5.12** | ✅ 最強（⚠ 樣本 260） |
| conviction | 4.90 | 3.70 | **3.31** | ✅ 真 alpha |
| concentration | 5.45 | 3.94 | **3.22** | ✅ 真 alpha |
| contrarian_broker | 5.54 | 4.37 | **3.36** | ✅（*共用 conviction filter） |
| dual_window | 4.68 | 3.11 | **2.51** | ✅（*共用 conviction filter） |
| contrarian_smart | 2.43 | 0.64 | **-0.20** | ❌ 去重後 alpha 消失 |
| exodus | -1.03 | -0.53 | **-0.15** | ❌ 本來就沒有 |

- 1d-5d **全部策略 excess Sharpe 為負** — 短期交易成本吃掉 alpha
- **真正有 alpha 的獨立信號**：momentum_conviction + conviction + concentration（3 個獨立）
- momentum_conviction 的 broker 與 conviction 幾乎不重疊（Jaccard 0.03），但 74% events 重疊
- contrarian_smart 的 CV 5/5 通過但 alpha 為零 — 統計顯著 ≠ 可交易
- `analyze` CLI：`uv run python -m broker_analytics analyze [-s strategy] [--tag deduped]`

### Signal Strength Validation（完成，4 個候選全部否決）

**原則**：signal_value 不得在未驗證的情況下用於 position sizing。
方法論：excess return（扣 IX0001）+ per-stock z-score + winsorize 1%/99% + partial Spearman。
詳見 `docs/strength_analysis_checklist.md`。

**方法論演進**：
- v1（raw return）產生虛假的「churn 反向」結論 → v2 修正後推翻
- 教訓：永遠用 excess return 做因子分析；比例型指標用 log 轉換

**Quintile 結果（v2 methodology, conviction 10d, excess + z-scored）**：

| 維度 | 指標 | ρ (10d) | partial ρ | pattern | 結論 |
|------|------|---------|-----------|---------|------|
| 空間 | signal_count（幾家 broker）| +0.062 | — | 弱正向 | ❌ 太弱 |
| 方向 | log(churn)（top-K 一致性）| +0.011 | +0.008 | ≈ 零 | ❌ 無獨立資訊 |
| 時間 | persistence（連續天數）| +0.027 | -0.003 | 反向（頻繁=差）| ❌ rarity=value |
| 金額 | log(amount)（買入總金額）| +0.047 | +0.027 | 倒 U 型 | ❌ 非單調 |

**結論：signal_value = 1.0（uniform）。** CLI: `hypothesis --strength -s conviction`

### 為什麼 Signal Magnitude 對 Conviction 無效

conviction 的 alpha 是 **binary 的**（事件發生 vs 不發生），不是 continuous 的（多強）。
四個維度的驗證揭示同一個規律：**rarity = information value**。

1. **Alpha 來自行為本身**：浮盈加碼 = disposition effect 的反面。3 家 broker 這樣做就足以排除噪音。第 4-15 家增加的是「共識」不是「資訊」，共識 = 擁擠 = 邊際 alpha 遞減。
2. **「太多」永遠不好**：更多 broker（count）、更一致（churn）、更頻繁（persistence）、更大金額（amount 的右半段）→ 全部指向更差的 return。資訊一旦擴散就不再有價值。
3. **金額呈倒 U 型**：太小 = 噪音（交易量不足）、中等 = genuine conviction、太大 = crowding（大資金湧入 = 已是共識）。
4. **Selector 做了大部分工作**：策略的預測力來自「誰被選中」（rolling PNL top-K），不是「多少人通過 filter」。contrarian_broker 有最高 excess Sharpe（3.36），因為它的 selector 更精準地篩選資訊不對稱。

**含義**：改善 selector（更精準找到有資訊優勢的 broker）的回報遠大於優化 signal_value。
下一步方向：Selector Momentum（ranking acceleration, 全新維度）。

### Cluster Discovery（未實作，方向 D）

`cross_stock` 策略需要產業鏈 cluster 定義。計劃分三步：
1. **券商共現分析** — 計算每對股票的 top-K 券商重疊率 + 大單日期時間共現率
2. **產業知識驗證** — 人工確認候選關係（供應鏈/上下游/集團）
3. **clusters.json** — 確認後寫入 `data/clusters.json`

目前 cluster 需透過 CLI 手動指定：`--params cluster=2330,3711,6770`

## Upstream（資料來源）

本專案的資料由 ws-platform 上游管理：

- **ws-core** (`~/r20/wp/ws-core/`) — 共用資料讀取 API，見 `README.md`
- **ws-admin** (`~/r20/wp/ws-admin/`) — 資料管理 CLI，`catalog.yaml` 是資料合約
- 共用資料目錄：`~/r20/data/`
- 共用憑證：`~/r20/wp/.ws-env`

使用的資料集（定義於 `ws-admin/catalog.yaml`）：

| Dataset | 路徑 | 用途 |
|---------|------|------|
| `tej_prices` | `~/r20/data/tej/prices.parquet` | 收盤價（OHLCV） |
| `fugle_broker_tx` | `~/r20/data/fugle/broker_tx/` | 券商分點交易（ws-admin 每日自動拉取） |

## Known Issues

- `pnl_analytics/` is a backward-compatible shim — all imports redirect to `broker_analytics`
- `cross_stock` strategy requires `--params cluster=2330,3711,...` to specify industry chain cluster
- `concentration` strategy uses snapshot-based HHI (cannot detect "suddenly concentrated"); time-series version deferred
