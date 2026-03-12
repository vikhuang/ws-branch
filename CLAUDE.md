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

## Data Conventions

- FIFO accumulates from 2021-01, performance measured from 2023-01
- `DataPaths(variant="merged")` → output paths get `_merged` suffix, input paths shared
- Significance requires BOTH `p < 0.05` AND `|Cohen's d| >= 0.2`
- Statistical method details: @docs/statistics.md

## Git Conventions

[Conventional Commits](https://www.conventionalcommits.org/):

```
<type>: <concise description>
```

Types: `feat`, `fix`, `refactor`, `docs`, `test`, `perf`, `chore`

- Subject line under 72 characters, imperative mood
- Body optional, explain "why" not "what"

## Hypothesis Strategy Design

10 strategies follow a 5-step pipeline: Selector → Filter → Outcome → Baseline → StatTest.
Validated via 5-fold rolling CV (≥3/5 folds pass: sig>5%, FDR≥10, dir>60%).

| # | Strategy | Selector | Filter | CV | Core Logic |
|---|----------|----------|--------|----|------------|
| 3 | **conviction** | top_k | 浮盈>20% 且加碼 | **5/5** | 對抗 disposition effect（最強，sig=14-21%） |
| 7 | **contrarian_smart** | top_k | 恐慌日逆勢買 | **5/5** | 恐慌承接 = 高成本決策（最廣覆蓋 2000+ 股） |
| 9 | **herding** | top_k | 滾動群聯分歧百分位 | **5/5** | 持續散戶群聚 + 聰明錢缺席（v3: rolling mean） |
| 8 | **concentration** | HHI>8% | 集中券商加倉日 | **5/5** | 高信心（dir 97%，覆蓋少但品質極高） |
| 1 | **contrarian_broker** | contrast score | conviction signals | **5/5** | 局部強+全局弱 = stock-specific 資訊優勢 |
| 2 | **dual_window** | 1yr ∩ 3yr top-K | conviction signals | **5/5** | 持續贏家加碼浮盈 |
| 4 | **exodus** | top_k | price-context 撤退 | **3/5** | 漲後撤退→空，跌後撤退→多（v3: 方向分類） |
| 0 | ~~large_trade_scar~~ | training SCAR top-K | test window 2σ | ❌ | 假說不成立（regression to mean，d≈0） |
| 6 | ~~ta_regime~~ | TA z-score | large_trades | ❌ | 事件太稀疏 + 計算太慢（107min/CV） |
| 5 | cross_stock | top_k | cluster 內≥2股同時大單 | ⏭ | 需 cluster 定義（blocked） |

**注意**：contrarian_broker、dual_window 與 conviction 共用 conviction filter，信號高度相關。
獨立信號源：conviction、contrarian_smart、herding、concentration（4 個獨立）。
詳見 `docs/hypothesis_exploration_guide.md` 和 `data/reports/round2_report.md`。

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
