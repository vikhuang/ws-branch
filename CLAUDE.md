# ws-branch

## Build & Run

```bash
uv run python etl.py broker_tx.parquet      # Step 1: ETL
uv run python sync_prices.py                 # Step 2: BigQuery prices
uv run python pnl_engine.py                  # Step 3: FIFO PNL
uv run python pnl_engine.py --merged         # Step 3b: merged variant
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
```

## Architecture

Clean Architecture — violations break separation of concerns:

| Layer | Path | Rule |
|-------|------|------|
| domain | `broker_analytics/domain/` | Pure functions only. NO I/O, NO side effects, NO imports from other layers |
| infrastructure | `broker_analytics/infrastructure/` | I/O and external deps (config, repositories, BigQuery) |
| application | `broker_analytics/application/` | Use cases combining domain + infrastructure |
| interfaces | `broker_analytics/interfaces/` | CLI entry points only |

Top-level scripts (`etl.py`, `pnl_engine.py`, `sync_prices.py`) are the data pipeline.
`signal_report.py`, `market_scan.py`, `export_signals.py` are thin wrappers around `broker_analytics.application.services`.

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
| `infrastructure/bigquery.py` | Centralized BigQuery client (PROJECT_ID, fetch_ohlc, fetch_ohlc_batch) |
| `infrastructure/config.py` | `DataPaths`, `AnalysisConfig` |
| `infrastructure/repositories/` | `TradeRepository`, `RankingRepository`, `BrokerRepository`, `PriceRepository` |

## Code Style

- **Polars only** — NEVER use pandas
- **No scipy** — statistics hand-implemented with `math.erfc` (see @docs/statistics.md)
- **Frozen dataclass + `__slots__`** for all result/value types: `@dataclass(frozen=True, slots=True)`
- **Chinese UI, English code** — display strings in Chinese (做多/做空/億), all identifiers and docstrings in English
- **Top-level functions for multiprocessing** — `ProcessPoolExecutor` workers must be module-level functions (pickle constraint)
- **Workers write files directly** — in-worker parquet writes to avoid IPC overhead

## Critical Guardrails

These mistakes have been made before. Do NOT repeat them:

1. **Broker merge MUST precede FIFO** — merging after FIFO gives wrong cost basis and realized PNL
2. **Smart Money Signal uses per-stock PNL ranking** (`data/pnl/{symbol}.parquet`), NOT global `broker_ranking.parquet` — global ranking biases toward large-cap activity
3. **Timing alpha MUST normalize by `std(net_buy)`** — without normalization, high-volume brokers are automatically overrated

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

| # | Strategy | Selector | Filter | Core Logic |
|---|----------|----------|--------|------------|
| 0 | large_trade_scar | training SCAR top-K | test window 2σ+金額 | 訓練窗口大單準 → 測試窗口驗證 (**假說不成立**) |
| 1 | contrarian_broker | global bottom ∩ local top | large_trades 2σ | 全市場虧損但特定股票強 = stock-specific 資訊優勢 |
| 2 | dual_window | 1yr ∩ 3yr top-K | large_trades 2σ | 長期贏家重新進場 = regime change |
| 3 | conviction | top_k | 浮盈>20% 且加碼 | 對抗 disposition effect 的強信號 |
| 4 | exodus | top_k | 持倉歸零/減半（20天窗口） | 聰明錢集體撤退 = negative signal |
| 5 | cross_stock | top_k | cluster 內≥2股同時大單 | 同一券商跨產業鏈同步買進 |
| 6 | ta_regime | TA temporal z-score | large_trades 2σ | 券商擇時能力突然「開竅」 |
| 7 | contrarian_smart | top_k | 恐慌日逆勢買（單日>2%或3日>5%跌） | 恐慌時承接 = 高成本決策 |
| 8 | concentration | cross-stock HHI>30% | 集中券商加倉日 | 跨股票持倉高度集中 = 高信心 |
| 9 | herding | top_k | herding index（散戶vs聰明錢） | 散戶一致但聰明錢缺席 = 危險 |

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
