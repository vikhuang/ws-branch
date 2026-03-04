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

## Known Issues

- `tests/test_services.py` has stale fixtures referencing removed fields (`win_count`, `loss_count`, etc.)
- `pnl_analytics/` is a backward-compatible shim — all imports redirect to `broker_analytics`
