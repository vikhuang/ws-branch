# ws-branch

## Build & Run

```bash
uv run python etl.py broker_tx.parquet      # Step 1: ETL
uv run python sync_prices.py                 # Step 2: BigQuery prices
uv run python pnl_engine.py                  # Step 3: FIFO PNL
uv run python pnl_engine.py --merged         # Step 3b: merged variant
uv run python -m pnl_analytics <subcommand>  # Analytics CLI
uv run pytest                                # Tests
```

IMPORTANT: Always use `uv run python`, never bare `python`.

## Architecture

Clean Architecture — violations break separation of concerns:

| Layer | Path | Rule |
|-------|------|------|
| domain | `pnl_analytics/domain/` | Pure functions only. NO I/O, NO side effects, NO imports from other layers |
| infrastructure | `pnl_analytics/infrastructure/` | I/O and external deps (config, repositories) |
| application | `pnl_analytics/application/` | Use cases combining domain + infrastructure |
| interfaces | `pnl_analytics/interfaces/` | CLI entry points only |

Top-level scripts (`etl.py`, `pnl_engine.py`, etc.) are the data pipeline, separate from the `pnl_analytics` package.

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
