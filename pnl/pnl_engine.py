"""PNL Engine: FIFO-based PNL calculation with timing alpha.

Processes daily_summary/{symbol}.parquet files and outputs:
- pnl_daily/{symbol}.parquet  - Daily PNL events per broker (Layer 1.5)
- fifo_state/{symbol}.parquet - FIFO checkpoint for incremental updates
- pnl/{symbol}.parquet        - Aggregated per-symbol broker ranking (Layer 2)
- derived/broker_ranking.parquet - Global broker ranking (Layer 2)

FIFO Logic:
- Each (symbol, broker) pair is an independent account
- Sells close long positions first, then open short
- Buys cover short positions first, then open long
- Tracks realized PNL (closed trades) and unrealized PNL (open positions)

Timing Alpha (normalized):
- Measures timing ability: Σ((net_buy[t-1] - avg) × return[t]) / std(net_buy)
- Normalized by trade volume std to remove volume bias
- Positive = buys before price rises, sells before price falls

Backtest Window:
- FIFO state accumulates from 2021-01-01
- Performance (PNL, timing alpha) only counts from 2023-01-01
- Daily PNL events stored for ALL dates (enables rolling window queries)
"""

import sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import polars as pl

from broker_analytics.domain.fifo import Lot, FIFOAccount, BrokerResult
from broker_analytics.domain.timing_alpha import compute_timing_alpha
from broker_analytics.infrastructure.config import DataPaths, AnalysisConfig, DEFAULT_PATHS, DEFAULT_CONFIG


def process_symbol(
    symbol: str,
    paths: DataPaths,
    sym_prices: dict[date, float],
    sym_returns: dict[date, float],
    backtest_start: date,
    write_daily: bool = True,
    merge_map: dict[str, str] | None = None,
) -> list[BrokerResult]:
    """Process a single symbol and return broker results.

    Also writes pnl_daily/{symbol}.parquet and fifo_state/{symbol}.parquet
    directly from the worker process (avoids large IPC overhead).

    Args:
        symbol: Stock symbol
        paths: Data paths configuration
        sym_prices: {date: close_price} for this symbol only
        sym_returns: {date: daily_return} for this symbol only
        backtest_start: Start date for performance calculation
        write_daily: If True, write pnl_daily and fifo_state files

    Returns:
        List of BrokerResult for each broker
    """
    trade_path = paths.symbol_trade_path(symbol)
    if not trade_path.exists():
        return []

    df = pl.read_parquet(trade_path)
    if len(df) == 0:
        return []

    # Remap broker codes for merged mode
    if merge_map:
        df = df.with_columns(
            pl.col("broker").replace(merge_map, default=pl.col("broker"))
        )
        # Old + new code may overlap on same dates → re-aggregate
        df = df.group_by(["date", "broker"]).agg([
            pl.col("buy_shares").sum(),
            pl.col("sell_shares").sum(),
            pl.col("buy_amount").sum(),
            pl.col("sell_amount").sum(),
        ])

    # Get all dates and brokers
    dates = sorted(df["date"].unique().to_list())
    brokers = df["broker"].unique().to_list()

    # Pre-build trade lookup: {(broker, date): row} - ONE pass through DataFrame
    trade_lookup: dict[tuple[str, date], dict] = {}
    for row in df.iter_rows(named=True):
        trade_lookup[(row["broker"], row["date"])] = row

    results = []
    daily_rows: list[dict] = []
    fifo_rows: list[dict] = []

    for broker in brokers:
        account = FIFOAccount()
        realized_after_start = 0.0
        total_buy = 0.0
        total_sell = 0.0
        last_unrealized = 0.0
        last_price = 0.0

        # Collect net_buy series for timing alpha
        net_buy_series: list[int] = []
        return_series: list[float] = []

        for d in dates:
            # Get close price
            price = sym_prices.get(d, last_price)
            if price > 0:
                last_price = price

            row = trade_lookup.get((broker, d))
            if row:
                buy_shares = row["buy_shares"] or 0
                sell_shares = row["sell_shares"] or 0
                buy_amount = row["buy_amount"] or 0.0
                sell_amount = row["sell_amount"] or 0.0

                realized, unrealized = account.process_day(
                    buy_shares, sell_shares,
                    buy_amount, sell_amount,
                    price, d,
                )

                # Only count performance after backtest_start
                if d >= backtest_start:
                    realized_after_start += realized
                    total_buy += buy_amount
                    total_sell += sell_amount

                    # Collect for timing alpha
                    net_buy_series.append(buy_shares - sell_shares)
                    return_series.append(sym_returns.get(d, 0.0))

                last_unrealized = unrealized

                # Collect daily PNL event (all dates, for rolling windows)
                if write_daily and (realized != 0.0 or unrealized != 0.0):
                    daily_rows.append({
                        "broker": broker, "date": d,
                        "realized_pnl": realized,
                        "unrealized_pnl": unrealized,
                    })
            else:
                # No trade, recalculate unrealized
                unrealized = 0.0
                for lot in account.long_lots:
                    unrealized += lot.shares * (price - lot.cost_per_share)
                for lot in account.short_lots:
                    unrealized += lot.shares * (lot.cost_per_share - price)
                last_unrealized = unrealized

                # Still collect for timing alpha (net_buy = 0 on no-trade days)
                if d >= backtest_start:
                    net_buy_series.append(0)
                    return_series.append(sym_returns.get(d, 0.0))

                # Collect daily PNL for position holders (realized=0, unrealized≠0)
                if write_daily and unrealized != 0.0:
                    daily_rows.append({
                        "broker": broker, "date": d,
                        "realized_pnl": 0.0,
                        "unrealized_pnl": unrealized,
                    })

        # Total PNL = realized (after start) + final unrealized
        total_pnl = realized_after_start + last_unrealized

        timing_alpha = compute_timing_alpha(net_buy_series, return_series)

        results.append(BrokerResult(
            broker=broker,
            total_pnl=total_pnl,
            realized_pnl=realized_after_start,
            unrealized_pnl=last_unrealized,
            total_buy=total_buy,
            total_sell=total_sell,
            timing_alpha=timing_alpha,
        ))

        # Collect FIFO lots for checkpoint
        if write_daily:
            for side, shares, cost, open_date in account.get_lots():
                fifo_rows.append({
                    "broker": broker, "side": side,
                    "shares": shares, "cost_per_share": cost,
                    "open_date": open_date,
                })

    # Write daily PNL events (Layer 1.5)
    if write_daily and daily_rows:
        daily_df = pl.DataFrame(daily_rows, schema={
            "broker": pl.Utf8,
            "date": pl.Date,
            "realized_pnl": pl.Float64,
            "unrealized_pnl": pl.Float64,
        }).sort(["broker", "date"])
        daily_df.write_parquet(paths.symbol_pnl_daily_path(symbol))

    # Write FIFO state checkpoint
    if write_daily and fifo_rows:
        fifo_df = pl.DataFrame(fifo_rows, schema={
            "broker": pl.Utf8,
            "side": pl.Utf8,
            "shares": pl.Int64,
            "cost_per_share": pl.Float64,
            "open_date": pl.Date,
        })
        fifo_df.write_parquet(paths.symbol_fifo_state_path(symbol))

    return results


# =============================================================================
# Parallel Processing & Aggregation
# =============================================================================

def load_price_lookup(paths: DataPaths) -> dict[tuple[str, date], float]:
    """Load price data into lookup dict via ws-core."""
    from ws_core import prices as ws_prices

    df = ws_prices(columns=["coid", "mdate", "close_d"], start="2021-01-01")
    df = df.filter(pl.col("close_d").is_not_null())
    lookup = {}
    for row in df.iter_rows(named=True):
        lookup[(row["coid"], row["mdate"])] = float(row["close_d"])
    return lookup


def calculate_returns(
    price_lookup: dict[tuple[str, date], float],
) -> dict[tuple[str, date], float]:
    """Calculate daily returns from price lookup.

    Returns:
        {(symbol, date): daily_return} where return = (price[t] - price[t-1]) / price[t-1]
    """
    # Group by symbol
    by_symbol: dict[str, list[tuple[date, float]]] = defaultdict(list)
    for (symbol, d), price in price_lookup.items():
        by_symbol[symbol].append((d, price))

    # Calculate returns
    returns_lookup = {}
    for symbol, prices in by_symbol.items():
        prices.sort(key=lambda x: x[0])  # Sort by date
        for i in range(1, len(prices)):
            prev_date, prev_price = prices[i - 1]
            curr_date, curr_price = prices[i]
            if prev_price > 0:
                ret = (curr_price - prev_price) / prev_price
                returns_lookup[(symbol, curr_date)] = ret

    return returns_lookup


def calculate_all_pnl(
    paths: DataPaths = DEFAULT_PATHS,
    config: AnalysisConfig = DEFAULT_CONFIG,
    workers: int | None = None,
    merge_map: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Calculate PNL for all symbols and generate broker ranking.

    Args:
        paths: Data paths configuration
        config: Analysis configuration
        workers: Number of parallel workers (default: config.parallel_workers)

    Returns:
        Broker ranking DataFrame
    """
    workers = workers or config.parallel_workers
    backtest_start = date.fromisoformat(config.backtest_start)

    # Ensure output directories exist
    paths.ensure_dirs()

    # Load prices and calculate returns
    print("Loading prices...")
    price_lookup = load_price_lookup(paths)
    print(f"  Loaded {len(price_lookup):,} price records")

    print("Calculating returns...")
    returns_lookup = calculate_returns(price_lookup)
    print(f"  Calculated {len(returns_lookup):,} daily returns")

    # Get symbols
    symbols = paths.list_symbols()
    if not symbols:
        print("Error: No symbols found in daily_summary/")
        return pl.DataFrame()

    # Pre-partition prices and returns by symbol
    # Each worker only receives ~1,200 entries instead of 3,400,000
    print("Pre-partitioning price data...")
    prices_by_sym: dict[str, dict[date, float]] = defaultdict(dict)
    for (sym, d), price in price_lookup.items():
        prices_by_sym[sym][d] = price

    returns_by_sym: dict[str, dict[date, float]] = defaultdict(dict)
    for (sym, d), ret in returns_lookup.items():
        returns_by_sym[sym][d] = ret

    print(f"Processing {len(symbols)} symbols with {workers} workers...")

    # Aggregate results by broker
    broker_totals: dict[str, dict] = defaultdict(lambda: {
        "total_pnl": 0.0,
        "realized_pnl": 0.0,
        "unrealized_pnl": 0.0,
        "total_buy": 0.0,
        "total_sell": 0.0,
        "timing_alpha": 0.0,
    })

    # Parallel processing with pre-partitioned data
    completed = 0
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                process_symbol,
                symbol, paths,
                prices_by_sym[symbol],
                returns_by_sym[symbol],
                backtest_start,
                True,  # write_daily
                merge_map,
            ): symbol
            for symbol in symbols
        }

        for future in as_completed(futures):
            symbol = futures[future]
            completed += 1
            if completed % 500 == 0 or completed == len(symbols):
                print(f"  {completed}/{len(symbols)} symbols done")

            symbol_results = future.result()

            # Save per-symbol PNL
            if symbol_results:
                sym_rows = []
                for r in symbol_results:
                    sym_rows.append({
                        "broker": r.broker,
                        "total_pnl": r.total_pnl,
                        "realized_pnl": r.realized_pnl,
                        "unrealized_pnl": r.unrealized_pnl,
                        "total_buy_amount": r.total_buy,
                        "total_sell_amount": r.total_sell,
                        "timing_alpha": r.timing_alpha,
                    })
                sym_df = pl.DataFrame(sym_rows)
                sym_df = sym_df.sort("total_pnl", descending=True)
                sym_df = sym_df.with_row_index("rank", offset=1)
                sym_df.write_parquet(paths.symbol_pnl_path(symbol))

            # Aggregate to broker totals
            for r in symbol_results:
                b = broker_totals[r.broker]
                b["total_pnl"] += r.total_pnl
                b["realized_pnl"] += r.realized_pnl
                b["unrealized_pnl"] += r.unrealized_pnl
                b["total_buy"] += r.total_buy
                b["total_sell"] += r.total_sell
                b["timing_alpha"] += r.timing_alpha

    # Build ranking DataFrame
    rows = []
    for broker, stats in broker_totals.items():
        rows.append({
            "broker": broker,
            "total_pnl": stats["total_pnl"],
            "realized_pnl": stats["realized_pnl"],
            "unrealized_pnl": stats["unrealized_pnl"],
            "total_buy_amount": stats["total_buy"],
            "total_sell_amount": stats["total_sell"],
            "total_amount": stats["total_buy"] + stats["total_sell"],
            "timing_alpha": stats["timing_alpha"],
        })

    df = pl.DataFrame(rows)
    df = df.sort("total_pnl", descending=True)
    df = df.with_row_index("rank", offset=1)

    # Save ranking
    df.write_parquet(paths.broker_ranking)
    print(f"\nSaved: {paths.broker_ranking}")
    print(f"  {len(df)} brokers")
    print(f"  Total PNL range: {df['total_pnl'].min():,.0f} ~ {df['total_pnl'].max():,.0f}")

    # Layer 1.5 summary
    n_daily = len(list(paths.pnl_daily_dir.glob("*.parquet")))
    n_fifo = len(list(paths.fifo_state_dir.glob("*.parquet")))
    print(f"\nLayer 1.5:")
    print(f"  {n_daily} pnl_daily files")
    print(f"  {n_fifo} fifo_state files")

    return df


def _restore_fifo(fifo_path: Path) -> dict[str, FIFOAccount]:
    """Restore FIFO accounts from checkpoint.

    Returns:
        {broker: FIFOAccount} with lots pre-loaded.
    """
    df = pl.read_parquet(fifo_path)
    accounts: dict[str, FIFOAccount] = {}
    for row in df.iter_rows(named=True):
        broker = row["broker"]
        if broker not in accounts:
            accounts[broker] = FIFOAccount()
        lot = Lot(row["shares"], row["cost_per_share"], row["open_date"])
        if row["side"] == "long":
            accounts[broker].long_lots.append(lot)
        else:
            accounts[broker].short_lots.append(lot)
    return accounts


def process_symbol_incremental(
    symbol: str,
    paths: DataPaths,
    sym_prices: dict[date, float],
    sym_returns: dict[date, float],
    backtest_start: date,
    resume_after: date,
    merge_map: dict[str, str] | None = None,
) -> list[BrokerResult]:
    """Process a symbol incrementally from FIFO checkpoint.

    Restores FIFO from checkpoint, only runs FIFO for new dates.
    Still iterates all dates >= backtest_start for timing alpha.

    Args:
        resume_after: Last date already processed. Only FIFO-process dates > this.
    """
    trade_path = paths.symbol_trade_path(symbol)
    if not trade_path.exists():
        return []

    df = pl.read_parquet(trade_path)
    if len(df) == 0:
        return []

    # Remap broker codes for merged mode
    if merge_map:
        df = df.with_columns(
            pl.col("broker").replace(merge_map, default=pl.col("broker"))
        )
        df = df.group_by(["date", "broker"]).agg([
            pl.col("buy_shares").sum(),
            pl.col("sell_shares").sum(),
            pl.col("buy_amount").sum(),
            pl.col("sell_amount").sum(),
        ])

    # Check if there are new dates
    max_trade_date = df["date"].max()
    if max_trade_date <= resume_after:
        return []  # No new data

    dates = sorted(df["date"].unique().to_list())
    brokers = df["broker"].unique().to_list()

    # Pre-build trade lookup
    trade_lookup: dict[tuple[str, date], dict] = {}
    for row in df.iter_rows(named=True):
        trade_lookup[(row["broker"], row["date"])] = row

    # Restore FIFO state
    fifo_path = paths.symbol_fifo_state_path(symbol)
    accounts = _restore_fifo(fifo_path) if fifo_path.exists() else {}

    # Load existing pnl_daily for old cumulative realized
    pnl_daily_path = paths.symbol_pnl_daily_path(symbol)
    old_realized: dict[str, float] = {}
    if pnl_daily_path.exists():
        pnl_df = pl.read_parquet(pnl_daily_path)
        old_agg = (
            pnl_df.filter(pl.col("date") >= backtest_start)
            .group_by("broker")
            .agg(pl.col("realized_pnl").sum())
        )
        for row in old_agg.iter_rows(named=True):
            old_realized[row["broker"]] = row["realized_pnl"]

    results = []
    new_daily_rows: list[dict] = []
    fifo_rows: list[dict] = []

    for broker in brokers:
        account = accounts.get(broker, FIFOAccount())
        new_realized = 0.0
        total_buy = 0.0
        total_sell = 0.0
        last_unrealized = 0.0
        last_price = 0.0

        # Timing alpha needs full series from backtest_start
        net_buy_series: list[int] = []
        return_series: list[float] = []

        for d in dates:
            price = sym_prices.get(d, last_price)
            if price > 0:
                last_price = price

            row = trade_lookup.get((broker, d))

            if d <= resume_after:
                # Old dates: collect timing alpha + totals only (skip FIFO)
                if d >= backtest_start:
                    if row:
                        net_buy_series.append(
                            (row["buy_shares"] or 0) - (row["sell_shares"] or 0)
                        )
                        total_buy += row["buy_amount"] or 0.0
                        total_sell += row["sell_amount"] or 0.0
                    else:
                        net_buy_series.append(0)
                    return_series.append(sym_returns.get(d, 0.0))

                # Recalculate unrealized from restored FIFO (for last_unrealized)
                if d == resume_after:
                    unrealized = 0.0
                    for lot in account.long_lots:
                        unrealized += lot.shares * (price - lot.cost_per_share)
                    for lot in account.short_lots:
                        unrealized += lot.shares * (lot.cost_per_share - price)
                    last_unrealized = unrealized
                continue

            # New dates: full FIFO processing
            if row:
                buy_shares = row["buy_shares"] or 0
                sell_shares = row["sell_shares"] or 0
                buy_amount = row["buy_amount"] or 0.0
                sell_amount = row["sell_amount"] or 0.0

                realized, unrealized = account.process_day(
                    buy_shares, sell_shares,
                    buy_amount, sell_amount,
                    price, d,
                )

                if d >= backtest_start:
                    new_realized += realized
                    total_buy += buy_amount
                    total_sell += sell_amount
                    net_buy_series.append(buy_shares - sell_shares)
                    return_series.append(sym_returns.get(d, 0.0))

                last_unrealized = unrealized

                if realized != 0.0 or unrealized != 0.0:
                    new_daily_rows.append({
                        "broker": broker, "date": d,
                        "realized_pnl": realized,
                        "unrealized_pnl": unrealized,
                    })
            else:
                unrealized = 0.0
                for lot in account.long_lots:
                    unrealized += lot.shares * (price - lot.cost_per_share)
                for lot in account.short_lots:
                    unrealized += lot.shares * (lot.cost_per_share - price)
                last_unrealized = unrealized

                if d >= backtest_start:
                    net_buy_series.append(0)
                    return_series.append(sym_returns.get(d, 0.0))

                if unrealized != 0.0:
                    new_daily_rows.append({
                        "broker": broker, "date": d,
                        "realized_pnl": 0.0,
                        "unrealized_pnl": unrealized,
                    })

        # Total realized = old (from pnl_daily) + new (from FIFO)
        realized_after_start = old_realized.get(broker, 0.0) + new_realized
        total_pnl = realized_after_start + last_unrealized
        timing_alpha = compute_timing_alpha(net_buy_series, return_series)

        results.append(BrokerResult(
            broker=broker,
            total_pnl=total_pnl,
            realized_pnl=realized_after_start,
            unrealized_pnl=last_unrealized,
            total_buy=total_buy,
            total_sell=total_sell,
            timing_alpha=timing_alpha,
        ))

        for side, shares, cost, open_date in account.get_lots():
            fifo_rows.append({
                "broker": broker, "side": side,
                "shares": shares, "cost_per_share": cost,
                "open_date": open_date,
            })

    # Append new daily PNL events
    if new_daily_rows:
        new_daily_df = pl.DataFrame(new_daily_rows, schema={
            "broker": pl.Utf8,
            "date": pl.Date,
            "realized_pnl": pl.Float64,
            "unrealized_pnl": pl.Float64,
        })
        if pnl_daily_path.exists():
            existing = pl.read_parquet(pnl_daily_path)
            daily_df = pl.concat([existing, new_daily_df]).sort(["broker", "date"])
        else:
            daily_df = new_daily_df.sort(["broker", "date"])
        daily_df.write_parquet(pnl_daily_path)

    # Write FIFO state checkpoint
    if fifo_rows:
        fifo_df = pl.DataFrame(fifo_rows, schema={
            "broker": pl.Utf8,
            "side": pl.Utf8,
            "shares": pl.Int64,
            "cost_per_share": pl.Float64,
            "open_date": pl.Date,
        })
        fifo_df.write_parquet(paths.symbol_fifo_state_path(symbol))

    # Write per-symbol PNL
    if results:
        sym_rows = []
        for r in results:
            sym_rows.append({
                "broker": r.broker,
                "total_pnl": r.total_pnl,
                "realized_pnl": r.realized_pnl,
                "unrealized_pnl": r.unrealized_pnl,
                "total_buy_amount": r.total_buy,
                "total_sell_amount": r.total_sell,
                "timing_alpha": r.timing_alpha,
            })
        sym_df = pl.DataFrame(sym_rows)
        sym_df = sym_df.sort("total_pnl", descending=True)
        sym_df = sym_df.with_row_index("rank", offset=1)
        sym_df.write_parquet(paths.symbol_pnl_path(symbol))

    return results


def _rebuild_ranking(paths: DataPaths) -> pl.DataFrame:
    """Rebuild broker_ranking from all per-symbol PNL files."""
    broker_totals: dict[str, dict] = defaultdict(lambda: {
        "total_pnl": 0.0,
        "realized_pnl": 0.0,
        "unrealized_pnl": 0.0,
        "total_buy": 0.0,
        "total_sell": 0.0,
        "timing_alpha": 0.0,
    })

    pnl_files = list(paths.pnl_dir.glob("*.parquet"))
    for f in pnl_files:
        df = pl.read_parquet(f)
        for row in df.iter_rows(named=True):
            b = broker_totals[row["broker"]]
            b["total_pnl"] += row["total_pnl"]
            b["realized_pnl"] += row["realized_pnl"]
            b["unrealized_pnl"] += row["unrealized_pnl"]
            b["total_buy"] += row.get("total_buy_amount", 0.0)
            b["total_sell"] += row.get("total_sell_amount", 0.0)
            b["timing_alpha"] += row["timing_alpha"]

    rows = []
    for broker, stats in broker_totals.items():
        rows.append({
            "broker": broker,
            "total_pnl": stats["total_pnl"],
            "realized_pnl": stats["realized_pnl"],
            "unrealized_pnl": stats["unrealized_pnl"],
            "total_buy_amount": stats["total_buy"],
            "total_sell_amount": stats["total_sell"],
            "total_amount": stats["total_buy"] + stats["total_sell"],
            "timing_alpha": stats["timing_alpha"],
        })

    ranking = pl.DataFrame(rows)
    ranking = ranking.sort("total_pnl", descending=True)
    ranking = ranking.with_row_index("rank", offset=1)
    ranking.write_parquet(paths.broker_ranking)
    return ranking


def calculate_incremental_pnl(
    paths: DataPaths = DEFAULT_PATHS,
    config: AnalysisConfig = DEFAULT_CONFIG,
    workers: int | None = None,
    merge_map: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Incremental PNL: only process symbols with new data since last run.

    Steps:
        1. Find max date in existing pnl_daily (sample)
        2. Identify symbols with new data in daily_summary
        3. For those symbols: restore FIFO checkpoint, process new dates
        4. Rebuild broker_ranking from all pnl/ files
    """
    workers = workers or config.parallel_workers
    backtest_start = date.fromisoformat(config.backtest_start)
    paths.ensure_dirs()

    # 1. Find max date in existing pnl_daily
    pnl_daily_files = sorted(paths.pnl_daily_dir.glob("*.parquet"))
    if not pnl_daily_files:
        print("No existing pnl_daily found, running full PNL.")
        return calculate_all_pnl(paths, config, workers, merge_map)

    # Sample a few files to find max date
    sample_files = [
        pnl_daily_files[0],
        pnl_daily_files[len(pnl_daily_files) // 2],
        pnl_daily_files[-1],
    ]
    resume_after = date(2000, 1, 1)
    for f in sample_files:
        d = pl.read_parquet(f, columns=["date"])["date"].max()
        if d > resume_after:
            resume_after = d

    print(f"  Existing PNL data up to: {resume_after}")

    # 2. Find symbols with new data
    symbols = paths.list_symbols()
    symbols_to_update = []
    for sym in symbols:
        trade_path = paths.symbol_trade_path(sym)
        max_trade = pl.read_parquet(trade_path, columns=["date"])["date"].max()
        if max_trade > resume_after:
            symbols_to_update.append(sym)

    if not symbols_to_update:
        print("  No new data. PNL is up to date.")
        return pl.DataFrame()

    print(f"  {len(symbols_to_update)} symbols with new data")

    # 3. Load prices and returns
    print("Loading prices...")
    price_lookup = load_price_lookup(paths)
    print(f"  Loaded {len(price_lookup):,} price records")

    print("Calculating returns...")
    returns_lookup = calculate_returns(price_lookup)
    print(f"  Calculated {len(returns_lookup):,} daily returns")

    # Pre-partition by symbol
    prices_by_sym: dict[str, dict[date, float]] = defaultdict(dict)
    for (sym, d), price in price_lookup.items():
        if sym in symbols_to_update:
            prices_by_sym[sym][d] = price

    returns_by_sym: dict[str, dict[date, float]] = defaultdict(dict)
    for (sym, d), ret in returns_lookup.items():
        if sym in symbols_to_update:
            returns_by_sym[sym][d] = ret

    print(f"Processing {len(symbols_to_update)} symbols with {workers} workers...")

    # 4. Process symbols incrementally
    completed = 0
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                process_symbol_incremental,
                symbol, paths,
                prices_by_sym[symbol],
                returns_by_sym[symbol],
                backtest_start,
                resume_after,
                merge_map,
            ): symbol
            for symbol in symbols_to_update
        }

        for future in as_completed(futures):
            symbol = futures[future]
            completed += 1
            if completed % 100 == 0 or completed == len(symbols_to_update):
                print(f"  {completed}/{len(symbols_to_update)} symbols done")
            future.result()  # raise any exceptions

    # 5. Rebuild broker_ranking from all pnl/ files
    print("Rebuilding broker ranking...")
    df = _rebuild_ranking(paths)
    print(f"\nSaved: {paths.broker_ranking}")
    print(f"  {len(df)} brokers")
    if len(df) > 0:
        print(f"  Total PNL range: {df['total_pnl'].min():,.0f} ~ {df['total_pnl'].max():,.0f}")

    return df


def main() -> None:
    """CLI entry point."""
    import argparse
    import json

    parser = argparse.ArgumentParser(description="PNL Engine: FIFO-based PNL calculation")
    parser.add_argument("root", nargs="?", help="Project root directory")
    parser.add_argument("--no-merge", action="store_true", help="Skip broker merge (default: merged)")
    parser.add_argument("--incr", action="store_true", help="Incremental mode: only process new dates")
    args = parser.parse_args()

    config = DEFAULT_CONFIG
    use_merged = not args.no_merge

    if args.root:
        paths = DataPaths(root=Path(args.root), variant="" if args.no_merge else "merged")
    elif use_merged:
        paths = DataPaths(variant="merged")
    else:
        paths = DEFAULT_PATHS

    # Load merge map (default: merged mode)
    merge_map = None
    if use_merged:
        if not paths.broker_merge_map.exists():
            print(f"Error: Merge map not found: {paths.broker_merge_map}")
            print("Run generate_merge_map.py first.")
            sys.exit(1)
        with open(paths.broker_merge_map) as f:
            merge_map = json.load(f)
        print(f"Merged mode: {len(merge_map)} broker remappings")

    # Validate
    missing = paths.validate()
    if missing:
        print("Missing required files:")
        for p in missing:
            print(f"  - {p}")
        print("\nRun ETL and sync_prices first.")
        sys.exit(1)

    if args.incr:
        df = calculate_incremental_pnl(paths, config, merge_map=merge_map)
    else:
        df = calculate_all_pnl(paths, config, merge_map=merge_map)

    if len(df) > 0:
        print("\nTop 10 brokers by PNL:")
        print(df.head(10).select(["rank", "broker", "total_pnl", "timing_alpha"]))


if __name__ == "__main__":
    main()
