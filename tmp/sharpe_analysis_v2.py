"""Sharpe ratio analysis v2: multi-horizon, with/without cost, close-to-close."""

import polars as pl
import numpy as np
from pathlib import Path
import bisect

SIGNALS_DIR = Path("data/signals")
PRICES_PATH = Path.home() / "r20/data/tej/prices.parquet"
COST = 0.00435
HORIZONS = [1, 5, 10, 20]

# Load prices
prices = pl.read_parquet(PRICES_PATH)
prices = prices.rename({"coid": "symbol", "mdate": "date", "close_d": "close"})
prices = prices.select(["symbol", "date", "close"]).drop_nulls()
prices = prices.sort(["symbol", "date"])

print(f"Prices: {prices.shape[0]:,} rows | {prices['date'].min()} to {prices['date'].max()}")
print()

# Build per-symbol date→close lookup
sym_dates: dict[str, list] = {}
sym_close: dict[str, dict] = {}

for row in prices.iter_rows(named=True):
    s = str(row["symbol"])
    d = row["date"]
    c = float(row["close"])
    if s not in sym_dates:
        sym_dates[s] = []
        sym_close[s] = {}
    sym_dates[s].append(d)
    sym_close[s][d] = c

# Sort dates
for s in sym_dates:
    sym_dates[s].sort()


def get_future_date(symbol: str, d, offset: int):
    """Get trading date `offset` days after d."""
    dates = sym_dates.get(str(symbol))
    if not dates:
        return None
    idx = bisect.bisect_right(dates, d)
    target = idx + offset - 1
    if 0 <= target < len(dates):
        return dates[target]
    return None


def compute_return(symbol: str, date_from, date_to) -> float | None:
    """Close-to-close return."""
    s = str(symbol)
    c0 = sym_close.get(s, {}).get(date_from)
    c1 = sym_close.get(s, {}).get(date_to)
    if c0 and c1 and c0 > 0:
        return (c1 - c0) / c0
    return None


strategies = sorted(SIGNALS_DIR.glob("*.csv"))

# Results table
all_results = []

for csv_path in strategies:
    name = csv_path.stem
    signals = pl.read_csv(csv_path)

    # Parse direction
    if signals["direction"].dtype == pl.Utf8:
        signals = signals.with_columns(
            pl.when(pl.col("direction") == "long").then(1)
            .when(pl.col("direction") == "short").then(-1)
            .otherwise(0)
            .alias("dir_int")
        )
    else:
        signals = signals.with_columns(pl.col("direction").alias("dir_int"))

    signals = signals.with_columns(pl.col("date").str.to_date().alias("date"))

    strategy_results = {"strategy": name, "n_signals": len(signals)}

    for horizon in HORIZONS:
        # Collect per-trade returns for this horizon
        daily_returns: dict[object, list[float]] = {}  # entry_date -> [returns]
        n_traded = 0

        for row in signals.iter_rows(named=True):
            sym = str(row["symbol"])
            signal_date = row["date"]
            direction = row["dir_int"]
            if direction == 0:
                continue

            # Entry: next trading day after signal
            entry_date = get_future_date(sym, signal_date, 1)
            if entry_date is None:
                continue

            # Exit: `horizon` trading days after entry
            exit_date = get_future_date(sym, entry_date, horizon)
            if exit_date is None:
                continue

            entry_close = sym_close.get(sym, {}).get(entry_date)
            # Use signal_date close as entry (signal at close → measure from close)
            signal_close = sym_close.get(sym, {}).get(signal_date)
            exit_close = sym_close.get(sym, {}).get(exit_date)

            if signal_close and exit_close and signal_close > 0:
                raw_ret = (exit_close - signal_close) / signal_close
                net_ret = raw_ret * direction  # direction-adjusted
                n_traded += 1

                if entry_date not in daily_returns:
                    daily_returns[entry_date] = []
                daily_returns[entry_date].append(net_ret)

        if not daily_returns:
            for suffix in ["_raw", "_net"]:
                strategy_results[f"h{horizon}_sharpe{suffix}"] = None
                strategy_results[f"h{horizon}_mean{suffix}"] = None
            strategy_results[f"h{horizon}_traded"] = 0
            continue

        # Equal-weight portfolio daily returns
        sorted_dates = sorted(daily_returns.keys())
        port_returns = np.array([np.mean(daily_returns[d]) for d in sorted_dates])

        # Per-trade cost: deduct once at entry (cost is round-trip)
        port_returns_net = port_returns - COST

        # Annualize factor: adjust for holding period
        ann_factor = np.sqrt(252 / horizon)

        # Raw Sharpe (no cost)
        sharpe_raw = (
            float(np.mean(port_returns) / np.std(port_returns) * ann_factor)
            if len(port_returns) > 0 and np.std(port_returns) > 0
            else 0.0
        )

        # Net Sharpe (with cost)
        sharpe_net = (
            float(np.mean(port_returns_net) / np.std(port_returns_net) * ann_factor)
            if len(port_returns_net) > 0 and np.std(port_returns_net) > 0
            else 0.0
        )

        strategy_results[f"h{horizon}_sharpe_raw"] = sharpe_raw
        strategy_results[f"h{horizon}_sharpe_net"] = sharpe_net
        strategy_results[f"h{horizon}_mean_raw"] = float(np.mean(port_returns))
        strategy_results[f"h{horizon}_mean_net"] = float(np.mean(port_returns_net))
        strategy_results[f"h{horizon}_traded"] = n_traded
        strategy_results[f"h{horizon}_win_rate"] = float(np.mean(port_returns > 0))
        strategy_results[f"h{horizon}_trade_days"] = len(sorted_dates)

    all_results.append(strategy_results)

# Print results
for horizon in HORIZONS:
    print(f"\n{'='*100}")
    print(f"  HORIZON = {horizon} day(s) | close-to-close | annualized Sharpe = mean/std × √(252/{horizon})")
    print(f"{'='*100}")
    print(f"  {'Strategy':<22} {'Trades':>8} {'Days':>6} {'MeanRaw':>10} {'MeanNet':>10} "
          f"{'WinRate':>8} {'Sharpe(raw)':>12} {'Sharpe(net)':>12}")
    print(f"  {'-'*92}")

    # Sort by raw Sharpe
    sorted_res = sorted(all_results, key=lambda x: x.get(f"h{horizon}_sharpe_raw") or -99, reverse=True)
    for r in sorted_res:
        traded = r.get(f"h{horizon}_traded", 0)
        if traded == 0:
            print(f"  {r['strategy']:<22} {'N/A':>8}")
            continue
        print(f"  {r['strategy']:<22} {traded:>8,} {r[f'h{horizon}_trade_days']:>6,} "
              f"{r[f'h{horizon}_mean_raw']:>9.3%} {r[f'h{horizon}_mean_net']:>9.3%} "
              f"{r[f'h{horizon}_win_rate']:>7.1%} "
              f"{r[f'h{horizon}_sharpe_raw']:>12.2f} {r[f'h{horizon}_sharpe_net']:>12.2f}")

# Summary table
print(f"\n\n{'='*100}")
print("  SUMMARY: RAW SHARPE ACROSS HORIZONS (no cost)")
print(f"{'='*100}")
print(f"  {'Strategy':<22} {'H1':>8} {'H5':>8} {'H10':>8} {'H20':>8} {'Best':>8}")
print(f"  {'-'*60}")
for r in sorted(all_results, key=lambda x: max(
    x.get(f"h{h}_sharpe_raw") or -99 for h in HORIZONS
), reverse=True):
    vals = []
    for h in HORIZONS:
        v = r.get(f"h{h}_sharpe_raw")
        vals.append(v if v is not None else -99)
    best = max(vals)
    print(f"  {r['strategy']:<22} {vals[0]:>8.2f} {vals[1]:>8.2f} {vals[2]:>8.2f} {vals[3]:>8.2f} {best:>8.2f}")

print(f"\n  {'Strategy':<22} {'H1':>8} {'H5':>8} {'H10':>8} {'H20':>8} {'Best':>8}")
print(f"  {'-'*60}")
print("  NET SHARPE (with 0.435% cost)")
for r in sorted(all_results, key=lambda x: max(
    x.get(f"h{h}_sharpe_net") or -99 for h in HORIZONS
), reverse=True):
    vals = []
    for h in HORIZONS:
        v = r.get(f"h{h}_sharpe_net")
        vals.append(v if v is not None else -99)
    best = max(vals)
    print(f"  {r['strategy']:<22} {vals[0]:>8.2f} {vals[1]:>8.2f} {vals[2]:>8.2f} {vals[3]:>8.2f} {best:>8.2f}")
