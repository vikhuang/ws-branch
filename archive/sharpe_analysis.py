"""Compute Sharpe ratio for each hypothesis strategy from exported signals.

Portfolio construction: equal-weight across all concurrent signals on same day.
Signal[T] → trade on T+1 (buy at open, sell at close).
Cost: 0.435% round-trip per trade.
"""

import polars as pl
import numpy as np
from pathlib import Path

SIGNALS_DIR = Path("data/signals")
PRICES_PATH = Path.home() / "r20/data/tej/prices.parquet"
COST = 0.00435

# Load prices
prices = pl.read_parquet(PRICES_PATH)
# Ensure date column
# TEJ prices columns: coid, mdate, open_d, close_d, ...
prices = prices.rename({"coid": "symbol", "mdate": "date", "open_d": "open", "close_d": "close"})

print(f"Prices: {prices.shape[0]:,} rows")
print(f"Date range: {prices['date'].min()} to {prices['date'].max()}")
print()

# Build lookup: (symbol, date) -> (open, close)
prices_sub = prices.select(["symbol", "date", "open", "close"]).drop_nulls()

# Get all trading dates per symbol
trading_dates = (
    prices_sub
    .sort("date")
    .group_by("symbol")
    .agg(pl.col("date").alias("dates"))
)
td_map: dict[str, list] = {}
for row in trading_dates.iter_rows(named=True):
    td_map[row["symbol"]] = sorted(row["dates"])

# Build price lookup
price_lookup: dict[tuple, tuple[float, float]] = {}
for row in prices_sub.iter_rows(named=True):
    price_lookup[(str(row["symbol"]), row["date"])] = (float(row["open"]), float(row["close"]))


def next_trading_date(symbol: str, d) -> object:
    """Find the next trading date after d for the given symbol."""
    dates = td_map.get(str(symbol))
    if not dates:
        return None
    # Binary search
    import bisect
    idx = bisect.bisect_right(dates, d)
    if idx < len(dates):
        return dates[idx]
    return None


strategies = sorted(SIGNALS_DIR.glob("*.csv"))

print(f"{'Strategy':<22} {'Signals':>8} {'Traded':>8} {'Long':>6} {'Short':>6} "
      f"{'TotalRet':>10} {'Sharpe':>8} {'MaxDD':>8} {'Calmar':>8} "
      f"{'AvgRet':>10} {'WinRate':>8} {'TradeDays':>10}")
print("=" * 140)

results = []

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

    # Parse date
    signals = signals.with_columns(pl.col("date").str.to_date().alias("date"))

    # Compute per-signal T+1 returns
    daily_returns: dict[object, list[float]] = {}  # trade_date -> list of returns
    n_traded = 0
    n_long = 0
    n_short = 0

    for row in signals.iter_rows(named=True):
        sym = str(row["symbol"])
        signal_date = row["date"]
        direction = row["dir_int"]

        if direction == 0:
            continue

        trade_date = next_trading_date(sym, signal_date)
        if trade_date is None:
            continue

        key = (sym, trade_date)
        if key not in price_lookup:
            continue

        open_p, close_p = price_lookup[key]
        if open_p <= 0:
            continue

        raw_return = (close_p - open_p) / open_p
        if direction > 0:
            net_return = raw_return - COST
            n_long += 1
        else:
            net_return = -raw_return - COST
            n_short += 1

        n_traded += 1

        if trade_date not in daily_returns:
            daily_returns[trade_date] = []
        daily_returns[trade_date].append(net_return)

    # Equal-weight portfolio: average returns per day
    if not daily_returns:
        print(f"{name:<22} {'NO TRADES':>8}")
        continue

    sorted_dates = sorted(daily_returns.keys())
    portfolio_daily = np.array([np.mean(daily_returns[d]) for d in sorted_dates])

    # Sharpe (annualized)
    sharpe = (
        float(np.mean(portfolio_daily) / np.std(portfolio_daily) * np.sqrt(252))
        if len(portfolio_daily) > 0 and np.std(portfolio_daily) > 0
        else 0.0
    )

    # Equity curve
    equity = np.cumprod(1 + portfolio_daily)
    total_return = float(equity[-1] - 1)

    # Max drawdown
    running_max = np.maximum.accumulate(equity)
    drawdowns = (equity - running_max) / running_max
    max_dd = float(np.min(drawdowns))

    # Calmar
    calmar = total_return / abs(max_dd) if max_dd != 0 else 0.0

    avg_ret = float(np.mean(portfolio_daily))
    win_rate = float(np.mean(portfolio_daily > 0))

    print(f"{name:<22} {len(signals):>8,} {n_traded:>8,} {n_long:>6,} {n_short:>6,} "
          f"{total_return:>9.2%} {sharpe:>8.2f} {max_dd:>8.2%} {calmar:>8.2f} "
          f"{avg_ret:>9.4%} {win_rate:>7.1%} {len(sorted_dates):>10,}")

    results.append({
        "strategy": name,
        "n_signals": len(signals),
        "n_traded": n_traded,
        "n_long": n_long,
        "n_short": n_short,
        "total_return": total_return,
        "sharpe": sharpe,
        "max_dd": max_dd,
        "calmar": calmar,
        "avg_daily_return": avg_ret,
        "win_rate": win_rate,
        "n_trade_days": len(sorted_dates),
    })

print()
print("=" * 80)
print("RANKING BY SHARPE RATIO")
print("=" * 80)
results.sort(key=lambda x: x["sharpe"], reverse=True)
for i, r in enumerate(results, 1):
    print(f"  {i}. {r['strategy']:<22} Sharpe={r['sharpe']:>6.2f}  "
          f"Return={r['total_return']:>8.2%}  MaxDD={r['max_dd']:>8.2%}  "
          f"WinRate={r['win_rate']:.1%}  Trades={r['n_traded']:,}")
