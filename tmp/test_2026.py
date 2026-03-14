"""Test Information Fragmentation Alpha on 2026 data.

Uses same training period (2023-01 ~ 2024-06) for TA calculation.
Runs backtest on three periods for comparison:
  - Original test: 2024-07 ~ 2025-12
  - 2026 only: 2026-01 ~ 2026-03
  - Extended: 2024-07 ~ 2026-03
"""

import sys
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from broker_analytics.application.services.market_scan import (
    SIGMA_THRESHOLD,
    analyze_symbol,
)
from broker_analytics.domain.backtest import run_backtest
from broker_analytics.infrastructure.config import DEFAULT_PATHS
from broker_analytics.infrastructure.repositories.price_repo import PriceRepository

SYMBOLS = ["2383", "4749", "2345", "3017", "2455", "3665", "8996", "6683"]
TRAIN_START = "2023-01-01"
TRAIN_END = "2024-06-30"
COST = 0.00435


def build_signal_and_backtest(symbol: str, test_start: str, test_end: str):
    """Build TA signal (train fixed) and backtest on given test window."""
    price_repo = PriceRepository(DEFAULT_PATHS)
    prices = price_repo.get_close_prices(symbol)
    if not prices:
        return None

    train_start_d = date.fromisoformat(TRAIN_START)
    train_end_d = date.fromisoformat(TRAIN_END)
    test_start_d = date.fromisoformat(test_start)
    test_end_d = date.fromisoformat(test_end)

    sorted_dates = sorted(prices.keys())
    all_dates = [d for d in sorted_dates if train_start_d <= d <= test_end_d]
    train_dates = [d for d in all_dates if train_start_d <= d <= train_end_d]
    test_dates = [d for d in all_dates if test_start_d <= d <= test_end_d]

    if len(train_dates) < 30 or len(test_dates) < 5:
        return None

    # Returns
    returns: dict[date, float] = {}
    for i in range(1, len(sorted_dates)):
        prev_p = prices[sorted_dates[i - 1]]
        curr_p = prices[sorted_dates[i]]
        if prev_p > 0:
            returns[sorted_dates[i]] = (curr_p - prev_p) / prev_p

    # Load trades
    path = DEFAULT_PATHS.daily_summary_dir / f"{symbol}.parquet"
    trades = pl.read_parquet(path).with_columns(pl.col("broker").cast(pl.Utf8))

    date_set = set(all_dates)
    trade_lookup: dict[tuple[str, date], int] = {}
    for row in trades.iter_rows(named=True):
        d = row["date"]
        if d in date_set:
            nb = (row["buy_shares"] or 0) - (row["sell_shares"] or 0)
            trade_lookup[(row["broker"], d)] = nb

    brokers = list({k[0] for k in trade_lookup.keys()})

    # Broker stats (mean/std over ALL dates incl test)
    broker_stats: dict[str, dict] = {}
    for broker in brokers:
        nb_series = np.array(
            [trade_lookup.get((broker, d), 0) for d in all_dates], dtype=np.float64
        )
        mean_nb = float(np.mean(nb_series))
        std_nb = float(np.std(nb_series))
        if std_nb == 0:
            continue
        broker_stats[broker] = {"mean": mean_nb, "std": std_nb, "ta": 0.0}

    # Compute TA on training period only
    from broker_analytics.domain.timing_alpha import compute_timing_alpha

    for broker, bs in broker_stats.items():
        nb_train = [trade_lookup.get((broker, d), 0) for d in train_dates]
        ret_train = [returns.get(d, 0.0) for d in train_dates]
        bs["ta"] = compute_timing_alpha(nb_train, ret_train)

    # Build daily signal
    signal: dict[date, float] = {}
    for d in all_dates:
        day_sig = 0.0
        for broker, bs in broker_stats.items():
            ta = bs["ta"]
            if ta == 0.0:
                continue
            nb = trade_lookup.get((broker, d), 0)
            dev = nb - bs["mean"]
            if abs(dev) > SIGMA_THRESHOLD * bs["std"]:
                day_sig += ta * dev / bs["std"]
        signal[d] = day_sig

    # Correlation on test period
    test_set = set(test_dates)
    test_sigs, test_rets = [], []
    for i, d in enumerate(all_dates[1:], 1):
        prev_d = all_dates[i - 1]
        sig = signal.get(prev_d, 0.0)
        ret = returns.get(d, 0.0)
        if d in test_set:
            test_sigs.append(sig)
            test_rets.append(ret)

    test_corr = 0.0
    if len(test_sigs) >= 5:
        c = float(np.corrcoef(test_sigs, test_rets)[0, 1])
        if not np.isnan(c):
            test_corr = c

    # Load OHLC for backtest
    ohlc = price_repo.get_ohlc(symbol)
    if ohlc is None or ohlc.is_empty():
        return None

    bt = run_backtest(signal, ohlc, test_dates, cost=COST)

    return {
        "symbol": symbol,
        "n_test_days": len(test_dates),
        "test_corr": test_corr,
        "total_return": bt.total_return,
        "sharpe": bt.sharpe,
        "max_dd": bt.max_dd,
        "n_long": bt.n_long,
        "n_short": bt.n_short,
        "avg_long": bt.avg_long_return,
        "avg_short": bt.avg_short_return,
        "wr_long": bt.win_rate_long,
        "wr_short": bt.win_rate_short,
        "bh_return": bt.bh_return,
        "monthly": bt.monthly_returns,
    }


def main():
    periods = [
        ("Original (2024-07~2025-12)", "2024-07-01", "2025-12-31"),
        ("2026 only (2026-01~03)", "2026-01-01", "2026-03-31"),
        ("Extended (2024-07~2026-03)", "2024-07-01", "2026-03-31"),
    ]

    for period_name, test_start, test_end in periods:
        print(f"\n{'='*70}")
        print(f"  {period_name}")
        print(f"  Train: {TRAIN_START} ~ {TRAIN_END}")
        print(f"  Test:  {test_start} ~ {test_end}")
        print(f"{'='*70}")
        print(
            f"{'Symbol':>8} {'Days':>5} {'Return':>10} {'Sharpe':>7} {'MaxDD':>8} "
            f"{'Corr':>6} {'L_avg':>7} {'S_avg':>7} {'L_wr':>5} {'S_wr':>5} "
            f"{'B&H':>10}"
        )
        print("-" * 95)

        for sym in SYMBOLS:
            r = build_signal_and_backtest(sym, test_start, test_end)
            if r is None:
                print(f"{sym:>8} -- no data")
                continue
            print(
                f"{r['symbol']:>8} {r['n_test_days']:>5} "
                f"{r['total_return']:>+10.1%} {r['sharpe']:>7.2f} {r['max_dd']:>+8.1%} "
                f"{r['test_corr']:>6.3f} "
                f"{r['avg_long']:>+7.2%} {r['avg_short']:>+7.2%} "
                f"{r['wr_long']:>5.0%} {r['wr_short']:>5.0%} "
                f"{r['bh_return']:>+10.1%}"
            )

        # 2026 monthly detail
        if "2026" in test_start:
            print(f"\n  Monthly detail:")
            for sym in SYMBOLS:
                r = build_signal_and_backtest(sym, test_start, test_end)
                if r is None:
                    continue
                months = [m for m in r["monthly"] if m["month"].startswith("2026")]
                detail = "  ".join(
                    f"{m['month']}: {m['strategy']:+.1f}% (B&H {m['bh']:+.1f}%)"
                    for m in months
                )
                print(f"    {sym}: {detail}")


if __name__ == "__main__":
    main()
