"""Signal Report: Large trade detection → statistical validation → TA-weighted backtest.

For a given stock symbol, this service:
1. Detects abnormal trading activity (>2σ from broker's mean)
2. Validates whether large trades predict next-day returns
3. Builds a TA-weighted aggregate signal using all brokers
4. Backtests the signal with realistic open→close returns and costs
"""

import json
import sys
from dataclasses import dataclass, field, asdict
from datetime import date, datetime
from pathlib import Path

import numpy as np
import polars as pl

from broker_analytics.domain.backtest import run_backtest as _domain_backtest
from broker_analytics.domain.timing_alpha import compute_timing_alpha
from broker_analytics.infrastructure.config import DataPaths, DEFAULT_PATHS
from broker_analytics.infrastructure.repositories.price_repo import PriceRepository

# =============================================================================
# Constants
# =============================================================================

SIGMA_THRESHOLD = 2.0
COST_PER_TRADE = 0.00435  # 0.1425% commission × 2 + 0.15% day-trade tax
TOP_N = 15

DEFAULT_TRAIN_START = "2023-01-01"
DEFAULT_TRAIN_END = "2024-06-30"
DEFAULT_TEST_START = "2024-07-01"
DEFAULT_TEST_END = "2025-12-31"

EARLY_EXIT_MIN_SIGNIFICANT_PCT = 0.05  # 5%
EARLY_EXIT_MIN_TSTAT = 2.0


# =============================================================================
# Dataclasses
# =============================================================================

@dataclass
class BrokerStats:
    broker: str
    timing_alpha: float
    mean_net_buy: float
    std_net_buy: float
    n_large: int
    large_spread: float = 0.0
    large_tstat: float = 0.0


@dataclass
class SignalResult:
    symbol: str
    generated: str
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    n_train_days: int
    n_test_days: int

    # Step 1
    n_brokers_total: int = 0
    n_brokers_with_large: int = 0

    # Step 2
    n_brokers_significant: int = 0
    pct_significant: float = 0.0

    # Step 3
    train_corr: float = 0.0
    train_tstat: float = 0.0
    test_corr: float = 0.0
    test_tstat: float = 0.0
    n_signal_long: int = 0
    n_signal_short: int = 0

    # Step 4
    total_return: float | None = None
    sharpe: float | None = None
    max_dd: float | None = None
    calmar: float | None = None
    n_long: int = 0
    n_short: int = 0
    avg_long_return: float = 0.0
    avg_short_return: float = 0.0
    win_rate_long: float = 0.0
    win_rate_short: float = 0.0
    bh_return: float = 0.0
    bh_sharpe: float | None = None
    bh_max_dd: float | None = None
    monthly_returns: list = field(default_factory=list)

    early_exit: str | None = None


# =============================================================================
# Step 0: Data Loading
# =============================================================================

def load_trade_data(symbol: str, paths: DataPaths = DEFAULT_PATHS) -> pl.DataFrame:
    """Load daily_summary/{symbol}.parquet."""
    path = paths.symbol_trade_path(symbol)
    if not path.exists():
        print(f"Error: {path} not found. Run ETL first.")
        sys.exit(1)
    return pl.read_parquet(path)


def load_close_prices(symbol: str, paths: DataPaths = DEFAULT_PATHS) -> dict[date, float]:
    """Extract symbol's close prices."""
    repo = PriceRepository(paths)
    prices = repo.get_close_prices(symbol)
    if not prices:
        print(f"Error: No prices for {symbol}. Run sync_prices.py first.")
        sys.exit(1)
    return prices


def load_ohlc(symbol: str, paths: DataPaths = DEFAULT_PATHS) -> pl.DataFrame:
    """Fetch OHLC via ws-core."""
    repo = PriceRepository(paths)
    return repo.get_ohlc(symbol)


# =============================================================================
# Step 1: Detect Large Trades
# =============================================================================

def detect_large_trades(
    trades: pl.DataFrame,
    all_dates: list[date],
) -> dict[str, BrokerStats]:
    """Per broker: compute mean/std of net_buy, flag >2σ days."""
    trades = trades.with_columns(pl.col("broker").cast(pl.Utf8))
    brokers = trades["broker"].unique().to_list()
    date_set = set(all_dates)

    trade_lookup: dict[tuple[str, date], int] = {}
    for row in trades.iter_rows(named=True):
        d = row["date"]
        if d in date_set:
            nb = (row["buy_shares"] or 0) - (row["sell_shares"] or 0)
            trade_lookup[(row["broker"], d)] = nb

    stats = {}
    for broker in brokers:
        nb_series = np.array([
            trade_lookup.get((broker, d), 0) for d in all_dates
        ], dtype=np.float64)

        mean_nb = float(np.mean(nb_series))
        std_nb = float(np.std(nb_series))

        if std_nb == 0:
            continue

        n_large = int(np.sum(np.abs(nb_series - mean_nb) > SIGMA_THRESHOLD * std_nb))

        stats[broker] = BrokerStats(
            broker=broker,
            timing_alpha=0.0,
            mean_net_buy=mean_nb,
            std_net_buy=std_nb,
            n_large=n_large,
        )

    return stats


# =============================================================================
# Step 2: Statistical Validation
# =============================================================================

def validate_brokers(
    broker_stats: dict[str, BrokerStats],
    trades: pl.DataFrame,
    returns: dict[date, float],
    all_dates: list[date],
) -> tuple[dict[str, BrokerStats], bool]:
    """Compute per-broker large-trade spread and t-stat.

    Returns (updated stats, should_continue).
    """
    trades = trades.with_columns(pl.col("broker").cast(pl.Utf8))

    trade_lookup: dict[tuple[str, date], int] = {}
    for row in trades.iter_rows(named=True):
        nb = (row["buy_shares"] or 0) - (row["sell_shares"] or 0)
        trade_lookup[(row["broker"], row["date"])] = nb

    n_significant = 0

    for broker, bs in broker_stats.items():
        large_returns = []
        non_large_returns = []

        for i, d in enumerate(all_dates[:-1]):
            nb = trade_lookup.get((broker, d), 0)
            next_ret = returns.get(all_dates[i + 1], 0.0)
            dev = abs(nb - bs.mean_net_buy)

            if dev > SIGMA_THRESHOLD * bs.std_net_buy:
                large_returns.append(next_ret)
            else:
                non_large_returns.append(next_ret)

        if len(large_returns) >= 3 and len(non_large_returns) >= 3:
            large_arr = np.array(large_returns)
            non_large_arr = np.array(non_large_returns)
            spread = float(np.mean(large_arr) - np.mean(non_large_arr))

            n1, n2 = len(large_arr), len(non_large_arr)
            s1, s2 = float(np.std(large_arr, ddof=1)), float(np.std(non_large_arr, ddof=1))
            if s1 > 0 or s2 > 0:
                se = ((s1**2 / n1) + (s2**2 / n2)) ** 0.5
                tstat = spread / se if se > 0 else 0.0
            else:
                tstat = 0.0

            bs.large_spread = spread
            bs.large_tstat = tstat

            if tstat > 1.96:
                n_significant += 1
        else:
            bs.large_spread = 0.0
            bs.large_tstat = 0.0

    total = len(broker_stats)
    pct = n_significant / total if total > 0 else 0.0
    should_continue = pct >= EARLY_EXIT_MIN_SIGNIFICANT_PCT

    print(f"  Significant positive brokers: {n_significant}/{total} ({pct:.1%})")

    return broker_stats, should_continue


# =============================================================================
# Step 3: TA-Weighted Aggregate Signal
# =============================================================================

def compute_train_ta(
    trades: pl.DataFrame,
    broker_stats: dict[str, BrokerStats],
    train_dates: list[date],
    returns: dict[date, float],
) -> None:
    """Compute timing_alpha for each broker using ONLY training period data.

    Updates broker_stats in-place.
    """
    trades = trades.with_columns(pl.col("broker").cast(pl.Utf8))

    trade_lookup: dict[tuple[str, date], int] = {}
    for row in trades.iter_rows(named=True):
        nb = (row["buy_shares"] or 0) - (row["sell_shares"] or 0)
        trade_lookup[(str(row["broker"]), row["date"])] = nb

    for broker, bs in broker_stats.items():
        nb_series = [trade_lookup.get((broker, d), 0) for d in train_dates]
        ret_series = [returns.get(d, 0.0) for d in train_dates]
        bs.timing_alpha = compute_timing_alpha(nb_series, ret_series)


def build_ta_signal(
    trades: pl.DataFrame,
    broker_stats: dict[str, BrokerStats],
    all_dates: list[date],
    train_dates: list[date],
    test_dates: list[date],
    returns: dict[date, float],
) -> tuple[dict[date, float], SignalResult | None, bool]:
    """Build daily signal = Σ(TA_b × dev_b[t] / σ_b) for |dev| > 2σ."""
    compute_train_ta(trades, broker_stats, train_dates, returns)

    trades = trades.with_columns(pl.col("broker").cast(pl.Utf8))

    trade_lookup: dict[tuple[str, date], int] = {}
    for row in trades.iter_rows(named=True):
        nb = (row["buy_shares"] or 0) - (row["sell_shares"] or 0)
        trade_lookup[(row["broker"], row["date"])] = nb

    signal = {}
    for d in all_dates:
        day_signal = 0.0
        for broker, bs in broker_stats.items():
            ta = bs.timing_alpha
            if ta == 0.0:
                continue
            nb = trade_lookup.get((broker, d), 0)
            dev = nb - bs.mean_net_buy
            if abs(dev) > SIGMA_THRESHOLD * bs.std_net_buy:
                day_signal += ta * dev / bs.std_net_buy
        signal[d] = day_signal

    # Evaluate correlation
    train_set = set(train_dates)
    train_signals, train_returns = [], []
    for i, d in enumerate(all_dates[1:], 1):
        if d in train_set:
            prev_d = all_dates[i - 1]
            if prev_d in signal:
                train_signals.append(signal[prev_d])
                train_returns.append(returns.get(d, 0.0))

    train_corr, train_tstat = 0.0, 0.0
    if len(train_signals) >= 10:
        s_arr = np.array(train_signals)
        r_arr = np.array(train_returns)
        corr = float(np.corrcoef(s_arr, r_arr)[0, 1])
        n = len(s_arr)
        tstat = corr * ((n - 2) ** 0.5) / ((1 - corr**2) ** 0.5) if abs(corr) < 1 else 0.0
        train_corr, train_tstat = corr, tstat

    test_set = set(test_dates)
    test_signals, test_returns_list = [], []
    for i, d in enumerate(all_dates[1:], 1):
        if d in test_set:
            prev_d = all_dates[i - 1]
            if prev_d in signal:
                test_signals.append(signal[prev_d])
                test_returns_list.append(returns.get(d, 0.0))

    test_corr, test_tstat = 0.0, 0.0
    if len(test_signals) >= 10:
        s_arr = np.array(test_signals)
        r_arr = np.array(test_returns_list)
        corr = float(np.corrcoef(s_arr, r_arr)[0, 1])
        n = len(s_arr)
        tstat = corr * ((n - 2) ** 0.5) / ((1 - corr**2) ** 0.5) if abs(corr) < 1 else 0.0
        test_corr, test_tstat = corr, tstat

    n_long = sum(1 for d in test_dates if signal.get(d, 0) > 0)
    n_short = sum(1 for d in test_dates if signal.get(d, 0) < 0)

    print(f"  Train: corr={train_corr:.4f}, t={train_tstat:.2f} ({len(train_signals)} days)")
    print(f"  Test:  corr={test_corr:.4f}, t={test_tstat:.2f} ({len(test_signals)} days)")
    print(f"  Signal days (test): {n_long} long, {n_short} short")

    should_continue = abs(test_tstat) >= EARLY_EXIT_MIN_TSTAT

    return signal, None, should_continue


# =============================================================================
# Report Generation
# =============================================================================

def generate_markdown(symbol: str, result: SignalResult) -> str:
    """Generate human-readable .md report."""
    lines = [
        f"# Signal Report: {symbol}",
        f"Generated: {result.generated}",
        "",
        "## Summary",
        "",
    ]

    if result.early_exit:
        lines.append(f"- **Status**: EARLY EXIT — {result.early_exit}")
    else:
        lines.append("- **Status**: PASS")

    lines += [
        f"- Train: {result.train_start} ~ {result.train_end} ({result.n_train_days} days)",
        f"- Test: {result.test_start} ~ {result.test_end} ({result.n_test_days} days)",
        "",
        "## Step 1: Large Trade Detection",
        "",
        f"- Active brokers: {result.n_brokers_total}",
        f"- Brokers with large trades: {result.n_brokers_with_large}",
        "",
        "## Step 2: Statistical Validation",
        "",
        f"- Significant positive brokers: {result.n_brokers_significant}/{result.n_brokers_total} ({result.pct_significant:.1%})",
        "",
    ]

    if result.early_exit and "insufficient" in result.early_exit.lower():
        lines += [
            f"> Early exit: {result.early_exit}",
            "",
        ]
        return "\n".join(lines)

    lines += [
        "## Step 3: TA-Weighted Signal",
        "",
        "| Period | Correlation | t-stat |",
        "|--------|-------------|--------|",
        f"| Train  | {result.train_corr:+.4f}    | {result.train_tstat:+.2f}   |",
        f"| Test   | {result.test_corr:+.4f}    | {result.test_tstat:+.2f}   |",
        "",
        f"Signal days (test): {result.n_signal_long} long, {result.n_signal_short} short",
        "",
    ]

    if result.early_exit and "not significant" in result.early_exit.lower():
        lines += [
            f"> Early exit: {result.early_exit}",
            "",
        ]
        return "\n".join(lines)

    if result.total_return is not None:
        lines += [
            "## Step 4: Backtest (test period)",
            "",
            "| Metric | Strategy | Buy & Hold |",
            "|--------|----------|------------|",
            f"| Total Return | {result.total_return:+.1%} | {result.bh_return:+.1%} |",
            f"| Sharpe | {result.sharpe:.2f} | {result.bh_sharpe:.2f} |" if result.bh_sharpe is not None else f"| Sharpe | {result.sharpe:.2f} | — |",
            f"| Max DD | {result.max_dd:+.1%} | {result.bh_max_dd:+.1%} |" if result.bh_max_dd is not None else f"| Max DD | {result.max_dd:+.1%} | — |",
            f"| Calmar | {result.calmar:.2f} | — |",
            "",
            "### Per-trade Stats",
            "",
            "| Side | Trades | Avg Return | Win Rate |",
            "|------|--------|------------|----------|",
            f"| Long | {result.n_long} | {result.avg_long_return:+.2%} | {result.win_rate_long:.0%} |",
            f"| Short | {result.n_short} | {result.avg_short_return:+.2%} | {result.win_rate_short:.0%} |",
            "",
        ]

        if result.monthly_returns:
            lines += [
                "### Monthly Returns",
                "",
                "| Month | Strategy | B&H |",
                "|-------|----------|-----|",
            ]
            for m in result.monthly_returns:
                lines.append(f"| {m['month']} | {m['strategy']:+.1f}% | {m['bh']:+.1f}% |")
            lines.append("")

    return "\n".join(lines)


def generate_json_output(symbol: str, result: SignalResult) -> str:
    """Generate machine-readable .json report."""
    d = asdict(result)
    d["symbol"] = symbol
    return json.dumps(d, indent=2, ensure_ascii=False, default=str)


def save_reports(symbol: str, result: SignalResult, paths: DataPaths = DEFAULT_PATHS) -> None:
    """Save both .md and .json to reports dir."""
    reports_dir = paths.reports_dir
    reports_dir.mkdir(parents=True, exist_ok=True)

    md_path = reports_dir / f"{symbol}.md"
    md_path.write_text(generate_markdown(symbol, result), encoding="utf-8")
    print(f"  Saved: {md_path}")

    json_path = reports_dir / f"{symbol}.json"
    json_path.write_text(generate_json_output(symbol, result), encoding="utf-8")
    print(f"  Saved: {json_path}")


# =============================================================================
# Main Pipeline
# =============================================================================

def run_pipeline(
    symbol: str,
    train_start: str = DEFAULT_TRAIN_START,
    train_end: str = DEFAULT_TRAIN_END,
    test_start: str = DEFAULT_TEST_START,
    test_end: str = DEFAULT_TEST_END,
    paths: DataPaths = DEFAULT_PATHS,
) -> SignalResult:
    """Execute full signal analysis pipeline."""
    print(f"\n{'='*60}")
    print(f"Signal Report: {symbol}")
    print(f"{'='*60}")

    # --- Step 0: Load data ---
    print("\n[Step 0] Loading data...")
    trades = load_trade_data(symbol, paths)
    close_prices = load_close_prices(symbol, paths)
    ohlc = load_ohlc(symbol, paths)

    # Compute close-to-close returns
    sorted_dates = sorted(close_prices.keys())
    returns: dict[date, float] = {}
    for i in range(1, len(sorted_dates)):
        prev_p = close_prices[sorted_dates[i - 1]]
        curr_p = close_prices[sorted_dates[i]]
        if prev_p > 0:
            returns[sorted_dates[i]] = (curr_p - prev_p) / prev_p

    # Determine train/test split
    ts = date.fromisoformat(train_start)
    te = date.fromisoformat(train_end)
    test_s = date.fromisoformat(test_start)
    test_e = date.fromisoformat(test_end)

    all_dates = [d for d in sorted_dates if d >= ts and d <= test_e]
    train_dates = [d for d in all_dates if ts <= d <= te]
    test_dates = [d for d in all_dates if test_s <= d <= test_e]

    # Fallback to 60/40 split if insufficient data
    if len(train_dates) < 30 or len(test_dates) < 30:
        print("  Insufficient data for default split, using 60/40 fallback...")
        n = len(all_dates)
        split_idx = int(n * 0.6)
        train_dates = all_dates[:split_idx]
        test_dates = all_dates[split_idx:]
        if train_dates:
            ts, te = train_dates[0], train_dates[-1]
            train_start, train_end = ts.isoformat(), te.isoformat()
        if test_dates:
            test_s, test_e = test_dates[0], test_dates[-1]
            test_start, test_end = test_s.isoformat(), test_e.isoformat()

    print(f"  Trades: {len(trades):,} rows")
    print(f"  Prices: {len(close_prices):,} days")
    print(f"  Train: {train_start} ~ {train_end} ({len(train_dates)} days)")
    print(f"  Test:  {test_start} ~ {test_end} ({len(test_dates)} days)")

    result = SignalResult(
        symbol=symbol,
        generated=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        train_start=train_start,
        train_end=train_end,
        test_start=test_start,
        test_end=test_end,
        n_train_days=len(train_dates),
        n_test_days=len(test_dates),
    )

    # --- Step 1: Detect large trades ---
    print("\n[Step 1] Detecting large trades...")
    broker_stats = detect_large_trades(trades, all_dates)
    result.n_brokers_total = len(broker_stats)
    result.n_brokers_with_large = sum(1 for bs in broker_stats.values() if bs.n_large > 0)
    print(f"  {result.n_brokers_total} brokers, {result.n_brokers_with_large} with large trades")

    # --- Step 2: Statistical validation ---
    print("\n[Step 2] Validating broker large-trade spreads...")
    broker_stats, should_continue = validate_brokers(
        broker_stats, trades, returns, all_dates,
    )
    result.n_brokers_significant = sum(
        1 for bs in broker_stats.values() if bs.large_tstat > 1.96
    )
    result.pct_significant = (
        result.n_brokers_significant / result.n_brokers_total
        if result.n_brokers_total > 0 else 0.0
    )

    if not should_continue:
        result.early_exit = (
            f"Insufficient significant brokers: "
            f"{result.n_brokers_significant}/{result.n_brokers_total} "
            f"({result.pct_significant:.1%} < 5%)"
        )
        print(f"\n  EARLY EXIT: {result.early_exit}")
        save_reports(symbol, result, paths)
        return result

    # --- Step 3: TA-weighted signal ---
    print("\n[Step 3] Building TA-weighted aggregate signal...")
    signal, _, should_continue = build_ta_signal(
        trades, broker_stats, all_dates,
        train_dates, test_dates, returns,
    )
    result.train_corr = 0.0
    result.train_tstat = 0.0
    result.test_corr = 0.0
    result.test_tstat = 0.0

    # Recompute for result
    train_set = set(train_dates)
    test_set = set(test_dates)
    train_signals, train_rets = [], []
    test_signals, test_rets = [], []
    for i, d in enumerate(all_dates[1:], 1):
        prev_d = all_dates[i - 1]
        sig = signal.get(prev_d, 0.0)
        ret = returns.get(d, 0.0)
        if d in train_set:
            train_signals.append(sig)
            train_rets.append(ret)
        if d in test_set:
            test_signals.append(sig)
            test_rets.append(ret)

    if len(train_signals) >= 10:
        s, r = np.array(train_signals), np.array(train_rets)
        c = float(np.corrcoef(s, r)[0, 1])
        n = len(s)
        result.train_corr = c
        result.train_tstat = c * ((n - 2) ** 0.5) / ((1 - c**2) ** 0.5) if abs(c) < 1 else 0.0

    if len(test_signals) >= 10:
        s, r = np.array(test_signals), np.array(test_rets)
        c = float(np.corrcoef(s, r)[0, 1])
        n = len(s)
        result.test_corr = c
        result.test_tstat = c * ((n - 2) ** 0.5) / ((1 - c**2) ** 0.5) if abs(c) < 1 else 0.0

    result.n_signal_long = sum(1 for d in test_dates if signal.get(d, 0) > 0)
    result.n_signal_short = sum(1 for d in test_dates if signal.get(d, 0) < 0)

    should_continue = abs(result.test_tstat) >= EARLY_EXIT_MIN_TSTAT

    if not should_continue:
        result.early_exit = (
            f"Test signal not significant: t={result.test_tstat:.2f} "
            f"(threshold: {EARLY_EXIT_MIN_TSTAT})"
        )
        print(f"\n  EARLY EXIT: {result.early_exit}")
        save_reports(symbol, result, paths)
        return result

    # --- Step 4: Backtest ---
    print("\n[Step 4] Running backtest...")
    bt = _domain_backtest(signal, ohlc, test_dates, cost=COST_PER_TRADE)

    result.total_return = bt.total_return
    result.sharpe = bt.sharpe
    result.max_dd = bt.max_dd
    result.calmar = bt.calmar
    result.n_long = bt.n_long
    result.n_short = bt.n_short
    result.avg_long_return = bt.avg_long_return
    result.avg_short_return = bt.avg_short_return
    result.win_rate_long = bt.win_rate_long
    result.win_rate_short = bt.win_rate_short
    result.bh_return = bt.bh_return
    result.bh_sharpe = bt.bh_sharpe
    result.bh_max_dd = bt.bh_max_dd
    result.monthly_returns = bt.monthly_returns

    print(f"\n  Strategy: {result.total_return:+.1%} (Sharpe {result.sharpe:.2f}, MaxDD {result.max_dd:+.1%})")
    print(f"  B&H:      {result.bh_return:+.1%} (Sharpe {result.bh_sharpe:.2f})")
    print(f"  Long:  {result.n_long} trades, avg {result.avg_long_return:+.2%}, win {result.win_rate_long:.0%}")
    print(f"  Short: {result.n_short} trades, avg {result.avg_short_return:+.2%}, win {result.win_rate_short:.0%}")

    # Save reports
    print("\n[Reports]")
    save_reports(symbol, result, paths)

    return result
