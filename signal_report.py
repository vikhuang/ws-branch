"""Signal Report: Large trade detection → statistical validation → TA-weighted backtest.

For a given stock symbol, this script:
1. Detects abnormal trading activity (>2σ from broker's mean)
2. Validates whether large trades predict next-day returns
3. Builds a TA-weighted aggregate signal using all brokers
4. Backtests the signal with realistic open→close returns and costs

Usage:
    uv run python signal_report.py 2345
    uv run python signal_report.py 2330 --train-start 2023-01-01 --train-end 2024-06-30
"""

import argparse
import json
import sys
from dataclasses import dataclass, field, asdict
from datetime import date, datetime
from pathlib import Path

import numpy as np
import polars as pl
from google.cloud import bigquery

# =============================================================================
# Constants
# =============================================================================

PROJECT_ID = "gen-lang-client-0998197473"
DATASET = "wsai"
TABLE = "tej_prices"

DATA_DIR = Path("data")
REPORTS_DIR = DATA_DIR / "reports"

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

def load_trade_data(symbol: str) -> pl.DataFrame:
    """Load daily_summary/{symbol}.parquet."""
    path = DATA_DIR / "daily_summary" / f"{symbol}.parquet"
    if not path.exists():
        print(f"Error: {path} not found. Run ETL first.")
        sys.exit(1)
    return pl.read_parquet(path)


def load_close_prices(symbol: str) -> dict[date, float]:
    """Extract symbol's close prices from close_prices.parquet."""
    path = DATA_DIR / "price" / "close_prices.parquet"
    if not path.exists():
        print(f"Error: {path} not found. Run sync_prices.py first.")
        sys.exit(1)
    df = pl.read_parquet(path)
    df = df.filter(pl.col("symbol_id") == symbol)
    prices = {}
    for row in df.iter_rows(named=True):
        d = row["date"]
        if isinstance(d, str):
            d = date.fromisoformat(d)
        prices[d] = float(row["close_price"])
    return prices


def fetch_ohlc(symbol: str) -> pl.DataFrame:
    """Fetch open+close from BigQuery, cache to data/price/{symbol}_ohlc.parquet."""
    cache_path = DATA_DIR / "price" / f"{symbol}_ohlc.parquet"
    if cache_path.exists():
        print(f"  Using cached OHLC: {cache_path}")
        return pl.read_parquet(cache_path)

    print(f"  Fetching OHLC from BigQuery for {symbol}...")
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
    SELECT mdate AS date, open_d AS open, close_d AS close
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE coid = '{symbol}' AND mdate >= '2021-01-01'
    ORDER BY mdate
    """
    rows = []
    for row in client.query(query).result():
        if row.open is not None and row.close is not None:
            rows.append({
                "date": row.date,
                "open": float(row.open),
                "close": float(row.close),
            })

    df = pl.DataFrame(rows)
    df = df.with_columns(pl.col("date").cast(pl.Date))
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(cache_path)
    print(f"  Saved OHLC cache: {cache_path} ({len(df)} rows)")
    return df


# =============================================================================
# Step 1: Detect Large Trades
# =============================================================================

def detect_large_trades(
    trades: pl.DataFrame,
    all_dates: list[date],
) -> dict[str, BrokerStats]:
    """Per broker: compute mean/std of net_buy, flag >2σ days.

    Returns dict keyed by broker code.
    """
    # Cast broker to string for consistency
    trades = trades.with_columns(pl.col("broker").cast(pl.Utf8))

    brokers = trades["broker"].unique().to_list()
    date_set = set(all_dates)

    # Build lookup: {(broker, date): net_buy}
    trade_lookup: dict[tuple[str, date], int] = {}
    for row in trades.iter_rows(named=True):
        d = row["date"]
        if d in date_set:
            nb = (row["buy_shares"] or 0) - (row["sell_shares"] or 0)
            trade_lookup[(row["broker"], d)] = nb

    stats = {}
    for broker in brokers:
        # Get net_buy series for all dates
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
            timing_alpha=0.0,  # computed from train period in Step 3
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
    Early exit if < 5% brokers have significant positive spread.
    """
    trades = trades.with_columns(pl.col("broker").cast(pl.Utf8))

    # Build lookup
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

            # Welch's t-test
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

    Updates broker_stats in-place. This avoids look-ahead bias from using
    full-period TA weights stored in pnl/{symbol}.parquet.

    Formula: TA = Σ((net_buy[t-1] - avg) × return[t]) / std(net_buy)
    """
    trades = trades.with_columns(pl.col("broker").cast(pl.Utf8))

    # Build trade lookup
    trade_lookup: dict[tuple[str, date], int] = {}
    for row in trades.iter_rows(named=True):
        nb = (row["buy_shares"] or 0) - (row["sell_shares"] or 0)
        trade_lookup[(str(row["broker"]), row["date"])] = nb

    for broker, bs in broker_stats.items():
        # Get net_buy series for training dates only
        nb_series = [trade_lookup.get((broker, d), 0) for d in train_dates]

        if len(nb_series) < 2:
            bs.timing_alpha = 0.0
            continue

        avg_nb = sum(nb_series) / len(nb_series)
        raw_ta = 0.0
        for t in range(1, len(nb_series)):
            ret = returns.get(train_dates[t], 0.0)
            raw_ta += (nb_series[t - 1] - avg_nb) * ret

        variance = sum((x - avg_nb) ** 2 for x in nb_series) / len(nb_series)
        std_nb = variance ** 0.5
        bs.timing_alpha = raw_ta / std_nb if std_nb > 0 else 0.0


def build_ta_signal(
    trades: pl.DataFrame,
    broker_stats: dict[str, BrokerStats],
    all_dates: list[date],
    train_dates: list[date],
    test_dates: list[date],
    returns: dict[date, float],
) -> tuple[dict[date, float], SignalResult | None, bool]:
    """Build daily signal = Σ(TA_b × dev_b[t] / σ_b) for |dev| > 2σ.

    TA weights are computed from training period only (no look-ahead).
    Returns (signal_dict, partial_result_for_early_exit, should_continue).
    """
    # Compute TA from training data only (avoid look-ahead bias)
    compute_train_ta(trades, broker_stats, train_dates, returns)

    trades = trades.with_columns(pl.col("broker").cast(pl.Utf8))

    # Build trade lookup
    trade_lookup: dict[tuple[str, date], int] = {}
    for row in trades.iter_rows(named=True):
        nb = (row["buy_shares"] or 0) - (row["sell_shares"] or 0)
        trade_lookup[(row["broker"], row["date"])] = nb

    # Compute daily signal for all dates
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

    # Evaluate on train period (signal[t-1] vs return[t])
    train_set = set(train_dates)
    train_signals = []
    train_returns = []
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

    # Evaluate on test period
    test_set = set(test_dates)
    test_signals = []
    test_returns = []
    for i, d in enumerate(all_dates[1:], 1):
        if d in test_set:
            prev_d = all_dates[i - 1]
            if prev_d in signal:
                test_signals.append(signal[prev_d])
                test_returns.append(returns.get(d, 0.0))

    test_corr, test_tstat = 0.0, 0.0
    if len(test_signals) >= 10:
        s_arr = np.array(test_signals)
        r_arr = np.array(test_returns)
        corr = float(np.corrcoef(s_arr, r_arr)[0, 1])
        n = len(s_arr)
        tstat = corr * ((n - 2) ** 0.5) / ((1 - corr**2) ** 0.5) if abs(corr) < 1 else 0.0
        test_corr, test_tstat = corr, tstat

    # Count signal days in test period
    n_long = sum(1 for d in test_dates if signal.get(d, 0) > 0)
    n_short = sum(1 for d in test_dates if signal.get(d, 0) < 0)

    print(f"  Train: corr={train_corr:.4f}, t={train_tstat:.2f} ({len(train_signals)} days)")
    print(f"  Test:  corr={test_corr:.4f}, t={test_tstat:.2f} ({len(test_signals)} days)")
    print(f"  Signal days (test): {n_long} long, {n_short} short")

    should_continue = abs(test_tstat) >= EARLY_EXIT_MIN_TSTAT

    return signal, None, should_continue


# =============================================================================
# Step 4: Backtest
# =============================================================================

def run_backtest(
    signal: dict[date, float],
    ohlc: pl.DataFrame,
    test_dates: list[date],
) -> dict:
    """Execute strategy using open→close returns with costs.

    Returns dict with all backtest metrics.
    """
    # Build OHLC lookup
    ohlc_lookup: dict[date, tuple[float, float]] = {}
    for row in ohlc.iter_rows(named=True):
        d = row["date"]
        if isinstance(d, str):
            d = date.fromisoformat(d)
        ohlc_lookup[d] = (float(row["open"]), float(row["close"]))

    # Strategy: signal[T] → trade on T+1 (open→close)
    long_returns = []
    short_returns = []
    daily_pnl = []  # For Sharpe/DD calculation
    dates_traded = []

    test_dates_sorted = sorted(test_dates)

    for i in range(len(test_dates_sorted) - 1):
        signal_date = test_dates_sorted[i]
        trade_date = test_dates_sorted[i + 1]

        sig = signal.get(signal_date, 0.0)
        if sig == 0.0:
            daily_pnl.append(0.0)
            dates_traded.append(trade_date)
            continue

        if trade_date not in ohlc_lookup:
            daily_pnl.append(0.0)
            dates_traded.append(trade_date)
            continue

        open_p, close_p = ohlc_lookup[trade_date]
        if open_p <= 0:
            daily_pnl.append(0.0)
            dates_traded.append(trade_date)
            continue

        raw_return = (close_p - open_p) / open_p

        if sig > 0:
            # Long: buy open, sell close
            net_return = raw_return - COST_PER_TRADE
            long_returns.append(net_return)
            daily_pnl.append(net_return)
        else:
            # Short: sell open, buy close
            net_return = -raw_return - COST_PER_TRADE
            short_returns.append(net_return)
            daily_pnl.append(net_return)

        dates_traded.append(trade_date)

    # Compute compounded return
    equity = [1.0]
    for r in daily_pnl:
        equity.append(equity[-1] * (1 + r))
    total_return = equity[-1] / equity[0] - 1

    # Sharpe (annualized, using all days including cash days)
    pnl_arr = np.array(daily_pnl)
    if len(pnl_arr) > 0 and np.std(pnl_arr) > 0:
        sharpe = float(np.mean(pnl_arr) / np.std(pnl_arr) * np.sqrt(252))
    else:
        sharpe = 0.0

    # Max drawdown
    equity_arr = np.array(equity)
    running_max = np.maximum.accumulate(equity_arr)
    drawdowns = (equity_arr - running_max) / running_max
    max_dd = float(np.min(drawdowns))

    # Calmar
    calmar = total_return / abs(max_dd) if max_dd != 0 else 0.0

    # Buy & Hold (compounded)
    bh_equity = [1.0]
    for i in range(len(test_dates_sorted) - 1):
        d = test_dates_sorted[i + 1]
        if d in ohlc_lookup:
            prev_d = test_dates_sorted[i]
            if prev_d in ohlc_lookup:
                prev_close = ohlc_lookup[prev_d][1]
                curr_close = ohlc_lookup[d][1]
                if prev_close > 0:
                    bh_equity.append(bh_equity[-1] * (curr_close / prev_close))
                else:
                    bh_equity.append(bh_equity[-1])
            else:
                bh_equity.append(bh_equity[-1])
        else:
            bh_equity.append(bh_equity[-1])

    bh_return = bh_equity[-1] / bh_equity[0] - 1

    # B&H Sharpe
    bh_daily = np.diff(np.log(np.array(bh_equity)))
    bh_daily = bh_daily[bh_daily != 0]
    bh_sharpe = float(np.mean(bh_daily) / np.std(bh_daily) * np.sqrt(252)) if len(bh_daily) > 0 and np.std(bh_daily) > 0 else 0.0

    # B&H Max DD
    bh_eq_arr = np.array(bh_equity)
    bh_running_max = np.maximum.accumulate(bh_eq_arr)
    bh_dd = (bh_eq_arr - bh_running_max) / bh_running_max
    bh_max_dd = float(np.min(bh_dd))

    # Monthly returns
    monthly = {}
    for i, d in enumerate(dates_traded):
        key = d.strftime("%Y-%m")
        if key not in monthly:
            monthly[key] = {"strategy": [], "bh": []}
        monthly[key]["strategy"].append(daily_pnl[i])

    # B&H monthly
    for i in range(1, len(test_dates_sorted)):
        d = test_dates_sorted[i]
        key = d.strftime("%Y-%m")
        if key not in monthly:
            monthly[key] = {"strategy": [], "bh": []}
        prev_d = test_dates_sorted[i - 1]
        if d in ohlc_lookup and prev_d in ohlc_lookup:
            prev_close = ohlc_lookup[prev_d][1]
            curr_close = ohlc_lookup[d][1]
            if prev_close > 0:
                monthly[key]["bh"].append(curr_close / prev_close - 1)

    monthly_returns = []
    for month in sorted(monthly.keys()):
        strat_rets = monthly[month]["strategy"]
        bh_rets = monthly[month]["bh"]
        # Compound
        strat_cum = 1.0
        for r in strat_rets:
            strat_cum *= (1 + r)
        bh_cum = 1.0
        for r in bh_rets:
            bh_cum *= (1 + r)
        monthly_returns.append({
            "month": month,
            "strategy": round((strat_cum - 1) * 100, 2),
            "bh": round((bh_cum - 1) * 100, 2),
        })

    # Win rates
    long_arr = np.array(long_returns) if long_returns else np.array([])
    short_arr = np.array(short_returns) if short_returns else np.array([])

    return {
        "total_return": total_return,
        "sharpe": sharpe,
        "max_dd": max_dd,
        "calmar": calmar,
        "n_long": len(long_returns),
        "n_short": len(short_returns),
        "avg_long_return": float(np.mean(long_arr)) if len(long_arr) > 0 else 0.0,
        "avg_short_return": float(np.mean(short_arr)) if len(short_arr) > 0 else 0.0,
        "win_rate_long": float(np.mean(long_arr > 0)) if len(long_arr) > 0 else 0.0,
        "win_rate_short": float(np.mean(short_arr > 0)) if len(short_arr) > 0 else 0.0,
        "bh_return": bh_return,
        "bh_sharpe": bh_sharpe,
        "bh_max_dd": bh_max_dd,
        "monthly_returns": monthly_returns,
    }


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


def save_reports(symbol: str, result: SignalResult) -> None:
    """Save both .md and .json to data/reports/."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    md_path = REPORTS_DIR / f"{symbol}.md"
    md_path.write_text(generate_markdown(symbol, result), encoding="utf-8")
    print(f"  Saved: {md_path}")

    json_path = REPORTS_DIR / f"{symbol}.json"
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
) -> SignalResult:
    """Execute full signal analysis pipeline."""
    print(f"\n{'='*60}")
    print(f"Signal Report: {symbol}")
    print(f"{'='*60}")

    # --- Step 0: Load data ---
    print("\n[Step 0] Loading data...")
    trades = load_trade_data(symbol)
    close_prices = load_close_prices(symbol)
    ohlc = fetch_ohlc(symbol)

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

    # Filter dates within data range
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

    # Initialize result
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
        save_reports(symbol, result)
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

    # Recompute for result (duplicated from build_ta_signal for clean reporting)
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
        save_reports(symbol, result)
        return result

    # --- Step 4: Backtest ---
    print("\n[Step 4] Running backtest...")
    bt = run_backtest(signal, ohlc, test_dates)

    result.total_return = bt["total_return"]
    result.sharpe = bt["sharpe"]
    result.max_dd = bt["max_dd"]
    result.calmar = bt["calmar"]
    result.n_long = bt["n_long"]
    result.n_short = bt["n_short"]
    result.avg_long_return = bt["avg_long_return"]
    result.avg_short_return = bt["avg_short_return"]
    result.win_rate_long = bt["win_rate_long"]
    result.win_rate_short = bt["win_rate_short"]
    result.bh_return = bt["bh_return"]
    result.bh_sharpe = bt["bh_sharpe"]
    result.bh_max_dd = bt["bh_max_dd"]
    result.monthly_returns = bt["monthly_returns"]

    print(f"\n  Strategy: {result.total_return:+.1%} (Sharpe {result.sharpe:.2f}, MaxDD {result.max_dd:+.1%})")
    print(f"  B&H:      {result.bh_return:+.1%} (Sharpe {result.bh_sharpe:.2f})")
    print(f"  Long:  {result.n_long} trades, avg {result.avg_long_return:+.2%}, win {result.win_rate_long:.0%}")
    print(f"  Short: {result.n_short} trades, avg {result.avg_short_return:+.2%}, win {result.win_rate_short:.0%}")

    # Save reports
    print("\n[Reports]")
    save_reports(symbol, result)

    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Signal Report: Large trade detection → TA-weighted backtest"
    )
    parser.add_argument("symbol", help="Stock symbol (e.g., 2345)")
    parser.add_argument("--train-start", default=DEFAULT_TRAIN_START)
    parser.add_argument("--train-end", default=DEFAULT_TRAIN_END)
    parser.add_argument("--test-start", default=DEFAULT_TEST_START)
    parser.add_argument("--test-end", default=DEFAULT_TEST_END)
    args = parser.parse_args()

    run_pipeline(
        args.symbol,
        train_start=args.train_start,
        train_end=args.train_end,
        test_start=args.test_start,
        test_end=args.test_end,
    )


if __name__ == "__main__":
    main()
