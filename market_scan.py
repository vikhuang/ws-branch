"""Market Scan: Full-market signal screening with FDR correction.

Screens ~2400 stocks through 5 filters:
  F0: Split/reverse-split detection
  F1: Liquidity (avg turnover > threshold)
  F2: Significant broker ratio > 5%
  F3: BH-FDR corrected OOS t-stat
  F4: Full backtest with conservative costs

Usage:
    uv run python market_scan.py
    uv run python market_scan.py --min-turnover 200000000 --cost 0.005 --fdr 0.01
"""

import argparse
import json
import math
import sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
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
DEFAULT_COST = 0.005  # 0.50% (commission + tax + slippage)
DEFAULT_MIN_TURNOVER = 2e8  # 200M NTD
MIN_SIGNIFICANT_PCT = 0.05  # 5%
MIN_TEST_DAYS = 250
DEFAULT_FDR = 0.01  # 1%

DEFAULT_TRAIN_START = "2023-01-01"
DEFAULT_TRAIN_END = "2024-06-30"
DEFAULT_TEST_START = "2024-07-01"
DEFAULT_TEST_END = "2025-12-31"

SPLIT_TOLERANCE = 0.05
SPLIT_RATIOS = [0.5, 1 / 3, 0.25, 0.2, 0.1, 2.0, 3.0, 4.0, 5.0, 10.0]

DEFAULT_WORKERS = 12


# =============================================================================
# Config
# =============================================================================


@dataclass
class ScanConfig:
    min_turnover: float = DEFAULT_MIN_TURNOVER
    cost: float = DEFAULT_COST
    fdr_threshold: float = DEFAULT_FDR
    min_test_days: int = MIN_TEST_DAYS
    train_start: str = DEFAULT_TRAIN_START
    train_end: str = DEFAULT_TRAIN_END
    test_start: str = DEFAULT_TEST_START
    test_end: str = DEFAULT_TEST_END
    workers: int = DEFAULT_WORKERS

    def to_dict(self) -> dict:
        return {
            "min_turnover": self.min_turnover,
            "cost": self.cost,
            "fdr_threshold": self.fdr_threshold,
            "min_test_days": self.min_test_days,
            "train_start": self.train_start,
            "train_end": self.train_end,
            "test_start": self.test_start,
            "test_end": self.test_end,
        }


# =============================================================================
# Utility Functions
# =============================================================================


def detect_split(prices: dict[date, float]) -> bool:
    """Check if price series contains split/reverse-split events."""
    sorted_dates = sorted(prices.keys())
    for i in range(1, len(sorted_dates)):
        prev = prices[sorted_dates[i - 1]]
        curr = prices[sorted_dates[i]]
        if prev <= 0:
            continue
        ratio = curr / prev
        for known in SPLIT_RATIOS:
            if abs(ratio - known) / known < SPLIT_TOLERANCE:
                return True
    return False


def tstat_to_pvalue(t: float) -> float:
    """Two-tailed p-value from t-stat using normal approximation (valid for df > 30)."""
    return math.erfc(abs(t) / math.sqrt(2))


def benjamini_hochberg(
    symbols_pvalues: list[tuple[str, float]], alpha: float
) -> list[str]:
    """BH-FDR correction. Returns list of passing symbols sorted by p-value."""
    if not symbols_pvalues:
        return []
    sorted_sp = sorted(symbols_pvalues, key=lambda x: x[1])
    n = len(sorted_sp)
    max_k = 0
    for k in range(1, n + 1):
        if sorted_sp[k - 1][1] <= alpha * k / n:
            max_k = k
    return [sorted_sp[i][0] for i in range(max_k)]


# =============================================================================
# Core Analysis (shared by Phase 1 and Phase 2)
# =============================================================================


def analyze_symbol(
    symbol: str,
    prices: dict[date, float],
    summary_dir: str,
    config: dict,
) -> dict:
    """Run Filters 0-3a for a symbol.

    Returns dict with:
        passed, filter_stage, reason, metrics,
        signal/test_dates/train_dates (only if passed)
    """
    train_start = date.fromisoformat(config["train_start"])
    train_end = date.fromisoformat(config["train_end"])
    test_start = date.fromisoformat(config["test_start"])
    test_end = date.fromisoformat(config["test_end"])
    min_turnover = config["min_turnover"]
    min_test_days = config["min_test_days"]

    base = {
        "symbol": symbol,
        "passed": False,
        "filter_stage": 0,
        "reason": "",
        "n_brokers": 0,
        "n_significant": 0,
        "pct_significant": 0.0,
        "train_corr": 0.0,
        "train_tstat": 0.0,
        "test_corr": 0.0,
        "test_tstat": 0.0,
        "test_pvalue": 1.0,
        "avg_turnover": 0.0,
        "n_train_days": 0,
        "n_test_days": 0,
    }

    # --- Filter 0: Split detection ---
    if detect_split(prices):
        base["reason"] = "split/reverse-split detected"
        return base

    # Setup dates
    sorted_dates = sorted(prices.keys())
    all_dates = [d for d in sorted_dates if train_start <= d <= test_end]
    train_dates = [d for d in all_dates if train_start <= d <= train_end]
    test_dates = [d for d in all_dates if test_start <= d <= test_end]

    base["n_train_days"] = len(train_dates)
    base["n_test_days"] = len(test_dates)

    if len(train_dates) < 30:
        base["reason"] = f"insufficient train data ({len(train_dates)} days)"
        return base
    if len(test_dates) < min_test_days:
        base["reason"] = (
            f"insufficient test data ({len(test_dates)} < {min_test_days})"
        )
        return base

    # Compute returns
    returns: dict[date, float] = {}
    for i in range(1, len(sorted_dates)):
        prev_p = prices[sorted_dates[i - 1]]
        curr_p = prices[sorted_dates[i]]
        if prev_p > 0:
            returns[sorted_dates[i]] = (curr_p - prev_p) / prev_p

    # Load trades
    path = Path(summary_dir) / f"{symbol}.parquet"
    if not path.exists():
        base["reason"] = "no daily_summary data"
        return base
    trades = pl.read_parquet(path)
    trades = trades.with_columns(pl.col("broker").cast(pl.Utf8))

    # --- Filter 1: Liquidity ---
    train_set = set(train_dates)
    daily_vol: dict[date, int] = {}
    for row in trades.iter_rows(named=True):
        d = row["date"]
        if d in train_set:
            daily_vol[d] = daily_vol.get(d, 0) + (row["buy_shares"] or 0)

    turnovers = []
    for d, vol in daily_vol.items():
        p = prices.get(d, 0.0)
        if p > 0:
            turnovers.append(vol * p)
    avg_turnover = sum(turnovers) / len(turnovers) if turnovers else 0.0
    base["avg_turnover"] = avg_turnover

    if avg_turnover < min_turnover:
        base["filter_stage"] = 1
        base["reason"] = (
            f"low turnover ({avg_turnover / 1e8:.1f}億 < {min_turnover / 1e8:.0f}億)"
        )
        return base

    # --- Filter 2: Large trade detection + broker validation ---
    date_set = set(all_dates)
    trade_lookup: dict[tuple[str, date], int] = {}
    for row in trades.iter_rows(named=True):
        d = row["date"]
        if d in date_set:
            nb = (row["buy_shares"] or 0) - (row["sell_shares"] or 0)
            trade_lookup[(row["broker"], d)] = nb

    brokers = list({k[0] for k in trade_lookup.keys()})

    # Per broker: compute mean/std of net_buy
    broker_stats: dict[str, dict] = {}
    for broker in brokers:
        nb_series = np.array(
            [trade_lookup.get((broker, d), 0) for d in all_dates], dtype=np.float64
        )
        mean_nb = float(np.mean(nb_series))
        std_nb = float(np.std(nb_series))
        if std_nb == 0:
            continue
        n_large = int(
            np.sum(np.abs(nb_series - mean_nb) > SIGMA_THRESHOLD * std_nb)
        )
        broker_stats[broker] = {
            "mean": mean_nb,
            "std": std_nb,
            "n_large": n_large,
            "ta": 0.0,
        }

    n_brokers = len(broker_stats)
    base["n_brokers"] = n_brokers

    if n_brokers == 0:
        base["filter_stage"] = 2
        base["reason"] = "no valid brokers"
        return base

    # Validate: large-trade spread + Welch's t-test
    n_significant = 0
    for broker, bs in broker_stats.items():
        large_rets: list[float] = []
        non_large_rets: list[float] = []
        for i, d in enumerate(all_dates[:-1]):
            nb = trade_lookup.get((broker, d), 0)
            next_ret = returns.get(all_dates[i + 1], 0.0)
            dev = abs(nb - bs["mean"])
            if dev > SIGMA_THRESHOLD * bs["std"]:
                large_rets.append(next_ret)
            else:
                non_large_rets.append(next_ret)

        if len(large_rets) >= 3 and len(non_large_rets) >= 3:
            la = np.array(large_rets)
            nla = np.array(non_large_rets)
            spread = float(np.mean(la) - np.mean(nla))
            n1, n2 = len(la), len(nla)
            s1 = float(np.std(la, ddof=1))
            s2 = float(np.std(nla, ddof=1))
            se = ((s1**2 / n1) + (s2**2 / n2)) ** 0.5 if (s1 > 0 or s2 > 0) else 0.0
            tstat = spread / se if se > 0 else 0.0
            if tstat > 1.96:
                n_significant += 1

    pct_sig = n_significant / n_brokers
    base["n_significant"] = n_significant
    base["pct_significant"] = pct_sig

    if pct_sig < MIN_SIGNIFICANT_PCT:
        base["filter_stage"] = 2
        base["reason"] = (
            f"insufficient significant brokers ({pct_sig:.1%} < 5%)"
        )
        return base

    # --- Filter 3a: TA-weighted signal ---
    # Compute TA from train period only (avoid look-ahead bias)
    for broker, bs in broker_stats.items():
        nb_train = [trade_lookup.get((broker, d), 0) for d in train_dates]
        if len(nb_train) < 2:
            continue
        avg_nb = sum(nb_train) / len(nb_train)
        raw_ta = 0.0
        for t in range(1, len(nb_train)):
            ret = returns.get(train_dates[t], 0.0)
            raw_ta += (nb_train[t - 1] - avg_nb) * ret
        variance = sum((x - avg_nb) ** 2 for x in nb_train) / len(nb_train)
        std_nb = variance**0.5
        bs["ta"] = raw_ta / std_nb if std_nb > 0 else 0.0

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

    # Evaluate correlation on train and test
    test_set = set(test_dates)
    train_sigs: list[float] = []
    train_rets: list[float] = []
    test_sigs: list[float] = []
    test_rets: list[float] = []
    for i, d in enumerate(all_dates[1:], 1):
        prev_d = all_dates[i - 1]
        sig = signal.get(prev_d, 0.0)
        ret = returns.get(d, 0.0)
        if d in train_set:
            train_sigs.append(sig)
            train_rets.append(ret)
        if d in test_set:
            test_sigs.append(sig)
            test_rets.append(ret)

    train_corr = train_tstat = 0.0
    if len(train_sigs) >= 10:
        s_arr, r_arr = np.array(train_sigs), np.array(train_rets)
        c = float(np.corrcoef(s_arr, r_arr)[0, 1])
        if not np.isnan(c) and abs(c) < 1:
            n = len(s_arr)
            train_tstat = c * ((n - 2) ** 0.5) / ((1 - c**2) ** 0.5)
            train_corr = c

    test_corr = test_tstat = 0.0
    test_pvalue = 1.0
    if len(test_sigs) >= 10:
        s_arr, r_arr = np.array(test_sigs), np.array(test_rets)
        c = float(np.corrcoef(s_arr, r_arr)[0, 1])
        if not np.isnan(c) and abs(c) < 1:
            n = len(s_arr)
            test_tstat = c * ((n - 2) ** 0.5) / ((1 - c**2) ** 0.5)
            test_corr = c
            test_pvalue = tstat_to_pvalue(test_tstat)

    base.update(
        {
            "passed": True,
            "filter_stage": 3,
            "reason": "passed filters 0-3a",
            "train_corr": train_corr,
            "train_tstat": train_tstat,
            "test_corr": test_corr,
            "test_tstat": test_tstat,
            "test_pvalue": test_pvalue,
            "signal": signal,
            "test_dates": test_dates,
            "train_dates": train_dates,
        }
    )
    return base


# =============================================================================
# Backtest (adapted from signal_report.py with configurable cost)
# =============================================================================


def run_backtest(
    signal: dict[date, float],
    ohlc: pl.DataFrame,
    test_dates: list[date],
    cost: float,
) -> dict:
    """Execute strategy: signal[T] → trade on T+1 (open→close) with costs."""
    # Build OHLC lookup
    ohlc_lookup: dict[date, tuple[float, float]] = {}
    for row in ohlc.iter_rows(named=True):
        d = row["date"]
        if isinstance(d, str):
            d = date.fromisoformat(d)
        ohlc_lookup[d] = (float(row["open"]), float(row["close"]))

    long_returns: list[float] = []
    short_returns: list[float] = []
    daily_pnl: list[float] = []
    dates_traded: list[date] = []

    test_sorted = sorted(test_dates)

    for i in range(len(test_sorted) - 1):
        signal_date = test_sorted[i]
        trade_date = test_sorted[i + 1]
        sig = signal.get(signal_date, 0.0)

        if sig == 0.0 or trade_date not in ohlc_lookup:
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
            net_return = raw_return - cost
            long_returns.append(net_return)
        else:
            net_return = -raw_return - cost
            short_returns.append(net_return)

        daily_pnl.append(net_return)
        dates_traded.append(trade_date)

    # Compounded return
    equity = [1.0]
    for r in daily_pnl:
        equity.append(equity[-1] * (1 + r))
    total_return = equity[-1] / equity[0] - 1

    # Sharpe (annualized)
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

    # Buy & Hold
    bh_equity = [1.0]
    for i in range(len(test_sorted) - 1):
        d = test_sorted[i + 1]
        prev_d = test_sorted[i]
        if d in ohlc_lookup and prev_d in ohlc_lookup:
            prev_close = ohlc_lookup[prev_d][1]
            curr_close = ohlc_lookup[d][1]
            if prev_close > 0:
                bh_equity.append(bh_equity[-1] * (curr_close / prev_close))
            else:
                bh_equity.append(bh_equity[-1])
        else:
            bh_equity.append(bh_equity[-1])

    bh_return = bh_equity[-1] / bh_equity[0] - 1
    bh_eq_arr = np.array(bh_equity)
    bh_daily = np.diff(bh_eq_arr) / bh_eq_arr[:-1]
    bh_sharpe = (
        float(np.mean(bh_daily) / np.std(bh_daily) * np.sqrt(252))
        if len(bh_daily) > 0 and np.std(bh_daily) > 0
        else 0.0
    )
    bh_running_max = np.maximum.accumulate(bh_eq_arr)
    bh_dd = (bh_eq_arr - bh_running_max) / bh_running_max
    bh_max_dd = float(np.min(bh_dd))

    # Monthly returns
    monthly: dict[str, dict[str, list]] = {}
    for i, d in enumerate(dates_traded):
        key = d.strftime("%Y-%m")
        if key not in monthly:
            monthly[key] = {"strategy": [], "bh": []}
        monthly[key]["strategy"].append(daily_pnl[i])

    for i in range(1, len(test_sorted)):
        d = test_sorted[i]
        key = d.strftime("%Y-%m")
        if key not in monthly:
            monthly[key] = {"strategy": [], "bh": []}
        prev_d = test_sorted[i - 1]
        if d in ohlc_lookup and prev_d in ohlc_lookup:
            prev_close = ohlc_lookup[prev_d][1]
            curr_close = ohlc_lookup[d][1]
            if prev_close > 0:
                monthly[key]["bh"].append(curr_close / prev_close - 1)

    monthly_returns = []
    for month in sorted(monthly.keys()):
        strat_cum = 1.0
        for r in monthly[month]["strategy"]:
            strat_cum *= 1 + r
        bh_cum = 1.0
        for r in monthly[month]["bh"]:
            bh_cum *= 1 + r
        monthly_returns.append(
            {
                "month": month,
                "strategy": round((strat_cum - 1) * 100, 2),
                "bh": round((bh_cum - 1) * 100, 2),
            }
        )

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
# Phase 1 Worker
# =============================================================================


def scan_phase1(
    symbol: str,
    prices: dict[date, float],
    summary_dir: str,
    config: dict,
) -> dict:
    """Phase 1: run Filters 0-3a, return lightweight result."""
    try:
        result = analyze_symbol(symbol, prices, summary_dir, config)
        # Strip large fields not needed for IPC
        result.pop("signal", None)
        result.pop("test_dates", None)
        result.pop("train_dates", None)
        return result
    except Exception as e:
        return {
            "symbol": symbol,
            "passed": False,
            "filter_stage": 0,
            "reason": f"error: {e}",
            "n_brokers": 0,
            "n_significant": 0,
            "pct_significant": 0.0,
            "train_corr": 0.0,
            "train_tstat": 0.0,
            "test_corr": 0.0,
            "test_tstat": 0.0,
            "test_pvalue": 1.0,
            "avg_turnover": 0.0,
            "n_train_days": 0,
            "n_test_days": 0,
        }


# =============================================================================
# Phase 2: OHLC Batch Fetch
# =============================================================================


def fetch_ohlc_batch(symbols: list[str]) -> dict[str, pl.DataFrame]:
    """Fetch OHLC for multiple symbols in a single BigQuery query.

    Caches per-symbol to data/price/{symbol}_ohlc.parquet.
    """
    cache_dir = DATA_DIR / "price"
    cache_dir.mkdir(parents=True, exist_ok=True)

    result: dict[str, pl.DataFrame] = {}
    to_fetch: list[str] = []

    for s in symbols:
        cache = cache_dir / f"{s}_ohlc.parquet"
        if cache.exists():
            result[s] = pl.read_parquet(cache)
        else:
            to_fetch.append(s)

    if to_fetch:
        print(f"  Fetching OHLC for {len(to_fetch)} symbols from BigQuery...")
        client = bigquery.Client(project=PROJECT_ID)
        symbols_str = ",".join(f"'{s}'" for s in to_fetch)
        query = f"""
        SELECT coid AS symbol, mdate AS date, open_d AS open, close_d AS close
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE coid IN ({symbols_str}) AND mdate >= '2021-01-01'
        ORDER BY coid, mdate
        """
        rows_by_symbol: dict[str, list[dict]] = defaultdict(list)
        for row in client.query(query).result():
            if row.open is not None and row.close is not None:
                rows_by_symbol[row.symbol].append(
                    {
                        "date": row.date,
                        "open": float(row.open),
                        "close": float(row.close),
                    }
                )

        for s in to_fetch:
            if rows_by_symbol[s]:
                df = pl.DataFrame(rows_by_symbol[s]).with_columns(
                    pl.col("date").cast(pl.Date)
                )
                cache = cache_dir / f"{s}_ohlc.parquet"
                df.write_parquet(cache)
                result[s] = df
                print(f"    Cached: {s} ({len(df)} rows)")

    return result


# =============================================================================
# Phase 2 Worker
# =============================================================================


def scan_phase2(
    symbol: str,
    prices: dict[date, float],
    price_dir: str,
    summary_dir: str,
    config: dict,
) -> dict | None:
    """Phase 2: rebuild signal + full backtest + save per-stock report."""
    try:
        result = analyze_symbol(symbol, prices, summary_dir, config)
        if not result.get("passed"):
            return None

        # Load OHLC from cache
        ohlc_path = Path(price_dir) / f"{symbol}_ohlc.parquet"
        if not ohlc_path.exists():
            return None
        ohlc = pl.read_parquet(ohlc_path)

        # Run backtest
        bt = run_backtest(
            result["signal"], ohlc, result["test_dates"], config["cost"]
        )

        # Count signal days
        signal = result["signal"]
        test_dates = result["test_dates"]
        n_signal_long = sum(1 for d in test_dates if signal.get(d, 0) > 0)
        n_signal_short = sum(1 for d in test_dates if signal.get(d, 0) < 0)

        # Build per-stock report (same format as signal_report.py)
        report = {
            "symbol": symbol,
            "generated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "train_start": config["train_start"],
            "train_end": config["train_end"],
            "test_start": config["test_start"],
            "test_end": config["test_end"],
            "n_train_days": result["n_train_days"],
            "n_test_days": result["n_test_days"],
            "n_brokers_total": result["n_brokers"],
            "n_brokers_with_large": result["n_brokers"],
            "n_brokers_significant": result["n_significant"],
            "pct_significant": result["pct_significant"],
            "train_corr": result["train_corr"],
            "train_tstat": result["train_tstat"],
            "test_corr": result["test_corr"],
            "test_tstat": result["test_tstat"],
            "n_signal_long": n_signal_long,
            "n_signal_short": n_signal_short,
            **bt,
            "early_exit": None,
        }

        # Save per-stock JSON report
        reports_dir = Path("data/reports")
        reports_dir.mkdir(parents=True, exist_ok=True)
        (reports_dir / f"{symbol}.json").write_text(
            json.dumps(report, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )

        return {
            "symbol": symbol,
            **bt,
            "test_tstat": result["test_tstat"],
            "test_pvalue": result["test_pvalue"],
            "train_corr": result["train_corr"],
            "train_tstat": result["train_tstat"],
            "test_corr": result["test_corr"],
            "n_brokers": result["n_brokers"],
            "n_significant": result["n_significant"],
            "pct_significant": result["pct_significant"],
            "n_train_days": result["n_train_days"],
            "n_test_days": result["n_test_days"],
            "n_signal_long": n_signal_long,
            "n_signal_short": n_signal_short,
        }
    except Exception as e:
        print(f"  Error in Phase 2 for {symbol}: {e}")
        return None


# =============================================================================
# Report Generation
# =============================================================================


def save_market_scan(
    config: ScanConfig,
    phase1_results: list[dict],
    phase2_results: list[dict],
) -> None:
    """Save market_scan.json and market_scan.md to data/derived/."""
    derived_dir = DATA_DIR / "derived"
    derived_dir.mkdir(parents=True, exist_ok=True)

    # Count filter funnel (F0 split into sub-categories)
    total = len(phase1_results)
    f0_split = sum(
        1 for r in phase1_results
        if r["filter_stage"] == 0 and "split" in r["reason"]
    )
    f0_data = sum(
        1 for r in phase1_results
        if r["filter_stage"] == 0 and "split" not in r["reason"]
    )
    f0a_pass = total - f0_split  # after removing splits
    f0b_pass = f0a_pass - f0_data  # after removing data-insufficient
    f1_pass = sum(1 for r in phase1_results if r["filter_stage"] >= 2 or r["passed"])
    f2_pass = sum(1 for r in phase1_results if r["passed"])
    f3_pass = len(phase2_results)

    funnel = {
        "universe": total,
        "f0a_no_split": f0a_pass,
        "f0b_sufficient_data": f0b_pass,
        "f1_liquidity": f1_pass,
        "f2_signal_quality": f2_pass,
        "f3_fdr": f3_pass,
    }

    # Sort results by Sharpe descending
    sorted_results = sorted(phase2_results, key=lambda x: x["sharpe"], reverse=True)

    # JSON report
    json_report = {
        "config": config.to_dict(),
        "generated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "filter_funnel": funnel,
        "results": sorted_results,
    }
    json_path = derived_dir / "market_scan.json"
    json_path.write_text(
        json.dumps(json_report, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    print(f"  Saved: {json_path}")

    # Markdown report
    lines = [
        "# Market Scan Report",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## Config",
        "",
        f"- Train: {config.train_start} ~ {config.train_end}",
        f"- Test: {config.test_start} ~ {config.test_end}",
        f"- Min turnover: {config.min_turnover / 1e8:.0f}億 NTD",
        f"- Cost: {config.cost:.2%}",
        f"- FDR: {config.fdr_threshold:.0%}",
        "",
        "## Filter Funnel",
        "",
        "| Stage | Count | Description |",
        "|-------|-------|-------------|",
        f"| Universe | {total} | Stocks with price data |",
        f"| F0a: No split | {f0a_pass} | Exclude {f0_split} splits/reverse-splits |",
        f"| F0b: Data | {f0b_pass} | Train ≥ 30 days, Test ≥ {config.min_test_days} days |",
        f"| F1: Liquidity | {f1_pass} | Avg turnover > {config.min_turnover / 1e8:.0f}億 |",
        f"| F2: Signal | {f2_pass} | >5% significant brokers |",
        f"| F3: FDR | {f3_pass} | BH-FDR < {config.fdr_threshold:.0%} |",
        "",
    ]

    if sorted_results:
        lines += [
            "## Results (sorted by Sharpe)",
            "",
            "| Rank | Symbol | Sharpe | Return | MaxDD | Calmar | Test t |",
            "|------|--------|--------|--------|-------|--------|--------|",
        ]
        for i, r in enumerate(sorted_results, 1):
            lines.append(
                f"| {i} | {r['symbol']} "
                f"| {r['sharpe']:.2f} "
                f"| {r['total_return']:+.1%} "
                f"| {r['max_dd']:+.1%} "
                f"| {r['calmar']:.1f} "
                f"| {r['test_tstat']:+.1f} |"
            )
        lines.append("")
    else:
        lines += ["## Results", "", "No stocks passed all filters.", ""]

    md_path = derived_dir / "market_scan.md"
    md_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  Saved: {md_path}")


# =============================================================================
# Main
# =============================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Market Scan: Full-market signal screening with FDR correction"
    )
    parser.add_argument(
        "--min-turnover",
        type=float,
        default=DEFAULT_MIN_TURNOVER,
        help=f"Min avg daily turnover in NTD (default: {DEFAULT_MIN_TURNOVER:.0f})",
    )
    parser.add_argument(
        "--cost",
        type=float,
        default=DEFAULT_COST,
        help=f"Cost per trade (default: {DEFAULT_COST})",
    )
    parser.add_argument(
        "--fdr",
        type=float,
        default=DEFAULT_FDR,
        help=f"FDR threshold (default: {DEFAULT_FDR})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Number of parallel workers (default: {DEFAULT_WORKERS})",
    )
    parser.add_argument("--train-start", default=DEFAULT_TRAIN_START)
    parser.add_argument("--train-end", default=DEFAULT_TRAIN_END)
    parser.add_argument("--test-start", default=DEFAULT_TEST_START)
    parser.add_argument("--test-end", default=DEFAULT_TEST_END)
    args = parser.parse_args()

    config = ScanConfig(
        min_turnover=args.min_turnover,
        cost=args.cost,
        fdr_threshold=args.fdr,
        min_test_days=MIN_TEST_DAYS,
        train_start=args.train_start,
        train_end=args.train_end,
        test_start=args.test_start,
        test_end=args.test_end,
        workers=args.workers,
    )
    config_dict = config.to_dict()

    print("=" * 60)
    print("Market Scan")
    print("=" * 60)
    print(f"  Train: {config.train_start} ~ {config.train_end}")
    print(f"  Test:  {config.test_start} ~ {config.test_end}")
    print(f"  Min turnover: {config.min_turnover / 1e8:.0f}億 NTD")
    print(f"  Cost: {config.cost:.2%}")
    print(f"  FDR: {config.fdr_threshold:.0%}")
    print(f"  Workers: {config.workers}")

    # ---- Load close prices and partition by symbol ----
    print("\n[Step 1] Loading close prices...")
    prices_path = DATA_DIR / "price" / "close_prices.parquet"
    if not prices_path.exists():
        print(f"Error: {prices_path} not found. Run sync_prices.py first.")
        sys.exit(1)

    prices_df = pl.read_parquet(prices_path)
    prices_by_sym: dict[str, dict[date, float]] = defaultdict(dict)
    for row in prices_df.iter_rows(named=True):
        d = row["date"]
        if isinstance(d, str):
            d = date.fromisoformat(d)
        prices_by_sym[row["symbol_id"]][d] = float(row["close_price"])

    # Get symbols with both prices and daily_summary
    summary_dir = str(DATA_DIR / "daily_summary")
    summary_symbols = {p.stem for p in Path(summary_dir).glob("*.parquet")}
    symbols = sorted(summary_symbols & set(prices_by_sym.keys()))
    print(f"  {len(prices_by_sym)} symbols with prices")
    print(f"  {len(summary_symbols)} symbols with daily_summary")
    print(f"  {len(symbols)} symbols to scan")

    # ---- Phase 1: Parallel screening ----
    print(f"\n[Step 2] Phase 1: Screening {len(symbols)} symbols...")
    phase1_results: list[dict] = []
    completed = 0

    with ProcessPoolExecutor(max_workers=config.workers) as executor:
        futures = {
            executor.submit(
                scan_phase1, symbol, prices_by_sym[symbol], summary_dir, config_dict
            ): symbol
            for symbol in symbols
        }

        for future in as_completed(futures):
            completed += 1
            if completed % 200 == 0 or completed == len(symbols):
                print(f"  {completed}/{len(symbols)} symbols done")
            phase1_results.append(future.result())

    # Phase 1 summary
    n_split = sum(
        1 for r in phase1_results
        if r["filter_stage"] == 0 and "split" in r["reason"]
    )
    n_data = sum(
        1 for r in phase1_results
        if r["filter_stage"] == 0 and "split" not in r["reason"]
    )
    n_f1 = sum(1 for r in phase1_results if r["filter_stage"] == 1)
    n_f2 = sum(1 for r in phase1_results if r["filter_stage"] == 2)

    passed_phase1 = [r for r in phase1_results if r["passed"]]
    print(f"\n  Filter funnel:")
    print(f"    F0a (split):     {n_split} filtered")
    print(f"    F0b (data):      {n_data} filtered")
    print(f"    F1 (liquidity):  {n_f1} filtered")
    print(f"    F2 (brokers):    {n_f2} filtered")
    print(f"    Passed F0-F2:    {len(passed_phase1)} symbols")

    # ---- FDR correction (Filter 3) ----
    print(f"\n[Step 3] BH-FDR correction (α={config.fdr_threshold:.0%})...")
    symbols_pvalues = [(r["symbol"], r["test_pvalue"]) for r in passed_phase1]
    fdr_passing = benjamini_hochberg(symbols_pvalues, config.fdr_threshold)
    print(f"  {len(fdr_passing)} symbols pass FDR correction")

    if not fdr_passing:
        print("\nNo symbols passed FDR. Saving empty report.")
        save_market_scan(config, phase1_results, [])
        return

    # Show FDR passing symbols
    for sym in fdr_passing:
        r = next(r for r in passed_phase1 if r["symbol"] == sym)
        print(
            f"    {sym}: t={r['test_tstat']:+.2f}, p={r['test_pvalue']:.4f}, "
            f"brokers={r['n_brokers']}, sig={r['pct_significant']:.1%}"
        )

    # ---- Phase 2: OHLC fetch + backtest ----
    print(f"\n[Step 4] Phase 2: Backtest for {len(fdr_passing)} symbols...")

    # Batch fetch OHLC
    ohlc_data = fetch_ohlc_batch(fdr_passing)
    missing_ohlc = [s for s in fdr_passing if s not in ohlc_data]
    if missing_ohlc:
        print(f"  Warning: no OHLC data for {missing_ohlc}")

    price_dir = str(DATA_DIR / "price")
    phase2_results: list[dict] = []
    completed = 0

    with ProcessPoolExecutor(max_workers=config.workers) as executor:
        futures = {
            executor.submit(
                scan_phase2,
                symbol,
                prices_by_sym[symbol],
                price_dir,
                summary_dir,
                config_dict,
            ): symbol
            for symbol in fdr_passing
            if symbol in ohlc_data
        }

        for future in as_completed(futures):
            completed += 1
            symbol = futures[future]
            result = future.result()
            if result:
                phase2_results.append(result)
                print(
                    f"  {symbol}: Sharpe={result['sharpe']:.2f}, "
                    f"Return={result['total_return']:+.1%}, "
                    f"MaxDD={result['max_dd']:+.1%}"
                )

    # ---- Save reports ----
    print(f"\n[Step 5] Saving reports...")
    save_market_scan(config, phase1_results, phase2_results)

    # Final summary
    print(f"\n{'=' * 60}")
    print(f"Market Scan Complete")
    print(f"{'=' * 60}")
    print(f"  Universe: {len(symbols)} stocks")
    print(f"  FDR passing: {len(fdr_passing)}")
    print(f"  Backtested: {len(phase2_results)}")
    if phase2_results:
        sorted_r = sorted(phase2_results, key=lambda x: x["sharpe"], reverse=True)
        print(f"\n  Top results:")
        for r in sorted_r[:10]:
            print(
                f"    {r['symbol']}: Sharpe={r['sharpe']:.2f}, "
                f"Return={r['total_return']:+.1%}, "
                f"MaxDD={r['max_dd']:+.1%}, "
                f"t={r['test_tstat']:+.1f}"
            )


if __name__ == "__main__":
    main()
