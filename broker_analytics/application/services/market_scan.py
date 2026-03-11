"""Market Scan: Full-market signal screening with FDR correction.

Screens ~2400 stocks through 6 filters:
  F0a: ETF/ETN exclusion (symbol starts with "00")
  F0b: Split/reverse-split detection
  F0c: Sufficient historical data
  F1: Liquidity (avg turnover > threshold)
  F2: Significant broker ratio > 5%
  F3: BH-FDR corrected OOS t-stat + full backtest
"""

import json
import sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import numpy as np
import polars as pl

from broker_analytics.domain.backtest import run_backtest as _domain_backtest
from broker_analytics.domain.statistics import tstat_to_pvalue, benjamini_hochberg
from broker_analytics.infrastructure.config import DataPaths, DEFAULT_PATHS
from broker_analytics.infrastructure.repositories.price_repo import PriceRepository

# =============================================================================
# Constants
# =============================================================================

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

    # --- Filter 0: ETF exclusion ---
    if symbol.startswith("00"):
        base["reason"] = "ETF/ETN excluded"
        return base

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

        ohlc_path = Path(price_dir) / f"{symbol}_ohlc.parquet"
        if not ohlc_path.exists():
            return None
        ohlc = pl.read_parquet(ohlc_path)

        # Run backtest using domain function
        bt = _domain_backtest(
            result["signal"], ohlc, result["test_dates"], cost=config["cost"]
        )

        signal = result["signal"]
        test_dates = result["test_dates"]
        n_signal_long = sum(1 for d in test_dates if signal.get(d, 0) > 0)
        n_signal_short = sum(1 for d in test_dates if signal.get(d, 0) < 0)

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
            "total_return": bt.total_return,
            "sharpe": bt.sharpe,
            "max_dd": bt.max_dd,
            "calmar": bt.calmar,
            "n_long": bt.n_long,
            "n_short": bt.n_short,
            "avg_long_return": bt.avg_long_return,
            "avg_short_return": bt.avg_short_return,
            "win_rate_long": bt.win_rate_long,
            "win_rate_short": bt.win_rate_short,
            "bh_return": bt.bh_return,
            "bh_sharpe": bt.bh_sharpe,
            "bh_max_dd": bt.bh_max_dd,
            "monthly_returns": bt.monthly_returns,
            "early_exit": None,
        }

        reports_dir = Path("data/reports")
        reports_dir.mkdir(parents=True, exist_ok=True)
        (reports_dir / f"{symbol}.json").write_text(
            json.dumps(report, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )

        return {
            "symbol": symbol,
            "total_return": bt.total_return,
            "sharpe": bt.sharpe,
            "max_dd": bt.max_dd,
            "calmar": bt.calmar,
            "n_long": bt.n_long,
            "n_short": bt.n_short,
            "avg_long_return": bt.avg_long_return,
            "avg_short_return": bt.avg_short_return,
            "win_rate_long": bt.win_rate_long,
            "win_rate_short": bt.win_rate_short,
            "bh_return": bt.bh_return,
            "bh_sharpe": bt.bh_sharpe,
            "bh_max_dd": bt.bh_max_dd,
            "monthly_returns": bt.monthly_returns,
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
    paths: DataPaths = DEFAULT_PATHS,
) -> None:
    """Save market_scan.json and market_scan.md."""
    derived_dir = paths.derived_dir
    derived_dir.mkdir(parents=True, exist_ok=True)

    # Count filter funnel
    total = len(phase1_results)
    f0_etf = sum(
        1 for r in phase1_results
        if r["filter_stage"] == 0 and "ETF" in r["reason"]
    )
    f0_split = sum(
        1 for r in phase1_results
        if r["filter_stage"] == 0 and "split" in r["reason"]
    )
    f0_data = sum(
        1 for r in phase1_results
        if r["filter_stage"] == 0 and "ETF" not in r["reason"] and "split" not in r["reason"]
    )
    f0a_pass = total - f0_etf
    f0b_pass = f0a_pass - f0_split
    f0c_pass = f0b_pass - f0_data
    f1_pass = sum(1 for r in phase1_results if r["filter_stage"] >= 2 or r["passed"])
    f2_pass = sum(1 for r in phase1_results if r["passed"])
    f3_pass = len(phase2_results)

    funnel = {
        "universe": total,
        "f0a_no_etf": f0a_pass,
        "f0b_no_split": f0b_pass,
        "f0c_sufficient_data": f0c_pass,
        "f1_liquidity": f1_pass,
        "f2_signal_quality": f2_pass,
        "f3_fdr": f3_pass,
    }

    sorted_results = sorted(phase2_results, key=lambda x: x["sharpe"], reverse=True)

    json_report = {
        "config": config.to_dict(),
        "generated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "filter_funnel": funnel,
        "results": sorted_results,
    }
    json_path = paths.market_scan_path
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
        f"| F0a: No ETF | {f0a_pass} | Exclude {f0_etf} ETFs/ETNs |",
        f"| F0b: No split | {f0b_pass} | Exclude {f0_split} splits/reverse-splits |",
        f"| F0c: Data | {f0c_pass} | Train ≥ 30 days, Test ≥ {config.min_test_days} days |",
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
# Main Pipeline
# =============================================================================

def run_scan(config: ScanConfig, paths: DataPaths = DEFAULT_PATHS) -> None:
    """Execute full market scan pipeline."""
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

    # Load close prices
    print("\n[Step 1] Loading close prices...")
    from broker_analytics.infrastructure.repositories import PriceRepository
    price_repo = PriceRepository(paths)
    prices_by_sym = price_repo.get_all_close_prices()

    summary_dir = str(paths.daily_summary_dir)
    summary_symbols = {p.stem for p in paths.daily_summary_dir.glob("*.parquet")}
    symbols = sorted(summary_symbols & set(prices_by_sym.keys()))
    print(f"  {len(prices_by_sym)} symbols with prices")
    print(f"  {len(summary_symbols)} symbols with daily_summary")
    print(f"  {len(symbols)} symbols to scan")

    # Phase 1: Parallel screening
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
    n_etf = sum(
        1 for r in phase1_results
        if r["filter_stage"] == 0 and "ETF" in r["reason"]
    )
    n_split = sum(
        1 for r in phase1_results
        if r["filter_stage"] == 0 and "split" in r["reason"]
    )
    n_data = sum(
        1 for r in phase1_results
        if r["filter_stage"] == 0 and "ETF" not in r["reason"] and "split" not in r["reason"]
    )
    n_f1 = sum(1 for r in phase1_results if r["filter_stage"] == 1)
    n_f2 = sum(1 for r in phase1_results if r["filter_stage"] == 2)

    passed_phase1 = [r for r in phase1_results if r["passed"]]
    print(f"\n  Filter funnel:")
    print(f"    F0a (ETF):       {n_etf} filtered")
    print(f"    F0b (split):     {n_split} filtered")
    print(f"    F0c (data):      {n_data} filtered")
    print(f"    F1 (liquidity):  {n_f1} filtered")
    print(f"    F2 (brokers):    {n_f2} filtered")
    print(f"    Passed F0-F2:    {len(passed_phase1)} symbols")

    # FDR correction
    print(f"\n[Step 3] BH-FDR correction (α={config.fdr_threshold:.0%})...")
    symbols_pvalues = [(r["symbol"], r["test_pvalue"]) for r in passed_phase1]
    fdr_passing = benjamini_hochberg(symbols_pvalues, config.fdr_threshold)
    print(f"  {len(fdr_passing)} symbols pass FDR correction")

    if not fdr_passing:
        print("\nNo symbols passed FDR. Saving empty report.")
        save_market_scan(config, phase1_results, [], paths)
        return

    for sym in fdr_passing:
        r = next(r for r in passed_phase1 if r["symbol"] == sym)
        print(
            f"    {sym}: t={r['test_tstat']:+.2f}, p={r['test_pvalue']:.4f}, "
            f"brokers={r['n_brokers']}, sig={r['pct_significant']:.1%}"
        )

    # Phase 2: OHLC fetch + backtest
    print(f"\n[Step 4] Phase 2: Backtest for {len(fdr_passing)} symbols...")
    ohlc_data = PriceRepository(paths).get_ohlc_batch(fdr_passing)
    missing_ohlc = [s for s in fdr_passing if s not in ohlc_data]
    if missing_ohlc:
        print(f"  Warning: no OHLC data for {missing_ohlc}")

    price_dir = str(paths.price_dir)
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

    # Save reports
    print(f"\n[Step 5] Saving reports...")
    save_market_scan(config, phase1_results, phase2_results, paths)

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
