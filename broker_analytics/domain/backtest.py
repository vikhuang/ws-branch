"""Signal backtest engine: open-to-close daily returns.

Strategy: signal[T] → trade on T+1 (buy at open, sell at close).
Positive signal = long, negative signal = short.
Includes round-trip cost deduction.

Used by: signal_report, market_scan
"""

from dataclasses import dataclass
from datetime import date

import numpy as np
import polars as pl


@dataclass(frozen=True, slots=True)
class BacktestResult:
    """Result of a signal backtest."""
    total_return: float
    sharpe: float
    max_dd: float
    calmar: float
    n_long: int
    n_short: int
    avg_long_return: float
    avg_short_return: float
    win_rate_long: float
    win_rate_short: float
    bh_return: float
    bh_sharpe: float
    bh_max_dd: float
    monthly_returns: list[dict]


def run_backtest(
    signal: dict[date, float],
    ohlc: pl.DataFrame,
    test_dates: list[date],
    cost: float = 0.00435,
) -> BacktestResult:
    """Execute signal backtest with open-to-close returns.

    Args:
        signal: Dict mapping date to signal value (positive=long, negative=short).
        ohlc: DataFrame with columns [date, open, close].
        test_dates: Ordered list of trading dates in test period.
        cost: Round-trip cost per trade (default 0.435%).

    Returns:
        BacktestResult with all performance metrics.
    """
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

    # Compounded equity curve
    equity = [1.0]
    for r in daily_pnl:
        equity.append(equity[-1] * (1 + r))
    total_return = equity[-1] / equity[0] - 1

    # Sharpe (annualized)
    pnl_arr = np.array(daily_pnl)
    sharpe = (
        float(np.mean(pnl_arr) / np.std(pnl_arr) * np.sqrt(252))
        if len(pnl_arr) > 0 and np.std(pnl_arr) > 0
        else 0.0
    )

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
        monthly_returns.append({
            "month": month,
            "strategy": round((strat_cum - 1) * 100, 2),
            "bh": round((bh_cum - 1) * 100, 2),
        })

    # Win rates
    long_arr = np.array(long_returns) if long_returns else np.array([])
    short_arr = np.array(short_returns) if short_returns else np.array([])

    return BacktestResult(
        total_return=total_return,
        sharpe=sharpe,
        max_dd=max_dd,
        calmar=calmar,
        n_long=len(long_returns),
        n_short=len(short_returns),
        avg_long_return=float(np.mean(long_arr)) if len(long_arr) > 0 else 0.0,
        avg_short_return=float(np.mean(short_arr)) if len(short_arr) > 0 else 0.0,
        win_rate_long=float(np.mean(long_arr > 0)) if len(long_arr) > 0 else 0.0,
        win_rate_short=float(np.mean(short_arr > 0)) if len(short_arr) > 0 else 0.0,
        bh_return=bh_return,
        bh_sharpe=bh_sharpe,
        bh_max_dd=bh_max_dd,
        monthly_returns=monthly_returns,
    )
