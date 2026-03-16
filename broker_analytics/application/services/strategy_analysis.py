"""Strategy analysis service: post-hoc evaluation of backtest results.

Application layer — reads ws-quant trade logs and price data,
delegates to domain/beta_analysis for pure computation.
"""

from pathlib import Path

import numpy as np
import polars as pl

from broker_analytics.domain.beta_analysis import BetaDecomposition, decompose_beta
from broker_analytics.infrastructure.repositories import PriceRepository

# TAIEX weighted index symbol in TEJ data
_MARKET_SYMBOL = "IX0001"


def analyze_beta(
    trade_log_path: Path,
    price_repo: PriceRepository | None = None,
    market_symbol: str = _MARKET_SYMBOL,
) -> BetaDecomposition:
    """Compute beta decomposition for a ws-quant trade log.

    Reads the trade log CSV, computes market returns for the same
    (entry_date, exit_date) window of each trade, then decomposes
    trade returns into alpha + beta × market.

    Args:
        trade_log_path: Path to ws-quant experiment trade log CSV.
        price_repo: PriceRepository instance (created if None).
        market_symbol: Market index symbol for beta computation.

    Returns:
        BetaDecomposition with alpha, beta, excess Sharpe.
    """
    if price_repo is None:
        price_repo = PriceRepository()

    trades = _load_trade_log(trade_log_path)
    if len(trades) == 0:
        return decompose_beta(np.array([]), np.array([]))

    market_prices = _load_market_prices(price_repo, market_symbol)
    if len(market_prices) == 0:
        return decompose_beta(np.array([]), np.array([]))

    trade_returns, market_returns = _match_market_returns(trades, market_prices)
    return decompose_beta(trade_returns, market_returns)


def analyze_beta_batch(
    trade_log_dir: Path,
    strategy_name: str,
    price_repo: PriceRepository | None = None,
    market_symbol: str = _MARKET_SYMBOL,
    tag: str = "post-bias-fix",
) -> dict[str, BetaDecomposition]:
    """Analyze beta for all horizons of a strategy.

    Scans trade_log_dir for experiment logs with matching tag and factor_name.

    Args:
        tag: Required experiment tag (default "post-bias-fix").
            Use "deduped" for deduped backtest results.

    Returns:
        Dict mapping hold label (e.g. "10d") to BetaDecomposition.
    """
    if price_repo is None:
        price_repo = PriceRepository()

    results = {}
    for path in sorted(trade_log_dir.glob("*.json")):
        import json
        meta = json.loads(path.read_text())
        if tag not in meta.get("tags", []):
            continue
        factor_name = meta.get("factor_name", "")
        # Match both "strat_10d" and "strat_dedup_10d"
        prefix = strategy_name + "_"
        if not factor_name.startswith(prefix):
            continue

        hold_label = factor_name.removeprefix(prefix)
        # Strip "dedup_" prefix from hold label if present
        hold_label = hold_label.removeprefix("dedup_")
        trade_csv = path.with_name(path.stem + "_trades.csv")
        if not trade_csv.exists():
            continue

        results[hold_label] = analyze_beta(trade_csv, price_repo, market_symbol)

    return results


def _load_trade_log(path: Path) -> pl.DataFrame:
    """Load ws-quant trade log CSV.

    Expected columns: date, symbol, direction, hold_seconds, net_bps.
    Exit date is computed from entry_date + hold_seconds (the exit_time
    column in ws-quant logs only stores intraday time, not the actual exit date).
    """
    df = pl.read_csv(path, schema_overrides={"symbol": pl.Utf8})
    required = {"date", "hold_seconds", "net_bps"}
    if not required.issubset(set(df.columns)):
        missing = required - set(df.columns)
        raise ValueError(f"Trade log missing columns: {missing}")

    return df.with_columns(
        pl.col("date").str.to_date().alias("entry_date"),
        (pl.col("date").str.to_date().cast(pl.Datetime("us"))
         + pl.duration(seconds=pl.col("hold_seconds"))
        ).cast(pl.Date).alias("exit_date"),
    )


def _load_market_prices(
    price_repo: PriceRepository, market_symbol: str,
) -> dict:
    """Load market index close prices as {date: close_price} dict."""
    return price_repo.get_close_prices(market_symbol)


def _match_market_returns(
    trades: pl.DataFrame,
    market_prices: dict,
) -> tuple[np.ndarray, np.ndarray]:
    """Compute matched (trade_return, market_return) pairs.

    For each trade, compute market return over the same [entry_date, exit_date]
    window. Skips trades where market price is unavailable.
    """
    trade_rets = []
    market_rets = []

    for row in trades.iter_rows(named=True):
        entry_date = row["entry_date"]
        exit_date = row["exit_date"]
        net_bps = row["net_bps"]

        mkt_entry = market_prices.get(entry_date)
        mkt_exit = market_prices.get(exit_date)

        if mkt_entry is None or mkt_exit is None or mkt_entry == 0:
            continue

        mkt_ret_bps = (mkt_exit - mkt_entry) / mkt_entry * 10000
        # Direction-adjust market return (short trades benefit from market drops)
        direction = row.get("direction")
        if direction == "short" or direction == -1:
            mkt_ret_bps = -mkt_ret_bps

        trade_rets.append(net_bps)
        market_rets.append(mkt_ret_bps)

    return np.array(trade_rets, dtype=np.float64), np.array(market_rets, dtype=np.float64)
