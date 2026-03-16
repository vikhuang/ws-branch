"""Beta analysis: decompose trade returns into alpha and market beta.

Pure functions — input/output are numpy arrays and dataclasses.
No I/O, no side effects.
"""

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True, slots=True)
class BetaDecomposition:
    """Result of alpha/beta decomposition."""

    alpha_annualized_bps: float    # Annualized alpha (intercept × 252)
    beta: float                     # Market beta (regression slope)
    r_squared: float               # R² of regression
    total_sharpe: float            # Sharpe of raw trade returns
    excess_sharpe: float           # Sharpe of market-adjusted returns
    market_sharpe: float           # Buy-and-hold market Sharpe (same period)
    n_trades: int
    avg_return_bps: float          # Raw average per-trade return
    avg_excess_bps: float          # Average excess return per trade
    avg_market_bps: float          # Average market return over same hold periods


def decompose_beta(
    trade_returns_bps: np.ndarray,
    market_returns_bps: np.ndarray,
) -> BetaDecomposition:
    """Decompose per-trade returns into alpha + beta × market_return.

    Uses OLS regression: trade_return = alpha + beta × market_return + epsilon.
    Excess return = trade_return - market_return (simple subtraction, not
    beta-adjusted, which is more conservative).

    Args:
        trade_returns_bps: Per-trade net returns in basis points (after costs).
        market_returns_bps: Market index returns over the SAME hold period
            as each trade (matched 1:1).

    Returns:
        BetaDecomposition with alpha, beta, and Sharpe comparisons.
    """
    n = len(trade_returns_bps)
    if n < 2:
        return BetaDecomposition(
            alpha_annualized_bps=0.0, beta=0.0, r_squared=0.0,
            total_sharpe=0.0, excess_sharpe=0.0, market_sharpe=0.0,
            n_trades=n, avg_return_bps=0.0, avg_excess_bps=0.0,
            avg_market_bps=0.0,
        )

    # OLS: trade = alpha + beta * market
    x = market_returns_bps
    y = trade_returns_bps
    x_mean = np.mean(x)
    y_mean = np.mean(y)

    cov_xy = np.mean((x - x_mean) * (y - y_mean))
    var_x = np.mean((x - x_mean) ** 2)

    if var_x > 0:
        beta = float(cov_xy / var_x)
        alpha = float(y_mean - beta * x_mean)
    else:
        beta = 0.0
        alpha = float(y_mean)

    # R²
    y_hat = alpha + beta * x
    ss_res = np.sum((y - y_hat) ** 2)
    ss_tot = np.sum((y - y_mean) ** 2)
    r_squared = float(1.0 - ss_res / ss_tot) if ss_tot > 0 else 0.0

    # Sharpe calculations (annualized, assuming ~252 trades/year as scaling)
    total_sharpe = _sharpe(y)
    excess = y - x  # simple market-adjusted returns
    excess_sharpe = _sharpe(excess)
    market_sharpe = _sharpe(x)

    return BetaDecomposition(
        alpha_annualized_bps=alpha * 252,
        beta=beta,
        r_squared=r_squared,
        total_sharpe=total_sharpe,
        excess_sharpe=excess_sharpe,
        market_sharpe=market_sharpe,
        n_trades=n,
        avg_return_bps=float(np.mean(y)),
        avg_excess_bps=float(np.mean(excess)),
        avg_market_bps=float(np.mean(x)),
    )


def _sharpe(returns: np.ndarray) -> float:
    """Annualized Sharpe ratio from per-trade returns."""
    if len(returns) < 2:
        return 0.0
    std = float(np.std(returns, ddof=1))
    if std == 0:
        return 0.0
    return float(np.mean(returns) / std * (252 ** 0.5))
