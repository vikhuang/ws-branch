"""Timing Alpha: Measure broker market timing ability.

Formula:
    timing_alpha = Σ((net_buy[t-1] - avg_net_buy) × return[t]) / std(net_buy)

Interpretation:
- Positive: buys before rallies, sells before drops (good timing)
- Negative: buys before drops, sells before rallies (bad timing)
- Normalized by std(net_buy) to remove volume bias

Used by: pnl_engine.py, signal_report, market_scan
"""

from typing import Sequence


def compute_timing_alpha(
    net_buys: Sequence[float | int],
    returns: Sequence[float],
) -> float:
    """Compute normalized timing alpha.

    Args:
        net_buys: Daily net buy amounts (buy - sell), aligned by date.
        returns: Daily stock returns, same length as net_buys.
                 net_buys[t-1] predicts returns[t].

    Returns:
        Normalized timing alpha (raw / std(net_buy)).
        Returns 0.0 if insufficient data or zero variance.
    """
    n = len(net_buys)
    if n < 2:
        return 0.0

    avg = sum(net_buys) / n
    raw = 0.0
    for t in range(1, n):
        raw += (net_buys[t - 1] - avg) * returns[t]

    variance = sum((x - avg) ** 2 for x in net_buys) / n
    std = variance ** 0.5
    return raw / std if std > 0 else 0.0
