"""Signal strength analysis: test whether higher signal_count → better returns.

Pure functions — input/output are polars DataFrames and dataclasses.
No I/O, no side effects.
"""

from dataclasses import dataclass

import numpy as np
import polars as pl


@dataclass(frozen=True, slots=True)
class GroupStats:
    """Statistics for one signal_count group."""

    label: str
    count_range: tuple[int, int]  # (min, max) inclusive
    n_events: int
    mean_returns: dict[int, float]  # horizon → mean direction-adjusted return (bps)


@dataclass(frozen=True, slots=True)
class StrengthResult:
    """Result of signal strength analysis."""

    groups: tuple[GroupStats, ...]
    horizons: tuple[int, ...]
    monotonic: dict[int, bool]          # horizon → is mean return monotonically increasing?
    spearman_corr: dict[int, float]     # horizon → rank correlation (count vs return)
    top_vs_bottom_diff: dict[int, float]  # horizon → mean(top group) - mean(bottom group)
    n_total: int


def analyze_strength(
    events_with_returns: pl.DataFrame,
    n_groups: int = 3,
    horizons: tuple[int, ...] = (1, 5, 10, 20),
) -> StrengthResult:
    """Test whether signal_count predicts forward return magnitude.

    Args:
        events_with_returns: DataFrame with columns:
            signal_count (Int32), direction (Int8),
            ret_1d, ret_5d, ret_10d, ret_20d (Float64, bps).
            Returns should NOT be direction-adjusted (raw).
        n_groups: Number of groups to split signal_count into.
        horizons: Forward return horizons to analyze.

    Returns:
        StrengthResult with per-group means, monotonicity test,
        and rank correlation.
    """
    if len(events_with_returns) == 0 or "signal_count" not in events_with_returns.columns:
        empty_groups = tuple(
            GroupStats(f"G{i+1}", (0, 0), 0, {}) for i in range(n_groups)
        )
        return StrengthResult(
            groups=empty_groups, horizons=horizons,
            monotonic={h: False for h in horizons},
            spearman_corr={h: 0.0 for h in horizons},
            top_vs_bottom_diff={h: 0.0 for h in horizons},
            n_total=0,
        )

    df = events_with_returns.filter(pl.col("signal_count").is_not_null())

    # Direction-adjust returns
    ret_cols = [f"ret_{h}d" for h in horizons]
    for col in ret_cols:
        if col in df.columns:
            df = df.with_columns(
                (pl.col(col) * pl.col("direction")).alias(col)
            )

    # Compute group boundaries using quantiles on signal_count
    counts = df["signal_count"].to_numpy()
    boundaries = np.quantile(counts, np.linspace(0, 1, n_groups + 1))
    # Ensure unique boundaries (if many ties at min value)
    boundaries = np.unique(boundaries)
    actual_groups = len(boundaries) - 1

    if actual_groups < 2:
        # Not enough variation — fall back to min vs above-min
        min_count = int(counts.min())
        boundaries = np.array([min_count, min_count + 0.5, counts.max() + 1])
        actual_groups = 2

    # Assign groups
    group_labels = np.digitize(counts, boundaries[1:], right=True)

    df = df.with_columns(pl.Series("_group", group_labels))

    # Compute per-group statistics
    groups = []
    for g in range(actual_groups):
        g_df = df.filter(pl.col("_group") == g)
        lo = int(boundaries[g]) if g < len(boundaries) - 1 else int(boundaries[-2])
        hi = int(boundaries[g + 1]) if g + 1 < len(boundaries) else int(boundaries[-1])
        n = len(g_df)

        means = {}
        for h in horizons:
            col = f"ret_{h}d"
            if col in g_df.columns:
                valid = g_df[col].drop_nulls()
                means[h] = float(valid.mean()) if len(valid) > 0 else 0.0

        groups.append(GroupStats(
            label=f"G{g+1}",
            count_range=(lo, hi),
            n_events=n,
            mean_returns=means,
        ))

    # Monotonicity test per horizon
    monotonic = {}
    for h in horizons:
        group_means = [g.mean_returns.get(h, 0.0) for g in groups]
        monotonic[h] = all(
            group_means[i] <= group_means[i + 1]
            for i in range(len(group_means) - 1)
        )

    # Spearman rank correlation: signal_count vs direction-adjusted return
    spearman_corr = {}
    for h in horizons:
        col = f"ret_{h}d"
        if col in df.columns:
            valid = df.filter(pl.col(col).is_not_null())
            if len(valid) > 2:
                spearman_corr[h] = _spearman(
                    valid["signal_count"].to_numpy().astype(float),
                    valid[col].to_numpy(),
                )
            else:
                spearman_corr[h] = 0.0
        else:
            spearman_corr[h] = 0.0

    # Top vs bottom group diff
    top_vs_bottom = {}
    for h in horizons:
        top = groups[-1].mean_returns.get(h, 0.0)
        bottom = groups[0].mean_returns.get(h, 0.0)
        top_vs_bottom[h] = top - bottom

    return StrengthResult(
        groups=tuple(groups),
        horizons=horizons,
        monotonic=monotonic,
        spearman_corr=spearman_corr,
        top_vs_bottom_diff=top_vs_bottom,
        n_total=len(df),
    )


def _spearman(x: np.ndarray, y: np.ndarray) -> float:
    """Spearman rank correlation (no scipy dependency)."""
    n = len(x)
    if n < 3:
        return 0.0
    rx = _rank(x)
    ry = _rank(y)
    d = rx - ry
    return float(1.0 - 6.0 * np.sum(d ** 2) / (n * (n ** 2 - 1)))


def _rank(arr: np.ndarray) -> np.ndarray:
    """Compute ranks (average method for ties)."""
    order = arr.argsort()
    ranks = np.empty_like(order, dtype=float)
    ranks[order] = np.arange(1, len(arr) + 1, dtype=float)
    # Handle ties: average rank
    sorted_arr = arr[order]
    i = 0
    while i < len(sorted_arr):
        j = i
        while j < len(sorted_arr) and sorted_arr[j] == sorted_arr[i]:
            j += 1
        avg_rank = (i + 1 + j) / 2.0
        for k in range(i, j):
            ranks[order[k]] = avg_rank
        i = j
    return ranks
