"""Signal strength analysis: test whether a metric predicts forward returns.

Pure functions — input/output are polars DataFrames and dataclasses.
No I/O, no side effects.

Methodology:
- Returns should be excess (market-subtracted) and z-scored (per-stock normalized)
  BEFORE passing to analyze_strength. The runner handles this.
- group_col values are winsorized at 1st/99th percentile to prevent outlier dominance.
- Partial Spearman correlation available for controlling confounders.
"""

from dataclasses import dataclass

import numpy as np
import polars as pl


@dataclass(frozen=True, slots=True)
class GroupStats:
    """Statistics for one signal metric group."""

    label: str
    range_lo: float
    range_hi: float
    n_events: int
    mean_returns: dict[int, float]  # horizon → mean return


@dataclass(frozen=True, slots=True)
class StrengthResult:
    """Result of signal strength analysis."""

    groups: tuple[GroupStats, ...]
    horizons: tuple[int, ...]
    monotonic: dict[int, bool]
    spearman_corr: dict[int, float]
    partial_corr: dict[int, float]   # controlling for confound_col
    top_vs_bottom_diff: dict[int, float]
    n_total: int


def analyze_strength(
    events_with_returns: pl.DataFrame,
    n_groups: int = 3,
    horizons: tuple[int, ...] = (1, 5, 10, 20),
    group_col: str = "signal_count",
    confound_col: str | None = None,
    winsorize_pct: float = 0.01,
) -> StrengthResult:
    """Test whether a signal metric predicts forward return magnitude.

    Args:
        events_with_returns: DataFrame with columns:
            {group_col}, direction (Int8),
            ret_1d, ret_5d, ... (Float64). Returns should already be
            excess + z-scored (handled by runner) but NOT direction-adjusted
            (this function does that).
        n_groups: Number of groups to split into.
        horizons: Forward return horizons to analyze.
        group_col: Column to group by.
        confound_col: Column to control for in partial correlation
            (e.g. "signal_count" when testing churn_ratio).
        winsorize_pct: Percentile for winsorizing group_col (0.01 = 1st/99th).

    Returns:
        StrengthResult with per-group means, Spearman + partial correlation,
        and monotonicity test. Higher group_col → higher return = positive ρ.
    """
    if len(events_with_returns) == 0 or group_col not in events_with_returns.columns:
        empty = tuple(GroupStats(f"G{i+1}", 0, 0, 0, {}) for i in range(n_groups))
        return StrengthResult(
            groups=empty, horizons=horizons,
            monotonic={h: False for h in horizons},
            spearman_corr={h: 0.0 for h in horizons},
            partial_corr={h: 0.0 for h in horizons},
            top_vs_bottom_diff={h: 0.0 for h in horizons},
            n_total=0,
        )

    df = events_with_returns.filter(pl.col(group_col).is_not_null())

    # Direction-adjust returns
    ret_cols = [f"ret_{h}d" for h in horizons]
    for col in ret_cols:
        if col in df.columns:
            df = df.with_columns(
                (pl.col(col) * pl.col("direction")).alias(col)
            )

    # Winsorize group_col
    raw_vals = df[group_col].to_numpy().astype(float)
    lo_pct, hi_pct = np.percentile(raw_vals, [winsorize_pct * 100, (1 - winsorize_pct) * 100])
    clipped = np.clip(raw_vals, lo_pct, hi_pct)
    df = df.with_columns(pl.Series("_metric", clipped))

    # Compute group boundaries using quantiles on winsorized values
    boundaries = np.unique(np.quantile(clipped, np.linspace(0, 1, n_groups + 1)))
    actual_groups = len(boundaries) - 1

    if actual_groups < 2:
        min_val = float(clipped.min())
        boundaries = np.array([min_val, min_val + (clipped.max() - min_val) / 2, clipped.max() + 0.001])
        actual_groups = 2

    group_labels = np.digitize(clipped, boundaries[1:], right=True)
    df = df.with_columns(pl.Series("_group", group_labels))

    # Per-group statistics
    groups = []
    for g in range(actual_groups):
        g_df = df.filter(pl.col("_group") == g)
        lo = float(boundaries[g])
        hi = float(boundaries[min(g + 1, len(boundaries) - 1)])
        means = {}
        for h in horizons:
            col = f"ret_{h}d"
            if col in g_df.columns:
                valid = g_df[col].drop_nulls()
                means[h] = float(valid.mean()) if len(valid) > 0 else 0.0
        groups.append(GroupStats(f"G{g+1}", lo, hi, len(g_df), means))

    # Monotonicity: higher metric → higher return
    monotonic = {}
    for h in horizons:
        gm = [g.mean_returns.get(h, 0.0) for g in groups]
        monotonic[h] = all(gm[i] <= gm[i + 1] for i in range(len(gm) - 1))

    # Spearman: metric vs return
    spearman_corr = {}
    partial_corr = {}
    for h in horizons:
        col = f"ret_{h}d"
        if col not in df.columns:
            spearman_corr[h] = 0.0
            partial_corr[h] = 0.0
            continue
        valid = df.filter(pl.col(col).is_not_null())
        if len(valid) < 10:
            spearman_corr[h] = 0.0
            partial_corr[h] = 0.0
            continue

        metric = valid["_metric"].to_numpy()
        returns = valid[col].to_numpy()
        spearman_corr[h] = _spearman(metric, returns)

        # Partial correlation controlling for confound
        if confound_col and confound_col in valid.columns:
            confound = valid[confound_col].to_numpy().astype(float)
            partial_corr[h] = _partial_spearman(metric, returns, confound)
        else:
            partial_corr[h] = spearman_corr[h]

    # Top vs bottom
    top_vs_bottom = {}
    for h in horizons:
        top_vs_bottom[h] = groups[-1].mean_returns.get(h, 0.0) - groups[0].mean_returns.get(h, 0.0)

    return StrengthResult(
        groups=tuple(groups),
        horizons=horizons,
        monotonic=monotonic,
        spearman_corr=spearman_corr,
        partial_corr=partial_corr,
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


def _partial_spearman(x: np.ndarray, y: np.ndarray, z: np.ndarray) -> float:
    """Partial Spearman: correlation of x and y controlling for z."""
    rho_xy = _spearman(x, y)
    rho_xz = _spearman(x, z)
    rho_yz = _spearman(y, z)
    den_sq = (1 - rho_xz ** 2) * (1 - rho_yz ** 2)
    if den_sq <= 0:
        return 0.0
    return float((rho_xy - rho_xz * rho_yz) / (den_sq ** 0.5))


def _rank(arr: np.ndarray) -> np.ndarray:
    """Compute ranks (average method for ties)."""
    order = arr.argsort()
    ranks = np.empty_like(order, dtype=float)
    ranks[order] = np.arange(1, len(arr) + 1, dtype=float)
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
