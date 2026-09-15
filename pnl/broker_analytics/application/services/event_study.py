"""Event Study Service: Smart money large trades → forward returns.

Assembles domain functions to answer: do PNL top-K brokers' individual
large trades (per-broker 2σ) predict medium-term returns?

Pipeline:
    1. Detect events (rolling ranking + per-broker large trades)
    2. Descriptive statistics (threshold calibration)
    3. Compute forward returns, split by direction
    4. Per-direction: permutation test + parametric test
    5. Decay curve (direction-adjusted daily CAR)
    6. Robustness checks (optional)

CRITICAL: Uses rolling PNL ranking — no look-ahead bias.
"""

from dataclasses import dataclass, field

import numpy as np
import polars as pl

from broker_analytics.infrastructure.config import DataPaths, DEFAULT_PATHS
from broker_analytics.infrastructure.repositories import (
    TradeRepository,
    RepositoryError,
)
from broker_analytics.domain.statistics import (
    DistributionSummary,
    DistributionShape,
    summarize,
    describe_shape,
    welch_t_test,
    cohens_d,
    permutation_test,
)
from broker_analytics.domain.event_detection import (
    EventConfig,
    detect_smart_money_events,
    detect_placebo_events,
)
from broker_analytics.domain.forward_returns import (
    compute_forward_returns,
    sample_unconditional_returns,
    compute_daily_car,
    standardize_returns,
)


# =============================================================================
# Result Data Classes
# =============================================================================

@dataclass(frozen=True, slots=True)
class HorizonResult:
    """Statistical comparison at a single forward horizon.

    Uses permutation test as primary significance measure.
    """
    horizon: int
    n_events: int
    cond_mean: float
    cond_median: float
    uncond_mean: float
    t_stat: float
    perm_p: float
    cohens_d: float
    significant: bool  # perm_p < alpha AND |d| >= 0.2


@dataclass(frozen=True, slots=True)
class DirectionResult:
    """Results for one direction (accumulation or distribution)."""
    label: str  # "accumulation" / "distribution"
    n_events: int
    horizons: list[HorizonResult]
    significant_horizons: list[int]
    decay_curve: list[float]  # daily direction-adjusted CAR, len=max_horizon


@dataclass(frozen=True, slots=True)
class RobustnessResult:
    """Placebo test result."""
    placebo_significant: bool


@dataclass(frozen=True, slots=True)
class EventStudyReport:
    """Complete event study report for a single stock."""
    symbol: str
    config: EventConfig
    n_events: int
    n_accumulation: int
    n_distribution: int
    date_range: tuple[str, str]
    threshold_shape: DistributionShape
    accumulation: DirectionResult | None
    distribution: DirectionResult | None
    conclusion: str
    robustness: RobustnessResult | None


# =============================================================================
# Service
# =============================================================================

class EventStudyService:
    """Runs event study analysis for a single stock.

    Uses rolling PNL ranking + per-broker large trade detection.

    Example:
        >>> svc = EventStudyService()
        >>> report = svc.run("6285")
        >>> print(report.conclusion)
    """

    def __init__(self, paths: DataPaths = DEFAULT_PATHS):
        self._paths = paths
        self._trade_repo = TradeRepository(paths)

    def run(
        self,
        symbol: str,
        config: EventConfig = EventConfig(),
        horizons: tuple[int, ...] = (1, 5, 10, 20),
        run_robustness: bool = True,
    ) -> EventStudyReport | None:
        """Execute full event study pipeline.

        Returns:
            EventStudyReport, or None if data is insufficient.
        """
        # 1. Load data
        try:
            trade_df = self._trade_repo.get_symbol(symbol)
        except RepositoryError:
            return None

        pnl_daily_df = self._load_pnl_daily(symbol)
        if pnl_daily_df is None:
            return None

        prices = self._load_prices()
        if prices is None:
            return None

        # 2. Detect events (rolling ranking + per-broker large trades)
        events = detect_smart_money_events(trade_df, pnl_daily_df, config)
        if len(events) == 0:
            return None

        n_acc = int(events.filter(pl.col("direction") == 1).height)
        n_dist = int(events.filter(pl.col("direction") == -1).height)

        # 3. Descriptive statistics (threshold calibration)
        threshold_shape = self._compute_threshold_shape(trade_df, config.threshold_sigma)

        # 4. Compute forward returns
        event_returns = compute_forward_returns(events, prices, symbol, horizons)
        if len(event_returns) == 0:
            return None

        # 5. Unconditional baseline
        uncond = sample_unconditional_returns(prices, symbol, horizons=horizons)

        # 6. Analyze by direction
        acc_result = self._analyze_direction(
            "accumulation", 1, events, event_returns, prices, symbol, horizons, uncond,
        )
        dist_result = self._analyze_direction(
            "distribution", -1, events, event_returns, prices, symbol, horizons, uncond,
        )

        # 7. Conclusion: either direction has 2+ significant horizons
        acc_sig = len(acc_result.significant_horizons) if acc_result else 0
        dist_sig = len(dist_result.significant_horizons) if dist_result else 0
        best_sig = max(acc_sig, dist_sig)

        if best_sig >= 2:
            conclusion = "significant"
        elif best_sig == 1:
            conclusion = "marginal"
        else:
            conclusion = "no_effect"

        # 8. Robustness (placebo)
        robustness = None
        if run_robustness and best_sig >= 1:
            robustness = self._run_placebo(
                trade_df, pnl_daily_df, prices, symbol, config, horizons,
            )

        dates = events["date"].to_list()
        date_range = (min(dates).isoformat(), max(dates).isoformat())

        return EventStudyReport(
            symbol=symbol,
            config=config,
            n_events=len(events),
            n_accumulation=n_acc,
            n_distribution=n_dist,
            date_range=date_range,
            threshold_shape=threshold_shape,
            accumulation=acc_result,
            distribution=dist_result,
            conclusion=conclusion,
            robustness=robustness,
        )

    def run_pooled(
        self,
        symbols: list[str],
        config: EventConfig = EventConfig(),
        horizons: tuple[int, ...] = (1, 5, 10, 20),
    ) -> dict:
        """Cross-stock SCAR pooling.

        Runs per-stock analysis, standardizes returns by stock volatility,
        pools across stocks, and runs permutation test on pooled data.

        Returns:
            Dict with pooled results per direction per horizon.
        """
        prices = self._load_prices()
        if prices is None:
            return {}

        results = {"accumulation": {}, "distribution": {}}

        for direction, dir_val in [("accumulation", 1), ("distribution", -1)]:
            for h in horizons:
                all_scar = []

                for symbol in symbols:
                    try:
                        trade_df = self._trade_repo.get_symbol(symbol)
                    except RepositoryError:
                        continue

                    pnl_daily_df = self._load_pnl_daily(symbol)
                    if pnl_daily_df is None:
                        continue

                    events = detect_smart_money_events(trade_df, pnl_daily_df, config)
                    if len(events) == 0:
                        continue

                    dir_events = events.filter(pl.col("direction") == dir_val)
                    if len(dir_events) == 0:
                        continue

                    rets = compute_forward_returns(dir_events, prices, symbol, (h,))
                    col = f"ret_{h}d"
                    if col not in rets.columns:
                        continue

                    raw = rets[col].drop_nulls().to_numpy()
                    if len(raw) == 0:
                        continue

                    scar = standardize_returns(raw, prices, symbol, h)
                    all_scar.extend(scar.tolist())

                if not all_scar:
                    continue

                scar_arr = np.array(all_scar)
                # Pool unconditional: draw from all stocks
                uncond_scar = []
                for symbol in symbols:
                    unc = sample_unconditional_returns(
                        prices, symbol, n_samples=2000, horizons=(h,),
                    )
                    raw_unc = unc.get(h, np.array([]))
                    if len(raw_unc) > 0:
                        s = standardize_returns(raw_unc, prices, symbol, h)
                        uncond_scar.extend(s.tolist())

                uncond_arr = np.array(uncond_scar) if uncond_scar else np.array([])

                perm_p = 1.0
                t = 0.0
                d = 0.0
                if len(scar_arr) >= 3 and len(uncond_arr) >= 3:
                    perm_p = permutation_test(scar_arr, uncond_arr)
                    t, _ = welch_t_test(scar_arr, uncond_arr)
                    d = cohens_d(scar_arr, uncond_arr)

                results[direction][h] = {
                    "n_events": len(scar_arr),
                    "scar_mean": float(np.mean(scar_arr)),
                    "t_stat": t,
                    "perm_p": perm_p,
                    "cohens_d": d,
                    "significant": perm_p < (0.05 / len(horizons)) and abs(d) >= 0.2,
                }

        return results

    # -------------------------------------------------------------------------
    # Private Helpers
    # -------------------------------------------------------------------------

    def _analyze_direction(
        self,
        label: str,
        direction: int,
        events: pl.DataFrame,
        event_returns: pl.DataFrame,
        prices: pl.DataFrame,
        symbol: str,
        horizons: tuple[int, ...],
        uncond: dict[int, np.ndarray],
    ) -> DirectionResult | None:
        """Analyze one direction (accumulation or distribution)."""
        dir_events = events.filter(pl.col("direction") == direction)
        n_events = len(dir_events)
        if n_events < 3:
            return None

        dir_returns = event_returns.filter(pl.col("direction") == direction)
        if len(dir_returns) == 0:
            return None

        alpha = 0.05 / len(horizons)  # Bonferroni within direction
        horizon_results = []
        sig_horizons = []

        for h in horizons:
            col = f"ret_{h}d"
            cond_vals = dir_returns[col].drop_nulls().to_numpy()
            uncond_vals = uncond.get(h, np.array([]))

            if len(cond_vals) < 3 or len(uncond_vals) < 3:
                continue

            t, _ = welch_t_test(cond_vals, uncond_vals)
            d = cohens_d(cond_vals, uncond_vals)
            perm_p = permutation_test(cond_vals, uncond_vals)
            sig = perm_p < alpha and abs(d) >= 0.2

            horizon_results.append(HorizonResult(
                horizon=h,
                n_events=len(cond_vals),
                cond_mean=float(np.mean(cond_vals)),
                cond_median=float(np.median(cond_vals)),
                uncond_mean=float(np.mean(uncond_vals)),
                t_stat=t,
                perm_p=perm_p,
                cohens_d=d,
                significant=sig,
            ))

            if sig:
                sig_horizons.append(h)

        if not horizon_results:
            return None

        # Decay curve (direction-adjusted)
        decay = compute_daily_car(dir_events, prices, symbol, max(horizons))
        decay_list = [float(x) if not np.isnan(x) else 0.0 for x in decay]

        return DirectionResult(
            label=label,
            n_events=n_events,
            horizons=horizon_results,
            significant_horizons=sig_horizons,
            decay_curve=decay_list,
        )

    def _compute_threshold_shape(
        self, trade_df: pl.DataFrame, threshold_sigma: float,
    ) -> DistributionShape:
        """Distribution of per-broker net_buy z-scores for threshold calibration."""
        trades = (
            trade_df
            .with_columns(
                pl.col("broker").cast(pl.Utf8),
                (pl.col("buy_shares") - pl.col("sell_shares")).alias("net_buy"),
            )
        )
        broker_stats = (
            trades
            .group_by("broker")
            .agg(
                pl.col("net_buy").mean().alias("mean_nb"),
                pl.col("net_buy").std().alias("std_nb"),
            )
            .filter(pl.col("std_nb") > 0)
        )
        z_df = (
            trades
            .join(broker_stats, on="broker")
            .with_columns(
                ((pl.col("net_buy") - pl.col("mean_nb")) / pl.col("std_nb")).alias("z")
            )
        )
        z_values = z_df["z"].drop_nulls().to_numpy()
        return describe_shape(z_values, threshold_sigma)

    def _load_pnl_daily(self, symbol: str) -> pl.DataFrame | None:
        """Load per-stock daily PNL data."""
        path = self._paths.symbol_pnl_daily_path(symbol)
        if not path.exists():
            return None
        df = pl.read_parquet(path)
        required = {"broker", "date", "realized_pnl", "unrealized_pnl"}
        if not required.issubset(df.columns):
            return None
        return df

    def _load_prices(self) -> pl.DataFrame | None:
        """Load close prices via ws-core."""
        from broker_analytics.infrastructure.repositories import PriceRepository
        repo = PriceRepository(self._paths)
        df = repo.get_prices_df()
        return df if not df.is_empty() else None

    def _run_placebo(
        self,
        trade_df: pl.DataFrame,
        pnl_daily_df: pl.DataFrame,
        prices: pl.DataFrame,
        symbol: str,
        config: EventConfig,
        horizons: tuple[int, ...],
    ) -> RobustnessResult:
        """Placebo test: random brokers instead of top-K."""
        placebo_events = detect_placebo_events(trade_df, pnl_daily_df, config)
        placebo_sig = False

        if len(placebo_events) >= 10:
            placebo_rets = compute_forward_returns(
                placebo_events, prices, symbol, horizons,
            )
            uncond = sample_unconditional_returns(
                prices, symbol, horizons=horizons, seed=99,
            )
            for h in horizons:
                col = f"ret_{h}d"
                if col not in placebo_rets.columns:
                    continue
                cond = placebo_rets[col].drop_nulls().to_numpy()
                unc = uncond.get(h, np.array([]))
                if len(cond) >= 5 and len(unc) >= 5:
                    perm_p = permutation_test(cond, unc)
                    if perm_p < 0.05:
                        placebo_sig = True
                        break

        return RobustnessResult(placebo_significant=placebo_sig)
