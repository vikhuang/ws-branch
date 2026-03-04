"""Event Study Service: Smart money accumulation → forward returns.

Assembles domain functions to answer: do PNL top-K brokers' cumulative
buy/sell patterns predict medium-term returns?

CRITICAL: Uses rolling PNL ranking — no look-ahead bias. For each date T,
"smart money" is defined using only PNL data up to T.

Pipeline:
    1. Detect events (domain/event_detection) — rolling ranking
    2. Compute forward returns (domain/forward_returns)
    3. Sample unconditional baseline
    4. Compare distributions (domain/statistics)
    5. Robustness checks (placebo, dose-response, temporal split)
"""

from dataclasses import dataclass

import numpy as np
import polars as pl

from pnl_analytics.infrastructure.config import DataPaths, DEFAULT_PATHS
from pnl_analytics.infrastructure.repositories import (
    TradeRepository,
    RepositoryError,
)
from pnl_analytics.domain.statistics import (
    DistributionSummary,
    HypothesisTestResult,
    summarize,
    compare_distributions,
)
from pnl_analytics.domain.event_detection import (
    EventConfig,
    detect_smart_money_events,
    detect_placebo_events,
)
from pnl_analytics.domain.forward_returns import (
    compute_forward_returns,
    sample_unconditional_returns,
)


# =============================================================================
# Result Data Classes
# =============================================================================

@dataclass(frozen=True, slots=True)
class HorizonResult:
    """Statistical comparison at a single forward horizon.

    Attributes:
        horizon: Forward horizon in trading days.
        conditional: Distribution of post-event returns.
        unconditional: Distribution of baseline returns.
        test: Hypothesis test result.
    """
    horizon: int
    conditional: DistributionSummary
    unconditional: DistributionSummary
    test: HypothesisTestResult


@dataclass(frozen=True, slots=True)
class RobustnessResult:
    """Results of robustness checks.

    Attributes:
        placebo_significant: Whether placebo events are also significant
                             (should be False for a valid signal).
        dose_response_monotonic: Whether quintile CARs are monotonically
                                  increasing (stronger signal → bigger return).
        temporal_in_sample: Significant horizons in first half of events.
        temporal_out_of_sample: Significant horizons in second half.
        quintile_cars: Average CAR per signal quintile (5 values, bps).
    """
    placebo_significant: bool
    dose_response_monotonic: bool
    temporal_in_sample: int
    temporal_out_of_sample: int
    quintile_cars: list[float]


@dataclass(frozen=True, slots=True)
class EventStudyReport:
    """Complete event study report for a single stock.

    Attributes:
        symbol: Stock symbol.
        config: Event detection configuration used.
        n_events: Total number of events detected.
        n_accumulation: Events with direction +1.
        n_distribution: Events with direction -1.
        date_range: (first_date, last_date) as ISO strings.
        horizons: Per-horizon statistical results.
        conclusion: "significant" / "marginal" / "no_effect".
        significant_horizons: List of significant horizon values.
        robustness: Robustness check results (None if skipped).
    """
    symbol: str
    config: EventConfig
    n_events: int
    n_accumulation: int
    n_distribution: int
    date_range: tuple[str, str]
    horizons: list[HorizonResult]
    conclusion: str
    significant_horizons: list[int]
    robustness: RobustnessResult | None


# =============================================================================
# Service
# =============================================================================

class EventStudyService:
    """Runs event study analysis for a single stock.

    Uses rolling PNL ranking — no look-ahead bias.

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

        Args:
            symbol: Stock symbol (e.g., "6285").
            config: Event detection parameters.
            horizons: Forward return horizons in trading days.
            run_robustness: Whether to run robustness checks.

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

        # 2. Detect events (rolling ranking — no look-ahead)
        events = detect_smart_money_events(trade_df, pnl_daily_df, config)
        if len(events) == 0:
            return None

        n_acc = int(events.filter(pl.col("direction") == 1).height)
        n_dist = int(events.filter(pl.col("direction") == -1).height)

        # 3. Compute forward returns
        event_returns = compute_forward_returns(events, prices, symbol, horizons)
        if len(event_returns) == 0:
            return None

        # 4. Sample unconditional baseline
        uncond = sample_unconditional_returns(prices, symbol, horizons=horizons)

        # 5. Compare at each horizon
        horizon_results = []
        sig_horizons = []
        n_tests = len(horizons)

        for h in horizons:
            col = f"ret_{h}d"
            cond_vals = event_returns[col].drop_nulls().to_numpy()
            uncond_vals = uncond.get(h, np.array([]))

            if len(cond_vals) < 5 or len(uncond_vals) < 5:
                continue

            cond_summary = summarize(cond_vals)
            uncond_summary = summarize(uncond_vals)
            test = compare_distributions(cond_vals, uncond_vals, n_tests=n_tests)

            horizon_results.append(HorizonResult(
                horizon=h,
                conditional=cond_summary,
                unconditional=uncond_summary,
                test=test,
            ))

            if test.significant:
                sig_horizons.append(h)

        if not horizon_results:
            return None

        # 6. Determine conclusion
        n_sig = len(sig_horizons)
        if n_sig >= 2:
            conclusion = "significant"
        elif n_sig == 1:
            conclusion = "marginal"
        else:
            conclusion = "no_effect"

        # 7. Robustness checks
        robustness = None
        if run_robustness and n_sig >= 1:
            robustness = self._run_robustness(
                trade_df, pnl_daily_df, prices, symbol,
                events, event_returns, config, horizons,
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
            horizons=horizon_results,
            conclusion=conclusion,
            significant_horizons=sig_horizons,
            robustness=robustness,
        )

    # -------------------------------------------------------------------------
    # Private Helpers
    # -------------------------------------------------------------------------

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
        """Load close prices."""
        path = self._paths.close_prices
        if not path.exists():
            return None
        return pl.read_parquet(path)

    def _run_robustness(
        self,
        trade_df: pl.DataFrame,
        pnl_daily_df: pl.DataFrame,
        prices: pl.DataFrame,
        symbol: str,
        events: pl.DataFrame,
        event_returns: pl.DataFrame,
        config: EventConfig,
        horizons: tuple[int, ...],
    ) -> RobustnessResult:
        """Run all robustness checks."""
        n_tests = len(horizons)

        # --- Placebo ---
        placebo_events = detect_placebo_events(trade_df, pnl_daily_df, config)
        placebo_sig = False
        if len(placebo_events) >= 10:
            placebo_rets = compute_forward_returns(placebo_events, prices, symbol, horizons)
            uncond = sample_unconditional_returns(prices, symbol, horizons=horizons, seed=99)
            for h in horizons:
                col = f"ret_{h}d"
                if col not in placebo_rets.columns:
                    continue
                cond = placebo_rets[col].drop_nulls().to_numpy()
                unc = uncond.get(h, np.array([]))
                if len(cond) >= 5 and len(unc) >= 5:
                    test = compare_distributions(cond, unc, n_tests=n_tests)
                    if test.significant:
                        placebo_sig = True
                        break

        # --- Dose-response ---
        best_horizon = max(horizons)
        car_col = f"ret_{best_horizon}d"
        quintile_cars = [0.0] * 5

        valid_rets = event_returns.drop_nulls(car_col)
        if len(valid_rets) >= 10:
            n_total = len(valid_rets)
            q_size = n_total / 5

            for q in range(5):
                lo = int(q * q_size)
                hi = int((q + 1) * q_size) if q < 4 else n_total
                q_df = valid_rets.sort("signal_value").slice(lo, hi - lo)
                if len(q_df) > 0:
                    quintile_cars[q] = float(q_df[car_col].mean())

        is_monotonic = all(
            quintile_cars[i] <= quintile_cars[i + 1]
            for i in range(4)
        )

        # --- Temporal split ---
        n_events = len(event_returns)
        mid = n_events // 2
        sorted_rets = event_returns.sort("date")
        first_half = sorted_rets.head(mid)
        second_half = sorted_rets.tail(n_events - mid)

        uncond = sample_unconditional_returns(prices, symbol, horizons=horizons, seed=123)

        in_sample_sig = 0
        out_sample_sig = 0

        for h in horizons:
            col = f"ret_{h}d"
            unc = uncond.get(h, np.array([]))

            c1 = first_half[col].drop_nulls().to_numpy()
            if len(c1) >= 5 and len(unc) >= 5:
                t1 = compare_distributions(c1, unc, n_tests=n_tests)
                if t1.significant:
                    in_sample_sig += 1

            c2 = second_half[col].drop_nulls().to_numpy()
            if len(c2) >= 5 and len(unc) >= 5:
                t2 = compare_distributions(c2, unc, n_tests=n_tests)
                if t2.significant:
                    out_sample_sig += 1

        return RobustnessResult(
            placebo_significant=placebo_sig,
            dose_response_monotonic=is_monotonic,
            temporal_in_sample=in_sample_sig,
            temporal_out_of_sample=out_sample_sig,
            quintile_cars=quintile_cars,
        )
