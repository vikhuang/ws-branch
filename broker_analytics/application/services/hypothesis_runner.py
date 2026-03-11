"""Hypothesis Runner Service: Orchestrates the 5-step pipeline.

Application layer -- handles I/O (loading data from repositories),
then delegates to pure domain functions for each step.
"""

import sys
import time
from dataclasses import replace
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import polars as pl

from broker_analytics.infrastructure.config import DataPaths, DEFAULT_PATHS
from broker_analytics.infrastructure.repositories import (
    TradeRepository,
    RankingRepository,
    PriceRepository,
    RepositoryError,
)
from broker_analytics.domain.hypothesis.types import (
    SymbolData,
    GlobalContext,
    HypothesisConfig,
    HypothesisResult,
    HorizonDetail,
)
from broker_analytics.domain.hypothesis.registry import STRATEGIES, get_strategy
from broker_analytics.domain.statistics import benjamini_hochberg


class HypothesisRunner:
    """Runs hypothesis tests for one or more symbols.

    Example:
        >>> runner = HypothesisRunner()
        >>> result = runner.run_single("2330", "contrarian_broker")
        >>> print(result.conclusion)
    """

    def __init__(self, paths: DataPaths = DEFAULT_PATHS):
        self._paths = paths
        self._trade_repo = TradeRepository(paths)
        self._price_repo = PriceRepository(paths)
        self._global_ctx: GlobalContext | None = None

    def run_single(
        self,
        symbol: str,
        strategy_name: str,
        params_override: dict | None = None,
    ) -> HypothesisResult | None:
        """Run one hypothesis on one symbol.

        Args:
            symbol: Stock symbol (e.g., "2330")
            strategy_name: Key from STRATEGIES registry
            params_override: Override default params (merged, not replaced)

        Returns:
            HypothesisResult, or None if data insufficient.
        """
        config = get_strategy(strategy_name)
        if params_override:
            merged_params = {**config.params, **params_override}
            config = replace(config, params=merged_params)

        # Inject horizons into params for step functions
        params = {**config.params, "horizons": config.horizons}

        # Strategy-specific one-time setup
        self._inject_global_params(config, params)

        # Load data
        data = self._load_symbol_data(symbol, config.requires)
        if data is None:
            return None

        ctx = self._get_global_context()

        return self._run_pipeline(config, data, ctx, params)

    def run_batch(
        self,
        symbols: list[str],
        strategy_name: str,
        workers: int = 1,
    ) -> list[HypothesisResult]:
        """Run one strategy across multiple symbols.

        For workers=1, runs sequentially (simpler debugging).
        For workers>1, uses ProcessPoolExecutor.
        """
        results = []
        if workers <= 1:
            for sym in symbols:
                r = self.run_single(sym, strategy_name)
                if r is not None:
                    results.append(r)
        else:
            with ProcessPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(
                        _run_single_worker,
                        str(self._paths.root),
                        self._paths.variant,
                        sym,
                        strategy_name,
                    ): sym
                    for sym in symbols
                }
                for future in as_completed(futures):
                    r = future.result()
                    if r is not None:
                        results.append(r)

        return results

    def run_scan(
        self,
        strategy_name: str,
        fdr: float = 0.05,
        params_override: dict | None = None,
    ) -> list[HypothesisResult]:
        """Run one strategy across all symbols with progress and FDR correction.

        Sequential only — prices cached once, per-symbol loads only what's needed.
        """
        config = get_strategy(strategy_name)
        if params_override:
            merged_params = {**config.params, **params_override}
            config = replace(config, params=merged_params)
        requires = config.requires
        all_symbols = self._paths.list_symbols()
        n_total = len(all_symbols)

        print(f"【全市場掃描】{config.name}（{config.display_name}）")
        print(f"  策略：{config.description}")
        print(f"  股票數：{n_total}")
        print(f"  載入：{', '.join(sorted(requires))}")
        print()

        # Pre-warm prices cache
        self._price_repo.get_prices_df()
        ctx = self._get_global_context()
        params_template = {**config.params, "horizons": config.horizons}

        # Strategy-specific one-time setup (e.g. global rolling ranking)
        self._inject_global_params(config, params_template)

        results: list[HypothesisResult] = []
        n_skipped = 0
        n_no_broker = 0
        n_no_event = 0
        t0 = time.time()

        for i, symbol in enumerate(all_symbols):
            data = self._load_symbol_data(symbol, requires)
            if data is None:
                n_skipped += 1
                self._print_progress(
                    i + 1, n_total, symbol, "skip", len(results), t0,
                )
                continue

            params = {**params_template}
            result = self._run_pipeline(config, data, ctx, params)

            if result is None:
                n_skipped += 1
                tag = "skip"
            elif result.n_brokers_selected == 0:
                n_no_broker += 1
                tag = "no_broker"
            elif result.n_events == 0:
                n_no_event += 1
                tag = "no_event"
            else:
                results.append(result)
                tag = result.conclusion[:3]  # "sig" / "mar" / "no_"

            self._print_progress(i + 1, n_total, symbol, tag, len(results), t0)

        elapsed = time.time() - t0
        print(f"\n{'='*60}")
        print(f"完成：{elapsed:.0f}s（{elapsed/n_total:.2f}s/股）")
        print(f"  總股票：{n_total}")
        print(f"  跳過（資料不足）：{n_skipped}")
        print(f"  無券商被選：{n_no_broker}")
        print(f"  無事件：{n_no_event}")
        print(f"  有結果：{len(results)}")

        if not results:
            print("  無可分析結果")
            return results

        # Per-symbol conclusion counts (before FDR)
        sig_before = sum(1 for r in results if r.conclusion == "significant")
        mar_before = sum(1 for r in results if r.conclusion == "marginal")
        no_before = sum(1 for r in results if r.conclusion == "no_effect")
        print(f"\n--- 校正前（per-symbol permutation test）---")
        print(f"  顯著：{sig_before}（{sig_before/len(results)*100:.1f}%）")
        print(f"  邊際：{mar_before}（{mar_before/len(results)*100:.1f}%）")
        print(f"  無效：{no_before}（{no_before/len(results)*100:.1f}%）")

        # FDR correction across all symbols × horizons
        self._print_fdr_summary(results, config, fdr)

        return results

    def run_all_strategies(
        self,
        symbol: str,
    ) -> dict[str, HypothesisResult | None]:
        """Run all strategies on one symbol."""
        results = {}
        for name in STRATEGIES:
            try:
                results[name] = self.run_single(symbol, name)
            except Exception as e:
                print(f"  {name}: error - {e}")
                results[name] = None
        return results

    # -------------------------------------------------------------------------
    # Pipeline execution
    # -------------------------------------------------------------------------

    def _run_pipeline(
        self,
        config: HypothesisConfig,
        data: SymbolData,
        ctx: GlobalContext,
        params: dict,
    ) -> HypothesisResult:
        """Execute the 5-step pipeline on loaded data."""
        # Step 1: Select brokers
        brokers = config.selector(data, ctx, params)
        if not brokers:
            return self._empty_result(config.name, data.symbol, 0, 0, params)

        # Inject broker list (for herding baseline)
        params["_brokers_list"] = brokers

        # Inject cluster trade data for cross_stock strategy
        if "cluster" in params:
            cluster_syms = params["cluster"]
            if isinstance(cluster_syms, str):
                cluster_syms = [s.strip() for s in cluster_syms.split(",")]
            cluster_trades = {}
            for sym in cluster_syms:
                path = self._paths.symbol_trade_path(sym)
                if path.exists():
                    cluster_trades[sym] = pl.read_parquet(path)
            params["_cluster_trades"] = cluster_trades

        # Inject broker concentration data for concentration strategy
        if config.name == "concentration":
            params["_broker_concentrations"] = self._load_broker_concentrations(
                data.symbol
            )

        # Step 2: Filter events
        events = config.filter(data, brokers, params)
        if len(events) == 0:
            return self._empty_result(
                config.name, data.symbol, len(brokers), 0, params,
            )

        # Step 3: Compute outcome returns
        event_returns = config.outcome(data, events, params)

        # Step 4: Compute baseline returns
        baseline_returns = config.baseline(data, events, params)

        # Step 5: Statistical test
        test_results = config.stat_test(event_returns, baseline_returns, params)

        # Build result
        horizon_details = []
        sig_count = 0
        for h in config.horizons:
            cond = event_returns.get(h, np.array([]))
            uncond = baseline_returns.get(h, np.array([]))
            tr = test_results.get(h)
            if tr is None:
                continue
            detail = HorizonDetail(
                horizon=h,
                n_events=len(cond),
                n_baseline=len(uncond),
                cond_mean=float(np.mean(cond)) if len(cond) > 0 else 0.0,
                uncond_mean=float(np.mean(uncond)) if len(uncond) > 0 else 0.0,
                test_result=tr,
            )
            horizon_details.append(detail)
            if tr.significant:
                sig_count += 1

        # Conclusion logic (same as event_study)
        if sig_count >= 2:
            conclusion = "significant"
        elif sig_count == 1:
            conclusion = "marginal"
        else:
            conclusion = "no_effect"

        return HypothesisResult(
            strategy_name=config.name,
            symbol=data.symbol,
            n_brokers_selected=len(brokers),
            n_events=len(events),
            horizon_details=tuple(horizon_details),
            conclusion=conclusion,
            params=params,
        )

    # -------------------------------------------------------------------------
    # Strategy-specific setup (runs once, injects into params)
    # -------------------------------------------------------------------------

    def _inject_global_params(
        self, config: HypothesisConfig, params: dict,
    ) -> None:
        """Inject strategy-specific global data into params.

        Called once before the per-symbol loop. Cached to avoid recomputation.
        Currently no strategies require global pre-computation.
        """
        pass

    # -------------------------------------------------------------------------
    # Private data loading
    # -------------------------------------------------------------------------

    def _load_symbol_data(
        self, symbol: str, requires: frozenset[str] | None = None,
    ) -> SymbolData | None:
        """Load data for one symbol, respecting requires set.

        Only loads the parquet files specified in requires.
        Unneeded fields get empty DataFrames.
        """
        if requires is None:
            requires = frozenset({"trade_df", "pnl_daily_df", "pnl_df", "prices"})

        # trade_df is always needed (all strategies use it)
        try:
            trade_df = self._trade_repo.get_symbol(symbol)
        except RepositoryError:
            return None

        # pnl_daily_df
        if "pnl_daily_df" in requires:
            pnl_daily_path = self._paths.symbol_pnl_daily_path(symbol)
            if not pnl_daily_path.exists():
                return None
            pnl_daily_df = pl.read_parquet(pnl_daily_path)
        else:
            pnl_daily_df = pl.DataFrame()

        # pnl_df
        if "pnl_df" in requires:
            pnl_path = self._paths.symbol_pnl_path(symbol)
            if not pnl_path.exists():
                return None
            pnl_df = pl.read_parquet(pnl_path)
        else:
            pnl_df = pl.DataFrame()

        # prices (cached in PriceRepository)
        prices = self._price_repo.get_prices_df()
        if prices.is_empty():
            return None

        return SymbolData(
            symbol=symbol,
            trade_df=trade_df,
            pnl_daily_df=pnl_daily_df,
            pnl_df=pnl_df,
            prices=prices,
        )

    def _get_global_context(self) -> GlobalContext:
        """Load cross-symbol context data (cached per runner instance)."""
        if self._global_ctx is not None:
            return self._global_ctx

        try:
            ranking_repo = RankingRepository(self._paths)
            global_ranking = ranking_repo.get_all()
        except RepositoryError:
            global_ranking = pl.DataFrame()

        prices = self._price_repo.get_prices_df()

        self._global_ctx = GlobalContext(
            global_ranking=global_ranking,
            all_symbols=self._paths.list_symbols(),
            prices=prices,
        )
        return self._global_ctx

    def _load_broker_concentrations(self, target_symbol: str) -> pl.DataFrame:
        """Load all pnl/{symbol}.parquet, compute per-broker concentration ratio.

        Returns DataFrame[broker, concentration_ratio, hhi] where:
        - concentration_ratio = |urpnl in target| / total |urpnl| across all stocks
        - hhi = Herfindahl index of broker's cross-stock position weights
        """
        rows = []
        for sym in self._paths.list_symbols():
            path = self._paths.symbol_pnl_path(sym)
            if path.exists():
                try:
                    df = pl.read_parquet(path).select(
                        pl.col("broker").cast(pl.Utf8), "unrealized_pnl"
                    )
                    df = df.with_columns(pl.lit(sym).alias("symbol"))
                    rows.append(df)
                except Exception:
                    continue

        if not rows:
            return pl.DataFrame(schema={
                "broker": pl.Utf8,
                "concentration_ratio": pl.Float64,
                "hhi": pl.Float64,
            })

        all_pnl = pl.concat(rows)

        # Per broker: total absolute unrealized PNL and per-symbol weight
        broker_totals = (
            all_pnl
            .with_columns(pl.col("unrealized_pnl").abs().alias("abs_urpnl"))
            .group_by("broker")
            .agg(pl.col("abs_urpnl").sum().alias("total_abs_urpnl"))
        )

        # Per broker-symbol weight
        broker_sym = (
            all_pnl
            .with_columns(pl.col("unrealized_pnl").abs().alias("abs_urpnl"))
            .join(broker_totals, on="broker", how="inner")
            .filter(pl.col("total_abs_urpnl") > 0)
            .with_columns(
                (pl.col("abs_urpnl") / pl.col("total_abs_urpnl")).alias("weight")
            )
        )

        # Concentration ratio for target symbol
        target_weights = (
            broker_sym
            .filter(pl.col("symbol") == target_symbol)
            .select("broker", pl.col("weight").alias("concentration_ratio"))
        )

        # HHI per broker
        hhi = (
            broker_sym
            .with_columns((pl.col("weight") ** 2).alias("w_sq"))
            .group_by("broker")
            .agg(pl.col("w_sq").sum().alias("hhi"))
        )

        # Join concentration + HHI
        result = hhi.join(target_weights, on="broker", how="left").with_columns(
            pl.col("concentration_ratio").fill_null(0.0)
        )

        return result

    # -------------------------------------------------------------------------
    # Progress and reporting
    # -------------------------------------------------------------------------

    @staticmethod
    def _print_progress(
        done: int, total: int, symbol: str, tag: str,
        n_results: int, t0: float,
    ) -> None:
        """Print progress line to stderr (overwrites previous line)."""
        elapsed = time.time() - t0
        rate = done / elapsed if elapsed > 0 else 0
        eta = (total - done) / rate if rate > 0 else 0
        pct = done / total * 100
        sys.stderr.write(
            f"\r  [{done:>4}/{total}] {pct:5.1f}%  {symbol:<8} {tag:<10}"
            f"  有結果={n_results}  {rate:.1f}/s  ETA {eta:.0f}s"
        )
        sys.stderr.flush()

    @staticmethod
    def _print_fdr_summary(
        results: list[HypothesisResult],
        config: HypothesisConfig,
        fdr: float,
    ) -> None:
        """Apply BH-FDR across all symbols × horizons and print summary."""
        # Collect all p-values with their (symbol, horizon) label
        entries: list[tuple[str, int, float, float]] = []  # symbol, horizon, p, d
        for r in results:
            for hd in r.horizon_details:
                entries.append((
                    r.symbol,
                    hd.horizon,
                    hd.test_result.p_value,
                    hd.test_result.cohens_d,
                ))

        if not entries:
            print("\n  無 p-value 可校正")
            return

        # Build (label, p_value) pairs for BH
        labeled_pvalues = [
            (f"{e[0]}:{e[1]}d", e[2]) for e in entries
        ]
        passed_labels = set(benjamini_hochberg(labeled_pvalues, fdr))

        n_rejected = len(passed_labels)
        n_tests = len(entries)

        print(f"\n--- BH-FDR 校正（α={fdr}）---")
        print(f"  檢定數：{n_tests}（{len(results)} 股 × {len(config.horizons)} horizons）")
        print(f"  通過 FDR：{n_rejected}（{n_rejected/n_tests*100:.1f}%）")

        if n_rejected == 0:
            base_rate = sum(1 for e in entries if e[2] < 0.05) / n_tests * 100
            print(f"  （未校正 p<0.05 比率：{base_rate:.1f}%，"
                  f"隨機期望 5.0%）")
            return

        # Show which symbols × horizons survive FDR
        print(f"\n  通過 FDR 的股票×天期：")
        sig_by_symbol: dict[str, list[tuple[int, float, float]]] = {}
        for sym, h, p, d in entries:
            label = f"{sym}:{h}d"
            if label in passed_labels:
                sig_by_symbol.setdefault(sym, []).append((h, p, d))

        for sym in sorted(sig_by_symbol):
            horizons_str = ", ".join(
                f"{h}d(p={p:.4f},d={d:.3f})"
                for h, p, d in sorted(sig_by_symbol[sym])
            )
            print(f"    {sym}: {horizons_str}")

        print(f"\n  FDR 顯著股票數：{len(sig_by_symbol)}/{len(results)}")

    @staticmethod
    def _empty_result(
        name: str, symbol: str, n_brokers: int, n_events: int, params: dict,
    ) -> HypothesisResult:
        return HypothesisResult(
            strategy_name=name,
            symbol=symbol,
            n_brokers_selected=n_brokers,
            n_events=n_events,
            horizon_details=(),
            conclusion="no_effect",
            params=params,
        )


# Module-level worker for ProcessPoolExecutor (pickle-friendly)
def _run_single_worker(
    root: str, variant: str, symbol: str, strategy_name: str,
) -> HypothesisResult | None:
    """Top-level function for multiprocessing."""
    from pathlib import Path
    paths = DataPaths(root=Path(root), variant=variant)
    runner = HypothesisRunner(paths=paths)
    return runner.run_single(symbol, strategy_name)
