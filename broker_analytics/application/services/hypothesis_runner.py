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
    CVFold,
    DEFAULT_FOLDS,
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

    def run_scan_cv(
        self,
        strategy_name: str,
        folds: tuple[CVFold, ...] = DEFAULT_FOLDS,
        min_folds: int = 3,
        fdr: float = 0.05,
        params_override: dict | None = None,
    ) -> dict:
        """Run rolling window cross-validation scan.

        Runs the full market scan once per fold, then aggregates:
        - Each fold independently evaluated against pass criteria
        - Strategy passes if ≥ min_folds folds pass (majority vote)

        Returns:
            dict with per-fold metrics and overall pass/fail.
        """
        config = get_strategy(strategy_name)
        if params_override:
            merged_params = {**config.params, **params_override}
            config = replace(config, params=merged_params)

        n_folds = len(folds)
        print(f"{'='*60}")
        print(f"【滾動窗口交叉驗證】{config.name}（{config.display_name}）")
        print(f"  策略：{config.description}")
        print(f"  Folds：{n_folds}，通過門檻：≥{min_folds}/{n_folds}")
        print(f"  標準：sig>5% AND FDR≥10 AND dir>60%")
        print(f"{'='*60}")

        requires = config.requires
        all_symbols = self._paths.list_symbols()
        n_total = len(all_symbols)

        # Pre-warm caches
        self._price_repo.get_prices_df()
        ctx = self._get_global_context()
        params_template = {**config.params, "horizons": config.horizons}
        self._inject_global_params(config, params_template)

        fold_metrics = []
        t0_all = time.time()

        for fi, fold in enumerate(folds):
            print(f"\n{'─'*60}")
            print(f"Fold {fi+1}/{n_folds}: {fold.label}")
            print(f"  train ≤ {fold.train_end_date} | test {fold.test_start_date} → {fold.test_end_date}")

            # Per-fold params: inject date window
            fold_params = {
                **params_template,
                "train_end_date": fold.train_end_date,
                "test_start_date": fold.test_start_date,
                "test_end_date": fold.test_end_date,
            }

            results: list[HypothesisResult] = []
            n_skipped = 0
            t0 = time.time()

            for i, symbol in enumerate(all_symbols):
                data = self._load_symbol_data(symbol, requires)
                if data is None:
                    n_skipped += 1
                    continue

                params = {**fold_params}
                result = self._run_pipeline(config, data, ctx, params)

                if result is not None and result.n_events > 0 and result.n_brokers_selected > 0:
                    results.append(result)

                if (i + 1) % 500 == 0 or i + 1 == n_total:
                    self._print_progress(i + 1, n_total, symbol, "", len(results), t0)

            elapsed = time.time() - t0
            sys.stderr.write("\n")

            print(f"  完成：{elapsed:.0f}s，有結果 {len(results)}/{n_total}")

            if not results:
                fold_metrics.append({
                    "fold": fold.label, "sig_rate": 0, "fdr_stocks": 0,
                    "direction_consistency": 0, "passed": False,
                })
                continue

            metrics = self._compute_fdr_metrics(results, config, fdr)
            sig_rate = metrics.get("sig_rate", 0)
            fdr_stocks = metrics.get("fdr_stocks", 0)
            dir_con = metrics.get("direction_consistency", 0)
            passed = sig_rate > 5.0 and fdr_stocks >= 10 and dir_con > 60.0

            print(f"  顯著率：{sig_rate:.1f}%（{'✓' if sig_rate > 5 else '✗'} >5%）")
            print(f"  FDR 股票：{fdr_stocks}（{'✓' if fdr_stocks >= 10 else '✗'} ≥10）")
            print(f"  方向一致：{dir_con:.1f}%（{'✓' if dir_con > 60 else '✗'} >60%）")
            print(f"  → {'✅ PASS' if passed else '❌ FAIL'}")

            fold_metrics.append({
                "fold": fold.label,
                "sig_rate": sig_rate,
                "fdr_stocks": fdr_stocks,
                "direction_consistency": dir_con,
                "n_results": len(results),
                "passed": passed,
                **{k: v for k, v in metrics.items() if k not in ("sig_by_symbol",)},
            })

        # Overall verdict
        n_passed = sum(1 for m in fold_metrics if m["passed"])
        overall_pass = n_passed >= min_folds
        elapsed_all = time.time() - t0_all

        print(f"\n{'='*60}")
        print(f"【交叉驗證結果】{config.name}")
        print(f"  通過 folds：{n_passed}/{n_folds}（門檻 ≥{min_folds}）")
        print(f"  總耗時：{elapsed_all:.0f}s")
        print()
        print(f"  {'Fold':<20} {'Sig%':>6} {'FDR':>5} {'Dir%':>6} {'結果':>6}")
        print(f"  {'─'*45}")
        for m in fold_metrics:
            tag = "✅" if m["passed"] else "❌"
            print(f"  {m['fold']:<20} {m['sig_rate']:>5.1f}% {m['fdr_stocks']:>5} {m['direction_consistency']:>5.1f}% {tag:>6}")
        print()
        print(f"  → 整體：{'✅ PASS（穩健）' if overall_pass else '❌ FAIL（不穩健）'}")

        return {
            "strategy": config.name,
            "folds": fold_metrics,
            "n_passed": n_passed,
            "n_folds": n_folds,
            "min_folds": min_folds,
            "overall_pass": overall_pass,
            "elapsed": elapsed_all,
        }

    def run_export(
        self,
        strategy_name: str,
        params_override: dict | None = None,
    ) -> pl.DataFrame:
        """Export all events for a strategy as Signal Contract v1 CSV format.

        Runs selector + filter for all symbols (skips outcome/baseline/stat_test).
        Returns DataFrame[symbol: Utf8, date: Date, direction: Int8].
        """
        config = get_strategy(strategy_name)
        if params_override:
            merged_params = {**config.params, **params_override}
            config = replace(config, params=merged_params)

        requires = config.requires
        all_symbols = self._paths.list_symbols()
        n_total = len(all_symbols)

        print(f"【信號匯出】{config.name}（{config.display_name}）")
        print(f"  策略：{config.description}")
        print(f"  股票數：{n_total}")
        print()

        self._price_repo.get_prices_df()
        ctx = self._get_global_context()
        params_template = {**config.params, "horizons": config.horizons}
        self._inject_global_params(config, params_template)

        all_events: list[pl.DataFrame] = []
        n_skipped = 0
        n_no_broker = 0
        n_no_event = 0
        t0 = time.time()

        for i, symbol in enumerate(all_symbols):
            data = self._load_symbol_data(symbol, requires)
            if data is None:
                n_skipped += 1
                self._print_progress(i + 1, n_total, symbol, "skip", len(all_events), t0)
                continue

            params = {**params_template}

            # Inject concentration data (same as _run_pipeline)
            if config.name == "concentration":
                cache = params.get("_concentration_cache")
                if cache is not None:
                    params["_broker_concentrations"] = self._concentration_for_symbol(
                        cache, data.symbol
                    )
                else:
                    params["_broker_concentrations"] = self._load_broker_concentrations(
                        data.symbol
                    )

            # Step 1: Select brokers
            brokers = config.selector(data, ctx, params)
            if not brokers:
                n_no_broker += 1
                self._print_progress(i + 1, n_total, symbol, "no_br", len(all_events), t0)
                continue

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

            # Step 2: Filter events (this is all we need)
            events = config.filter(data, brokers, params)

            if len(events) == 0:
                n_no_event += 1
                self._print_progress(i + 1, n_total, symbol, "no_ev", len(all_events), t0)
                continue

            events = events.with_columns(pl.lit(symbol).alias("symbol"))
            all_events.append(events)
            self._print_progress(i + 1, n_total, symbol, f"ev={len(events)}", len(all_events), t0)

        elapsed = time.time() - t0
        sys.stderr.write("\n")
        print(f"\n完成：{elapsed:.0f}s")
        print(f"  跳過：{n_skipped}，無券商：{n_no_broker}，無事件：{n_no_event}")
        print(f"  有事件股票：{len(all_events)}/{n_total}")

        if not all_events:
            return pl.DataFrame(schema={"symbol": pl.Utf8, "date": pl.Date, "direction": pl.Int8})

        result = pl.concat(all_events).select("symbol", "date", "direction").sort("symbol", "date")

        # Deduplicate: same (symbol, date) keep first
        result = result.unique(subset=["symbol", "date"], keep="first")

        total_events = len(result)
        n_long = result.filter(pl.col("direction") == 1).height
        n_short = result.filter(pl.col("direction") == -1).height
        print(f"  總事件數：{total_events}（多={n_long}，空={n_short}）")

        return result

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
        # Inject broker concentration data for concentration strategy (BEFORE selector)
        if config.name == "concentration":
            cache = params.get("_concentration_cache")
            if cache is not None:
                params["_broker_concentrations"] = self._concentration_for_symbol(
                    cache, data.symbol
                )
            else:
                params["_broker_concentrations"] = self._load_broker_concentrations(
                    data.symbol
                )

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

        # Step 2: Filter events
        events = config.filter(data, brokers, params)

        # Enforce date window (for rolling CV)
        _test_start = params.get("test_start_date")
        _test_end = params.get("test_end_date")
        if _test_start and len(events) > 0:
            from datetime import date as _date_cls
            events = events.filter(pl.col("date") >= _date_cls.fromisoformat(_test_start))
        if _test_end and len(events) > 0:
            from datetime import date as _date_cls
            events = events.filter(pl.col("date") <= _date_cls.fromisoformat(_test_end))

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
        """
        if config.name == "concentration":
            # Pre-compute all broker weights + HHI once (reads all pnl files)
            params["_concentration_cache"] = self._build_concentration_cache()

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

    def _build_concentration_cache(self) -> dict:
        """Pre-compute broker weights and HHI across all symbols (once).

        Returns dict with 'broker_sym' (broker, symbol, weight) and 'hhi' (broker, hhi).
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
            return {"broker_sym": pl.DataFrame(), "hhi": pl.DataFrame()}

        all_pnl = pl.concat(rows)

        broker_totals = (
            all_pnl
            .with_columns(pl.col("unrealized_pnl").abs().alias("abs_urpnl"))
            .group_by("broker")
            .agg(pl.col("abs_urpnl").sum().alias("total_abs_urpnl"))
        )

        broker_sym = (
            all_pnl
            .with_columns(pl.col("unrealized_pnl").abs().alias("abs_urpnl"))
            .join(broker_totals, on="broker", how="inner")
            .filter(pl.col("total_abs_urpnl") > 0)
            .with_columns(
                (pl.col("abs_urpnl") / pl.col("total_abs_urpnl")).alias("weight")
            )
            .select("broker", "symbol", "weight")
        )

        hhi = (
            broker_sym
            .with_columns((pl.col("weight") ** 2).alias("w_sq"))
            .group_by("broker")
            .agg(pl.col("w_sq").sum().alias("hhi"))
        )

        return {"broker_sym": broker_sym, "hhi": hhi}

    @staticmethod
    def _concentration_for_symbol(cache: dict, target_symbol: str) -> pl.DataFrame:
        """Derive per-symbol concentration from pre-computed cache (fast)."""
        broker_sym = cache["broker_sym"]
        hhi = cache["hhi"]

        if broker_sym.is_empty():
            return pl.DataFrame(schema={
                "broker": pl.Utf8,
                "concentration_ratio": pl.Float64,
                "hhi": pl.Float64,
            })

        target_weights = (
            broker_sym
            .filter(pl.col("symbol") == target_symbol)
            .select("broker", pl.col("weight").alias("concentration_ratio"))
        )

        result = hhi.join(target_weights, on="broker", how="left").with_columns(
            pl.col("concentration_ratio").fill_null(0.0)
        )
        return result

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
    def _compute_fdr_metrics(
        results: list[HypothesisResult],
        config: HypothesisConfig,
        fdr: float,
    ) -> dict:
        """Apply BH-FDR and return metrics dict.

        Returns:
            dict with keys: n_results, n_tests, sig_before, sig_rate,
            fdr_stocks, direction_consistency, dominant_direction.
            Empty dict if no results.
        """
        sig_before = sum(1 for r in results if r.conclusion == "significant")

        entries: list[tuple[str, int, float, float]] = []
        for r in results:
            for hd in r.horizon_details:
                entries.append((
                    r.symbol, hd.horizon,
                    hd.test_result.p_value, hd.test_result.cohens_d,
                ))

        if not entries:
            return {}

        labeled_pvalues = [
            (f"{e[0]}:{e[1]}d", e[2]) for e in entries
        ]
        passed_labels = set(benjamini_hochberg(labeled_pvalues, fdr))

        sig_by_symbol: dict[str, list[tuple[int, float, float]]] = {}
        for sym, h, p, d in entries:
            if f"{sym}:{h}d" in passed_labels:
                sig_by_symbol.setdefault(sym, []).append((h, p, d))

        fdr_ds = [d for sym, h, p, d in entries
                  if f"{sym}:{h}d" in passed_labels]
        n_pos = sum(1 for d in fdr_ds if d > 0)
        n_neg = sum(1 for d in fdr_ds if d < 0)
        consistency = max(n_pos, n_neg) / len(fdr_ds) * 100 if fdr_ds else 0.0
        dominant = "positive" if n_pos >= n_neg else "negative"

        return {
            "n_results": len(results),
            "n_tests": len(entries),
            "sig_before": sig_before,
            "sig_rate": sig_before / len(results) * 100 if results else 0.0,
            "fdr_stocks": len(sig_by_symbol),
            "fdr_rejected": len(passed_labels),
            "direction_consistency": consistency,
            "dominant_direction": dominant,
            "sig_by_symbol": sig_by_symbol,
        }

    @staticmethod
    def _print_fdr_summary(
        results: list[HypothesisResult],
        config: HypothesisConfig,
        fdr: float,
    ) -> dict:
        """Apply BH-FDR across all symbols × horizons, print summary, return metrics."""
        metrics = HypothesisRunner._compute_fdr_metrics(results, config, fdr)
        if not metrics:
            print("\n  無 p-value 可校正")
            return metrics

        n_tests = metrics["n_tests"]
        n_rejected = metrics["fdr_rejected"]

        print(f"\n--- BH-FDR 校正（α={fdr}）---")
        print(f"  檢定數：{n_tests}（{len(results)} 股 × {len(config.horizons)} horizons）")
        print(f"  通過 FDR：{n_rejected}（{n_rejected/n_tests*100:.1f}%）")

        if n_rejected == 0:
            entries = []
            for r in results:
                for hd in r.horizon_details:
                    entries.append(hd.test_result.p_value)
            base_rate = sum(1 for p in entries if p < 0.05) / len(entries) * 100
            print(f"  （未校正 p<0.05 比率：{base_rate:.1f}%，"
                  f"隨機期望 5.0%）")
            return metrics

        sig_by_symbol = metrics["sig_by_symbol"]
        print(f"\n  通過 FDR 的股票×天期：")
        for sym in sorted(sig_by_symbol):
            horizons_str = ", ".join(
                f"{h}d(p={p:.4f},d={d:.3f})"
                for h, p, d in sorted(sig_by_symbol[sym])
            )
            print(f"    {sym}: {horizons_str}")

        print(f"\n  FDR 顯著股票數：{metrics['fdr_stocks']}/{len(results)}")

        consistency = metrics["direction_consistency"]
        dominant = "正(事件後漲)" if metrics["dominant_direction"] == "positive" else "負(事件後跌)"
        fdr_ds = []
        for sym, entries_list in sig_by_symbol.items():
            for h, p, d in entries_list:
                fdr_ds.append(d)
        n_pos = sum(1 for d in fdr_ds if d > 0)
        n_neg = sum(1 for d in fdr_ds if d < 0)
        print(f"  方向一致性：{consistency:.1f}%（{dominant}，正{n_pos}/負{n_neg}）")

        return metrics

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
