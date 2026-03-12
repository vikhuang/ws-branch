"""Command Line Interface for Broker Analytics.

Provides CLI access to analytics functions:
- ranking: Show broker ranking
- query: Query specific broker
- symbol: Analyze smart money flow for a stock
- event-study: Smart money event study
- signal: Per-stock signal analysis pipeline
- scan: Full-market signal screening with FDR
- export: Export signals to CSV
- verify: Verify data integrity

Usage:
    python -m broker_analytics ranking [--output FILE]
    python -m broker_analytics query BROKER
    python -m broker_analytics symbol SYMBOL [--detail WINDOW]
    python -m broker_analytics event-study SYMBOL [--top-k 20] [--window 5]
    python -m broker_analytics signal SYMBOL [--train-start DATE]
    python -m broker_analytics scan [--min-turnover N] [--fdr 0.01]
    python -m broker_analytics export [--symbols SYM1,SYM2]
    python -m broker_analytics verify
"""

import argparse
import sys
from pathlib import Path

from broker_analytics import __version__
from broker_analytics.infrastructure import DEFAULT_PATHS, DataPaths
from broker_analytics.application import (
    RankingService,
    RankingReportConfig,
    BrokerAnalyzer,
    SymbolAnalyzer,
    RollingRankingService,
    EventStudyService,
)
from broker_analytics.domain.event_detection import EventConfig


def cmd_ranking(args: argparse.Namespace) -> int:
    """Show broker ranking."""
    mode = "" if args.no_merge else "（合併版）"
    print(f"PNL Analytics v{__version__} {mode}")
    print("=" * 60)

    config = RankingReportConfig(
        output_dir=Path(args.output).parent if args.output else Path("."),
        output_formats=tuple(args.formats.split(",")),
    )

    service = RankingService(paths=args.paths, config=config)

    # Get summary
    summary = service.get_summary()
    print(f"券商數：{summary['broker_count']}")
    print()

    # Get ranking
    df = service.get_ranking()

    # Save if requested
    if args.output != "ranking_report" or args.save:
        base_name = Path(args.output).stem if args.output else "ranking_report"
        saved = service.save_report(df, base_name)
        for path in saved:
            print(f"已輸出：{path}")
        print()

    # Show top/bottom
    print("【PNL 排名 Top 10】")
    print(f"{'排名':<4} {'券商':<8} {'名稱':<12} {'PNL':>12} {'Alpha':>14}")
    print("-" * 56)

    for row in df.head(10).iter_rows(named=True):
        pnl_yi = row["total_pnl"] / 1e8
        alpha_m = row["timing_alpha"] / 1e6
        name = row.get("name", "")[:10]
        print(f"{row['rank']:<4} {row['broker']:<8} {name:<12} "
              f"{pnl_yi:>+10.2f}億 {alpha_m:>+12.1f}M")

    print()
    print("【PNL 排名 Bottom 5】")
    print("-" * 56)

    for row in df.tail(5).iter_rows(named=True):
        pnl_yi = row["total_pnl"] / 1e8
        alpha_m = row["timing_alpha"] / 1e6
        name = row.get("name", "")[:10]
        print(f"{row['rank']:<4} {row['broker']:<8} {name:<12} "
              f"{pnl_yi:>+10.2f}億 {alpha_m:>+12.1f}M")

    return 0


def cmd_query(args: argparse.Namespace) -> int:
    """Query specific broker."""
    analyzer = BrokerAnalyzer(paths=args.paths)
    result = analyzer.analyze(args.broker)

    if result is None:
        print(f"找不到券商：{args.broker}")
        return 1

    print(f"【{result.broker}】{result.name}")
    print("=" * 50)
    print()

    print("【排名】")
    print(f"  全市場排名：第 {result.rank} 名")
    print()

    print("【損益】")
    print(f"  已實現 PNL：{result.realized_pnl/1e8:+.2f} 億")
    print(f"  未實現 PNL：{result.unrealized_pnl/1e8:+.2f} 億")
    print(f"  總 PNL：{result.total_pnl/1e8:+.2f} 億")
    print()

    print("【交易統計】")
    print(f"  買入金額：{result.total_buy_amount/1e8:.2f} 億")
    print(f"  賣出金額：{result.total_sell_amount/1e8:.2f} 億")
    print(f"  總成交金額：{result.total_amount/1e8:.2f} 億")
    print(f"  交易方向：{result.direction}")
    print()

    print("【擇時能力】")
    alpha_m = result.timing_alpha / 1e6
    print(f"  Timing Alpha：{alpha_m:+.1f}M")
    if result.timing_alpha > 0:
        print("  解讀：買超後漲、賣超後跌（擇時正確）")
    elif result.timing_alpha < 0:
        print("  解讀：買超後跌、賣超後漲（擇時錯誤）")
    else:
        print("  解讀：無明顯擇時能力")

    # Get symbol breakdown if requested
    if args.breakdown:
        print()
        print("【股票明細】")
        print("-" * 60)
        breakdown = analyzer.get_symbol_breakdown(args.broker)
        if len(breakdown) > 0:
            print(f"{'股票':<8} {'交易日':<6} {'買入(億)':<10} {'賣出(億)':<10} {'淨部位':<10}")
            for row in breakdown.head(20).iter_rows(named=True):
                print(f"{row['symbol']:<8} {row['trading_days']:<6} "
                      f"{row['buy_amount']/1e8:<10.2f} {row['sell_amount']/1e8:<10.2f} "
                      f"{row['net_shares']:<10,}")
            if len(breakdown) > 20:
                print(f"... 還有 {len(breakdown) - 20} 檔股票")
        else:
            print("  無交易記錄")

    return 0


def cmd_symbol(args: argparse.Namespace) -> int:
    """Analyze smart money flow for a stock."""
    analyzer = SymbolAnalyzer(paths=args.paths)
    rolling_years = getattr(args, "years", None)
    result = analyzer.analyze(args.symbol, rolling_years=rolling_years)

    if result is None:
        print(f"找不到股票：{args.symbol}")
        return 1

    ranking_label = f"（{rolling_years}年滾動排名）" if rolling_years else ""
    print(f"【{result.symbol}】@ {result.last_date} {ranking_label}")
    print("=" * 60)

    # Summary table
    print()
    print("【買賣力道摘要】")
    print(f"{'窗口':>4}  {'買方力道':>8}  {'賣方力道':>8}  {'活躍券商':>8}  {'已實現(億)':>10}  {'未實現(億)':>10}")
    print("-" * 62)

    for s in result.signals:
        r_yi = s.realized_pnl / 1e8
        u_yi = s.unrealized_pnl / 1e8
        print(f"{s.window:>3}日  {s.buy_rank_sum:>8,}  {s.sell_rank_sum:>8,}  "
              f"{s.n_active_brokers:>8}  {r_yi:>+10,.0f}  {u_yi:>+10,.0f}")

    print()
    print("力道 = 淨買(賣)超 TOP 15 的 PNL 排名加總")
    print("理論最小 120（前15名都在買/賣）  理論最大 13,650")

    # Detail for specified window
    detail_window = args.detail
    buy_top, sell_top = analyzer.get_top_brokers(
        args.symbol, window=detail_window, rolling_years=rolling_years,
    )

    print()
    print(f"【近 {detail_window} 日 淨買超 TOP 15】")
    print(f"{'券商':<8} {'名稱':<12} {'淨買超(張)':>14} {'PNL排名':>8}")
    print("-" * 46)
    for row in buy_top.iter_rows(named=True):
        name = (row.get("name") or "")[:10]
        rank = row["rank"] if row["rank"] is not None else "-"
        print(f"{row['broker']:<8} {name:<12} {row['net_buy']:>+14,} {rank:>8}")

    print()
    print(f"【近 {detail_window} 日 淨賣超 TOP 15】")
    print(f"{'券商':<8} {'名稱':<12} {'淨賣超(張)':>14} {'PNL排名':>8}")
    print("-" * 46)
    for row in sell_top.iter_rows(named=True):
        name = (row.get("name") or "")[:10]
        rank = row["rank"] if row["rank"] is not None else "-"
        print(f"{row['broker']:<8} {name:<12} {row['net_buy']:>+14,} {rank:>8}")

    return 0


def cmd_rolling(args: argparse.Namespace) -> int:
    """Show rolling PNL ranking."""
    from datetime import date, timedelta

    # Determine query date
    if args.date:
        query_date = date.fromisoformat(args.date)
    else:
        # Use latest date from pnl_daily data
        import polars as pl
        files = sorted(args.paths.pnl_daily_dir.glob("*.parquet"))
        if not files:
            print("錯誤：找不到 pnl_daily 資料，請先執行 pnl_engine.py")
            return 1
        sample = pl.read_parquet(files[0], columns=["date"])
        query_date = sample["date"].max()

    n_display = args.n

    service = RollingRankingService(paths=args.paths)
    df = service.compute(query_date, window_years=args.years)

    if len(df) == 0:
        print("無排名資料")
        return 1

    try:
        window_start = query_date.replace(year=query_date.year - args.years)
    except ValueError:
        window_start = date(
            query_date.year - args.years, query_date.month, query_date.day - 1,
        )

    print(f"【{args.years} 年滾動 PNL 排名】")
    print(f"  窗口：{window_start} ~ {query_date}")
    n_files = len(list(args.paths.pnl_daily_dir.glob("*.parquet")))
    print(f"  股票數：{n_files:,}")
    print()

    print(f"{'排名':<6} {'券商':<8} {'名稱':<14} {'總PNL':>14} {'已實現':>14} {'未實現':>14}")
    print("-" * 74)

    for row in df.head(n_display).iter_rows(named=True):
        total_yi = row["total_pnl"] / 1e8
        real_yi = row["realized_pnl"] / 1e8
        unreal_yi = row["unrealized_pnl"] / 1e8
        name = (row.get("name") or "")[:12]
        print(f"{row['rank']:<6} {row['broker']:<8} {name:<14} "
              f"{total_yi:>+12.2f}億 {real_yi:>+12.2f}億 {unreal_yi:>+12.2f}億")

    if len(df) > n_display:
        print(f"... 共 {len(df)} 家券商")

    # Export to xlsx
    if args.xlsx:
        import polars as pl

        out_dir = args.paths.derived_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        filename = f"rolling_ranking_{query_date.isoformat()}.xlsx"
        out_path = out_dir / filename

        # Convert to 億 for readability
        export_df = df.with_columns(
            (pl.col("total_pnl") / 1e8).alias("總PNL(億)"),
            (pl.col("realized_pnl") / 1e8).alias("已實現(億)"),
            (pl.col("unrealized_pnl") / 1e8).alias("未實現(億)"),
        ).select(
            pl.col("rank").alias("排名"),
            pl.col("broker").alias("券商"),
            *([pl.col("name").alias("名稱")] if "name" in df.columns else []),
            "總PNL(億)", "已實現(億)", "未實現(億)",
        )

        export_df.write_excel(
            out_path,
            worksheet="排名",
            float_precision=2,
        )
        print()
        print(f"已匯出：{out_path}")

    return 0


def cmd_event_study(args: argparse.Namespace) -> int:
    """Run event study for a stock."""
    from broker_analytics.domain.event_detection import EventConfig

    config = EventConfig(
        top_k=args.top_k,
        window_days=args.window,
        threshold_sigma=args.threshold,
    )
    horizons = tuple(int(h) for h in args.horizons.split(","))

    service = EventStudyService(paths=args.paths)
    report = service.run(
        symbol=args.symbol,
        config=config,
        horizons=horizons,
        run_robustness=not args.no_robustness,
    )

    if report is None:
        print(f"找不到股票或事件不足：{args.symbol}")
        return 1

    # Header
    mode = "" if args.no_merge else "（合併版）"
    print("=" * 70)
    print(f"EVENT STUDY: {report.symbol} {mode}")
    print(f"Config: top_k={config.top_k}, window={config.window_days}d, "
          f"threshold={config.threshold_sigma}σ")
    print(f"Date range: {report.date_range[0]} ~ {report.date_range[1]}")
    print("=" * 70)

    # Events summary
    print()
    print(f"Events: {report.n_accumulation} accumulation + "
          f"{report.n_distribution} distribution = {report.n_events} total")

    # Step 1: Threshold calibration
    s = report.threshold_shape
    print()
    print(f"THRESHOLD CALIBRATION (per-broker z-scores):")
    print(f"  Skewness: {s.skewness:+.3f}  Excess kurtosis: {s.excess_kurtosis:+.3f}")
    print(f"  Beyond ±{config.threshold_sigma}σ: {s.pct_beyond:.1f}% "
          f"(normal expects {s.pct_expected:.1f}%)")
    fat = s.pct_beyond / s.pct_expected if s.pct_expected > 0 else 0
    if fat > 1.5:
        print(f"  Fat tails: {fat:.1f}x more triggers than normal assumption")
    print(f"  +{config.threshold_sigma}σ = top {100 - s.threshold_percentile:.1f}%")

    # Step 2-4: Per-direction results
    for dr in [report.accumulation, report.distribution]:
        if dr is None:
            continue
        print()
        label = "ACCUMULATION (top-K large buys)" if dr.label == "accumulation" \
            else "DISTRIBUTION (top-K large sells)"
        print(f"{label}: {dr.n_events} events")
        print(f"{'Horizon':>7} | {'Mean':>9} {'Median':>9} | "
              f"{'Uncond':>9} | {'t-stat':>6} {'perm-p':>7} {'Cohen d':>8} | Sig")
        print(f"{'-'*7}-+-{'-'*9}-{'-'*9}-+-{'-'*9}-+-"
              f"{'-'*6}-{'-'*7}-{'-'*8}-+----")

        for hr in dr.horizons:
            sig = " *" if hr.significant else ""
            print(f"{hr.horizon:>5}d  | {hr.cond_mean:>+9.1f} {hr.cond_median:>+9.1f} | "
                  f"{hr.uncond_mean:>+9.1f} | {hr.t_stat:>6.2f} {hr.perm_p:>7.4f} "
                  f"{hr.cohens_d:>+8.2f} |{sig}")

        if dr.significant_horizons:
            h_str = ", ".join(f"{h}d" for h in dr.significant_horizons)
            print(f"  Significant: {h_str}")

        # Step 5: Decay curve
        if dr.decay_curve:
            print(f"  Decay curve (direction-adjusted CAR, bps):")
            curve_str = "  "
            for d in range(len(dr.decay_curve)):
                if (d + 1) in [1, 2, 3, 5, 10, 15, 20] and d < len(dr.decay_curve):
                    curve_str += f"d{d+1}:{dr.decay_curve[d]:+.0f}  "
            print(curve_str)

    # Robustness
    if report.robustness is not None:
        r = report.robustness
        print()
        placebo_mark = "NOT significant" if not r.placebo_significant else "significant"
        print(f"PLACEBO: {placebo_mark}")

    # Conclusion
    print()
    print("=" * 70)
    conclusion_map = {
        "significant": "SIGNIFICANT",
        "marginal": "MARGINAL",
        "no_effect": "NO EFFECT",
    }
    label = conclusion_map[report.conclusion]
    print(f"CONCLUSION: {label}")
    print("=" * 70)

    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    """Verify data integrity."""
    from broker_analytics.infrastructure.repositories import RankingRepository

    print("【數據驗證】")
    print("=" * 50)

    errors = []

    # 1. Check data files exist
    print("\n1. 檢查資料檔案...")
    missing = args.paths.validate()
    if missing:
        for m in missing:
            print(f"  ✗ 缺少：{m}")
            errors.append(f"Missing file: {m}")
    else:
        print("  ✓ 資料目錄存在")

    # 2. Check broker_ranking.parquet
    print("\n2. 檢查排名資料...")
    try:
        ranking_repo = RankingRepository(args.paths)
        df = ranking_repo.get_all()
        broker_count = len(df)
        print(f"  券商數：{broker_count}")

        if broker_count >= 900:
            print("  ✓ 券商數量正常")
        else:
            print("  ⚠ 券商數量偏低")
    except Exception as e:
        print(f"  ✗ 錯誤：{e}")
        errors.append(str(e))

    # 3. Zero-sum check
    print("\n3. 零和檢驗...")
    try:
        total_pnl = df["total_pnl"].sum()
        print(f"  總 PNL：{total_pnl/1e8:+.4f}億")

        if abs(total_pnl) < abs(df["total_pnl"].max()) * 0.05:
            print("  ✓ 零和檢驗通過（相對誤差 < 5%）")
        else:
            print("  ⚠ 零和偏差較大")
    except Exception as e:
        print(f"  ✗ 錯誤：{e}")
        errors.append(str(e))

    # 4. Check daily_summary files
    print("\n4. 檢查交易資料...")
    symbols = args.paths.list_symbols()
    print(f"  股票數：{len(symbols)}")
    if len(symbols) >= 2800:
        print("  ✓ 股票數量正常")
    elif len(symbols) > 0:
        print("  ⚠ 股票數量偏低")
    else:
        print("  ✗ 無交易資料")
        errors.append("No trade data found")

    # 5. Check price data
    print("\n5. 檢查價格資料...")
    try:
        from ws_core import prices as ws_prices
        import polars as pl
        price_df = ws_prices(columns=["coid", "mdate", "close_d"], start="2021-01-01")
        print(f"  價格記錄數：{len(price_df):,}")
        print(f"  股票數：{price_df['coid'].n_unique()}")
        print(f"  日期範圍：{price_df['mdate'].min()} ~ {price_df['mdate'].max()}")
        print("  ✓ 價格資料存在 (ws-core)")
    except Exception as e:
        print(f"  ✗ 無法讀取價格資料: {e}")
        errors.append("Missing price data")

    # Summary
    print("\n" + "=" * 50)
    if errors:
        print(f"❌ 發現 {len(errors)} 個問題")
        return 1
    else:
        print("✅ 所有驗證通過")
        return 0


def cmd_signal(args: argparse.Namespace) -> int:
    """Run per-stock signal analysis pipeline."""
    from broker_analytics.application.services.signal_report import run_pipeline

    run_pipeline(
        args.symbol,
        train_start=args.train_start,
        train_end=args.train_end,
        test_start=args.test_start,
        test_end=args.test_end,
        paths=args.paths,
    )
    return 0


def cmd_scan(args: argparse.Namespace) -> int:
    """Run full-market signal screening."""
    from broker_analytics.application.services.market_scan import ScanConfig, run_scan

    config = ScanConfig(
        min_turnover=args.min_turnover,
        cost=args.cost,
        fdr_threshold=args.fdr,
        min_test_days=args.min_test_days,
        train_start=args.train_start,
        train_end=args.train_end,
        test_start=args.test_start,
        test_end=args.test_end,
        workers=args.workers,
    )
    run_scan(config, paths=args.paths)
    return 0


def cmd_hypothesis(args: argparse.Namespace) -> int:
    """Run composable hypothesis test."""
    from broker_analytics.application.services.hypothesis_runner import HypothesisRunner
    from broker_analytics.domain.hypothesis.registry import list_strategies, STRATEGIES

    runner = HypothesisRunner(paths=args.paths)

    if args.list:
        print("可用策略：")
        print(f"  {'名稱':<24} {'中文':<12} 說明")
        print("  " + "-" * 70)
        for name in list_strategies():
            cfg = STRATEGIES[name]
            print(f"  {name:<24} {cfg.display_name:<12} {cfg.description}")
        return 0

    # Parse params override (needed for both scan and single modes)
    params_override = None
    if args.params:
        params_override = _parse_hypothesis_params(args.params)

    # Export mode: generate Signal Contract CSV
    if args.export:
        if not args.strategy:
            print("Error: --export 需指定 -s <strategy>")
            return 1
        import polars as pl
        from pathlib import Path

        strategies = [s.strip() for s in args.strategy.split(",")]
        signals_dir = args.paths.data_dir / "signals"
        signals_dir.mkdir(exist_ok=True)

        for strategy_name in strategies:
            events = runner.run_export(strategy_name, params_override=params_override)
            if events.is_empty():
                print(f"  {strategy_name}: 無事件，跳過")
                continue

            # Convert direction Int8 → string for Signal Contract v1
            out = events.with_columns(
                pl.when(pl.col("direction") == 1)
                .then(pl.lit("long"))
                .otherwise(pl.lit("short"))
                .alias("direction"),
                pl.col("date").cast(pl.Utf8),
            ).select("symbol", "date", "direction")

            path = signals_dir / f"{strategy_name}.csv"
            out.write_csv(path)
            print(f"  → {path}（{len(out)} 筆）")

        return 0

    # CV scan mode: rolling window cross-validation
    if args.scan and args.cv:
        runner.run_scan_cv(
            args.strategy,
            min_folds=args.min_folds,
            fdr=args.fdr,
            params_override=params_override,
        )
        return 0

    # Scan mode: all symbols with FDR
    if args.scan:
        runner.run_scan(args.strategy, fdr=args.fdr, params_override=params_override)
        return 0

    if not args.symbol and not args.batch:
        print("Error: 需指定 symbol、--batch 或 --scan")
        return 1

    # Batch mode
    if args.batch:
        symbols = [s.strip() for s in args.batch.split(",")]
        results = runner.run_batch(symbols, args.strategy, workers=args.workers)
        _print_batch_results(results)
        return 0

    if args.all:
        results = runner.run_all_strategies(args.symbol)
        _print_all_strategies_results(args.symbol, results)
        return 0

    result = runner.run_single(
        args.symbol, args.strategy, params_override=params_override,
    )
    if result is None:
        print(f"資料不足：{args.symbol}")
        return 1

    _print_hypothesis_result(result)
    return 0


def _parse_hypothesis_params(param_args: list[str]) -> dict:
    """Parse ['key1=val1', 'key2=val2'] into dict with numeric conversion.

    Values containing commas are kept as strings (e.g., cluster=2330,3711).
    """
    params = {}
    for pair in param_args:
        if "=" not in pair:
            continue
        k, v = pair.split("=", 1)
        v = v.strip()
        try:
            params[k.strip()] = int(v)
        except ValueError:
            try:
                params[k.strip()] = float(v)
            except ValueError:
                params[k.strip()] = v
    return params


def _print_hypothesis_result(result) -> None:
    """Print single hypothesis result."""
    conclusion_map = {
        "significant": "✓ 顯著",
        "marginal": "◐ 邊際顯著",
        "no_effect": "✗ 無效果",
    }
    print(f"\n【假說檢定】{result.strategy_name}")
    print(f"  股票：{result.symbol}")
    print(f"  選定券商：{result.n_brokers_selected}")
    print(f"  事件數：{result.n_events}")
    print(f"  結論：{conclusion_map.get(result.conclusion, result.conclusion)}")

    if result.horizon_details:
        print(f"\n  {'Horizon':>8} {'事件均值':>10} {'基準均值':>10} {'Cohen d':>8} {'p_corr':>8} {'顯著':>4}")
        print("  " + "-" * 56)
        for d in result.horizon_details:
            sig = "✓" if d.test_result.significant else ""
            print(
                f"  {d.horizon:>6}d {d.cond_mean:>10.1f} {d.uncond_mean:>10.1f}"
                f" {d.test_result.cohens_d:>8.3f} {d.test_result.p_value_corrected:>8.4f}"
                f" {sig:>4}"
            )


def _print_all_strategies_results(symbol: str, results: dict) -> None:
    """Print results for all strategies on one symbol."""
    conclusion_map = {
        "significant": "✓ 顯著",
        "marginal": "◐ 邊際",
        "no_effect": "✗ 無效",
    }
    print(f"\n【全策略假說檢定】{symbol}")
    print(f"  {'策略':<24} {'券商':>6} {'事件':>6} {'結論':<12}")
    print("  " + "-" * 56)
    for name, r in results.items():
        if r is None:
            print(f"  {name:<24} {'—':>6} {'—':>6} 資料不足")
        else:
            c = conclusion_map.get(r.conclusion, r.conclusion)
            print(f"  {name:<24} {r.n_brokers_selected:>6} {r.n_events:>6} {c}")


def _print_batch_results(results: list) -> None:
    """Print batch results for one strategy across symbols."""
    if not results:
        print("無結果")
        return
    conclusion_map = {
        "significant": "✓",
        "marginal": "◐",
        "no_effect": "✗",
    }
    print(f"\n【批次假說檢定】{results[0].strategy_name}")
    print(f"  {'股票':>8} {'券商':>6} {'事件':>6} {'結論':>4}")
    print("  " + "-" * 30)
    for r in sorted(results, key=lambda x: x.symbol):
        c = conclusion_map.get(r.conclusion, "?")
        print(f"  {r.symbol:>8} {r.n_brokers_selected:>6} {r.n_events:>6} {c:>4}")

    sig_count = sum(1 for r in results if r.conclusion == "significant")
    mar_count = sum(1 for r in results if r.conclusion == "marginal")
    print(f"\n  顯著：{sig_count}/{len(results)}  邊際：{mar_count}/{len(results)}")


def cmd_export(args: argparse.Namespace) -> int:
    """Export signals to CSV."""
    from broker_analytics.application.services.signal_export import run_export

    symbols = None
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",")]

    run_export(
        symbols=symbols,
        trade_start=args.trade_start,
        trade_end=args.trade_end,
        output=args.output,
        workers=args.workers,
        train_start=args.train_start,
        train_end=args.train_end,
        test_start=args.test_start,
        test_end=args.test_end,
        paths=args.paths,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    """Main entry point for CLI."""
    parser = argparse.ArgumentParser(
        prog="broker_analytics",
        description="PNL Analytics - Broker Performance Analysis",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    parser.add_argument(
        "--no-merge",
        action="store_true",
        help="使用未合併版券商身份（預設使用合併版）",
    )

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # ranking command
    ranking_parser = subparsers.add_parser("ranking", help="Show broker ranking")
    ranking_parser.add_argument(
        "-o", "--output",
        default="ranking_report",
        help="Output filename (without extension)",
    )
    ranking_parser.add_argument(
        "-f", "--formats",
        default="csv,parquet",
        help="Output formats (comma-separated)",
    )
    ranking_parser.add_argument(
        "--save",
        action="store_true",
        help="Save output files",
    )

    # query command
    query_parser = subparsers.add_parser("query", help="Query broker")
    query_parser.add_argument("broker", help="Broker code (e.g., 1440)")
    query_parser.add_argument(
        "--breakdown",
        action="store_true",
        help="Show per-symbol breakdown",
    )

    # symbol command
    symbol_parser = subparsers.add_parser("symbol", help="Analyze symbol smart money")
    symbol_parser.add_argument("symbol", help="Stock symbol (e.g., 2330)")
    symbol_parser.add_argument(
        "--detail", type=int, default=1,
        help="Window (trading days) for detail view (default: 1)",
    )
    symbol_parser.add_argument(
        "--years", type=int, default=None,
        help="Use N-year rolling window PNL ranking (default: full period)",
    )

    # rolling command
    rolling_parser = subparsers.add_parser("rolling", help="Rolling PNL ranking")
    rolling_parser.add_argument(
        "--date",
        help="Query date (YYYY-MM-DD, default: latest in data)",
    )
    rolling_parser.add_argument(
        "--years", type=int, default=3,
        help="Window size in years (default: 3)",
    )
    rolling_parser.add_argument(
        "-n", type=int, default=10,
        help="Number of top brokers to show (default: 10)",
    )
    rolling_parser.add_argument(
        "--xlsx", action="store_true",
        help="Export full ranking to Excel (.xlsx)",
    )

    # event-study command
    event_parser = subparsers.add_parser(
        "event-study", help="Smart money event study",
    )
    event_parser.add_argument("symbol", help="Stock symbol (e.g., 6285)")
    event_parser.add_argument(
        "--top-k", type=int, default=20,
        help="Number of top PNL brokers to track (default: 20)",
    )
    event_parser.add_argument(
        "--window", type=int, default=5,
        help="Rolling window in trading days (default: 5)",
    )
    event_parser.add_argument(
        "--threshold", type=float, default=2.0,
        help="Event threshold in σ (default: 2.0)",
    )
    event_parser.add_argument(
        "--horizons", default="1,5,10,20",
        help="Forward return horizons, comma-separated (default: 1,5,10,20)",
    )
    event_parser.add_argument(
        "--no-robustness", action="store_true",
        help="Skip robustness checks",
    )

    # signal command
    signal_parser = subparsers.add_parser(
        "signal", help="Per-stock signal analysis pipeline",
    )
    signal_parser.add_argument("symbol", help="Stock symbol (e.g., 2330)")
    signal_parser.add_argument("--train-start", default="2023-01-01")
    signal_parser.add_argument("--train-end", default="2024-06-30")
    signal_parser.add_argument("--test-start", default="2024-07-01")
    signal_parser.add_argument("--test-end", default="2025-12-31")

    # scan command
    scan_parser = subparsers.add_parser(
        "scan", help="Full-market signal screening with FDR",
    )
    scan_parser.add_argument(
        "--min-turnover", type=float, default=2e8,
        help="Min avg daily turnover in NTD (default: 200000000)",
    )
    scan_parser.add_argument(
        "--cost", type=float, default=0.005,
        help="Cost per trade (default: 0.005)",
    )
    scan_parser.add_argument(
        "--fdr", type=float, default=0.01,
        help="FDR threshold (default: 0.01)",
    )
    scan_parser.add_argument(
        "--min-test-days", type=int, default=250,
        help="Min test days (default: 250)",
    )
    scan_parser.add_argument(
        "--workers", type=int, default=12,
        help="Parallel workers (default: 12)",
    )
    scan_parser.add_argument("--train-start", default="2023-01-01")
    scan_parser.add_argument("--train-end", default="2024-06-30")
    scan_parser.add_argument("--test-start", default="2024-07-01")
    scan_parser.add_argument("--test-end", default="2025-12-31")

    # export command
    export_parser = subparsers.add_parser(
        "export", help="Export signals to CSV",
    )
    export_parser.add_argument(
        "--symbols", default=None,
        help="Comma-separated symbols (default: all FDR-passing)",
    )
    export_parser.add_argument("--trade-start", default="2025-01-02")
    export_parser.add_argument("--trade-end", default="2025-12-31")
    export_parser.add_argument("-o", "--output", default=None)
    export_parser.add_argument("--workers", type=int, default=12)
    export_parser.add_argument("--train-start", default="2023-01-01")
    export_parser.add_argument("--train-end", default="2024-06-30")
    export_parser.add_argument("--test-start", default="2024-07-01")
    export_parser.add_argument("--test-end", default="2025-12-31")

    # hypothesis command
    hyp_parser = subparsers.add_parser(
        "hypothesis", help="Run composable hypothesis tests",
    )
    hyp_parser.add_argument("symbol", nargs="?", help="Stock symbol")
    hyp_parser.add_argument(
        "-s", "--strategy", default="contrarian_broker",
        help="Strategy name (default: contrarian_broker)",
    )
    hyp_parser.add_argument(
        "--all", action="store_true",
        help="Run all 9 strategies on one symbol",
    )
    hyp_parser.add_argument(
        "--list", action="store_true",
        help="List available strategies",
    )
    hyp_parser.add_argument(
        "--params", nargs="*", default=None,
        help="Override params as key=value pairs (space-separated, values may contain commas)",
    )
    hyp_parser.add_argument(
        "--batch", default=None,
        help="Run on multiple symbols (comma-separated)",
    )
    hyp_parser.add_argument(
        "--workers", type=int, default=1,
        help="Parallel workers for batch mode (default: 1)",
    )
    hyp_parser.add_argument(
        "--scan", action="store_true",
        help="Run strategy on ALL symbols with FDR correction",
    )
    hyp_parser.add_argument(
        "--fdr", type=float, default=0.05,
        help="FDR threshold for scan mode (default: 0.05)",
    )
    hyp_parser.add_argument(
        "--cv", action="store_true",
        help="Use rolling window cross-validation (5 folds)",
    )
    hyp_parser.add_argument(
        "--min-folds", type=int, default=3,
        help="Minimum folds to pass for CV mode (default: 3)",
    )
    hyp_parser.add_argument(
        "--export", action="store_true",
        help="Export Signal Contract CSV (comma-separated strategies with -s)",
    )

    # verify command
    subparsers.add_parser("verify", help="Verify data integrity")

    args = parser.parse_args(argv)
    args.paths = DEFAULT_PATHS if args.no_merge else DataPaths(variant="merged")

    if args.command is None:
        parser.print_help()
        return 0

    commands = {
        "ranking": cmd_ranking,
        "query": cmd_query,
        "symbol": cmd_symbol,
        "rolling": cmd_rolling,
        "event-study": cmd_event_study,
        "signal": cmd_signal,
        "scan": cmd_scan,
        "export": cmd_export,
        "verify": cmd_verify,
        "hypothesis": cmd_hypothesis,
    }

    return commands[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
