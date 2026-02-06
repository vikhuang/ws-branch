"""Command Line Interface for PNL Analytics.

Provides unified CLI access to all analytics functions:
- ranking: Generate broker ranking report
- query: Query specific broker metrics
- scorecard: Generate broker scorecard
- verify: Verify data integrity

Usage:
    python -m pnl_analytics ranking [--output FILE]
    python -m pnl_analytics query BROKER
    python -m pnl_analytics scorecard BROKER
    python -m pnl_analytics verify
"""

import argparse
import sys
from pathlib import Path

from pnl_analytics import __version__
from pnl_analytics.infrastructure import DEFAULT_PATHS
from pnl_analytics.application import RankingService, RankingReportConfig


def cmd_ranking(args: argparse.Namespace) -> int:
    """Generate broker ranking report."""
    print(f"PNL Analytics v{__version__}")
    print("=" * 60)

    # Configure output
    config = RankingReportConfig(
        min_trading_days=args.min_days,
        permutation_count=args.permutations,
        output_dir=Path(args.output).parent if args.output else Path("."),
        output_formats=tuple(args.formats.split(",")),
    )

    service = RankingService(paths=DEFAULT_PATHS, config=config)

    # Show market stats
    stats = service.get_market_stats()
    print(f"分析期間：{stats['start_date']} ~ {stats['end_date']}")
    print(f"交易日數：{stats['trading_days']}")
    print(f"市場報酬：{stats['market_return']*100:.1f}%")
    print()

    # Generate report with progress
    print("分析券商中...")

    def progress(current: int, total: int):
        if current % 100 == 0:
            print(f"  進度：{current}/{total}...")

    df = service.generate_report(progress_callback=progress)
    print(f"\n有效券商數：{len(df)}")

    # Save report
    base_name = Path(args.output).stem if args.output else "ranking_report"
    saved = service.save_report(df, base_name)

    for path in saved:
        print(f"已輸出：{path}")

    # Show top 5
    print("\n【PNL 排名 Top 5】")
    print(f"{'排名':<4} {'券商':<8} {'名稱':<12} {'PNL':>12} {'方向':<6}")
    print("-" * 50)

    for row in df.head(5).iter_rows(named=True):
        pnl_yi = row["total_pnl"] / 1e8
        print(f"{row['rank']:<4} {row['broker']:<8} {row['name']:<12} "
              f"{pnl_yi:>+11.2f}億 {row['direction']:<6}")

    return 0


def cmd_query(args: argparse.Namespace) -> int:
    """Query specific broker metrics."""
    service = RankingService(
        paths=DEFAULT_PATHS,
        config=RankingReportConfig(permutation_count=args.permutations),
    )

    result = service.analyze_single_broker(args.broker)

    if result is None:
        print(f"找不到券商：{args.broker}")
        return 1

    print(f"【{result.broker}】{result.name}")
    print("=" * 50)
    print()

    print("【基本資訊】")
    print(f"  交易日數：{result.trading_days}")
    print(f"  買入股數：{result.total_buy_shares:,}")
    print(f"  賣出股數：{result.total_sell_shares:,}")
    print(f"  累積淨部位：{result.cumulative_net:,}")
    print(f"  交易方向：{result.direction}")
    print()

    print("【損益】")
    print(f"  已實現 PNL：{result.realized_pnl/1e8:+.2f} 億")
    print(f"  未實現 PNL：{result.unrealized_pnl/1e8:+.2f} 億")
    print(f"  總 PNL：{result.total_pnl/1e8:+.2f} 億")
    print()

    print("【執行 Alpha】")
    if result.exec_alpha is not None:
        print(f"  Alpha：{result.exec_alpha*100:+.4f}%")
        print(f"  平倉筆數：{result.trade_count:,}")
        if result.exec_alpha > 0:
            print(f"  → 執行價格優於市場收盤價")
        else:
            print(f"  → 執行價格劣於市場收盤價")
    else:
        print(f"  無平倉記錄")
    print()

    print("【擇時能力】")
    if result.timing_alpha is not None:
        print(f"  擇時 Alpha：{result.timing_alpha:,.0f}")
        print(f"  顯著性：{result.timing_significance}")
        if result.p_value is not None:
            print(f"  p-value：{result.p_value:.4f}")
    else:
        print(f"  交易日不足，無法分析")
    print()

    print("【相關性分析】")
    if result.lead_corr is not None:
        print(f"  領先相關（預測）：{result.lead_corr:+.4f}")
        print(f"  落後相關（追蹤）：{result.lag_corr:+.4f}")
        print(f"  交易風格：{result.style}")
    else:
        print(f"  數據不足")

    return 0


def cmd_scorecard(args: argparse.Namespace) -> int:
    """Generate broker scorecard with detailed analysis."""
    service = RankingService(
        paths=DEFAULT_PATHS,
        config=RankingReportConfig(permutation_count=500),
    )

    result = service.analyze_single_broker(args.broker)

    if result is None:
        print(f"找不到券商：{args.broker}")
        return 1

    # Get market stats for context
    stats = service.get_market_stats()

    print(f"\n{'='*60}")
    print(f"【券商評分卡】{result.broker} {result.name}")
    print(f"{'='*60}")
    print(f"分析期間：{stats['start_date']} ~ {stats['end_date']}")
    print(f"市場報酬：{stats['market_return']*100:.1f}%")
    print()

    # Six dimensions evaluation
    scores = []

    # 1. PNL
    pnl_yi = result.total_pnl / 1e8
    if pnl_yi > 50:
        pnl_score = "A"
    elif pnl_yi > 10:
        pnl_score = "B"
    elif pnl_yi > 0:
        pnl_score = "C"
    elif pnl_yi > -10:
        pnl_score = "D"
    else:
        pnl_score = "F"
    scores.append(("總損益", pnl_score, f"{pnl_yi:+.2f}億"))

    # 2. Execution Alpha
    if result.exec_alpha is not None:
        alpha_pct = result.exec_alpha * 100
        if alpha_pct > 0.5:
            alpha_score = "A"
        elif alpha_pct > 0.1:
            alpha_score = "B"
        elif alpha_pct > -0.1:
            alpha_score = "C"
        elif alpha_pct > -0.5:
            alpha_score = "D"
        else:
            alpha_score = "F"
        scores.append(("執行 Alpha", alpha_score, f"{alpha_pct:+.4f}%"))
    else:
        scores.append(("執行 Alpha", "-", "無數據"))

    # 3. Timing Significance
    if result.timing_significance:
        if result.timing_significance == "顯著正向":
            timing_score = "A"
        elif result.timing_significance == "不顯著" and result.timing_alpha and result.timing_alpha > 0:
            timing_score = "B"
        elif result.timing_significance == "不顯著":
            timing_score = "C"
        else:  # 顯著負向
            timing_score = "F"
        scores.append(("擇時能力", timing_score, result.timing_significance))
    else:
        scores.append(("擇時能力", "-", "無數據"))

    # 4. Lead Correlation
    if result.lead_corr is not None:
        if result.lead_corr > 0.1:
            lead_score = "A"
        elif result.lead_corr > 0.05:
            lead_score = "B"
        elif result.lead_corr > -0.05:
            lead_score = "C"
        elif result.lead_corr > -0.1:
            lead_score = "D"
        else:
            lead_score = "F"
        scores.append(("預測能力", lead_score, f"{result.lead_corr:+.4f}"))
    else:
        scores.append(("預測能力", "-", "無數據"))

    # 5. Trading Volume
    volume_yi = result.total_amount / 1e8
    if volume_yi > 1000:
        vol_score = "A"
    elif volume_yi > 100:
        vol_score = "B"
    elif volume_yi > 10:
        vol_score = "C"
    else:
        vol_score = "D"
    scores.append(("交易規模", vol_score, f"{volume_yi:.1f}億"))

    # 6. Consistency (trading days)
    if result.trading_days > 500:
        cons_score = "A"
    elif result.trading_days > 200:
        cons_score = "B"
    elif result.trading_days > 50:
        cons_score = "C"
    else:
        cons_score = "D"
    scores.append(("活躍度", cons_score, f"{result.trading_days}天"))

    # Display scorecard
    print(f"{'維度':<12} {'評分':^6} {'數值':>16}")
    print("-" * 40)
    for dim, score, value in scores:
        print(f"{dim:<12} {score:^6} {value:>16}")

    # Overall assessment
    valid_scores = [s for _, s, _ in scores if s != "-"]
    score_map = {"A": 4, "B": 3, "C": 2, "D": 1, "F": 0}
    if valid_scores:
        avg = sum(score_map.get(s, 0) for s in valid_scores) / len(valid_scores)
        if avg >= 3.5:
            overall = "優秀"
        elif avg >= 2.5:
            overall = "良好"
        elif avg >= 1.5:
            overall = "普通"
        else:
            overall = "待改善"
        print("-" * 40)
        print(f"{'總評':<12} {overall:^6}")

    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    """Verify data integrity and calculations."""
    from pnl_analytics.infrastructure import (
        PnlRepository,
        IndexMapRepository,
    )

    print("【數據驗證】")
    print("=" * 50)

    errors = []

    # 1. Check data files exist
    print("\n1. 檢查資料檔案...")
    missing = DEFAULT_PATHS.validate()
    if missing:
        for m in missing:
            print(f"  ✗ 缺少：{m}")
            errors.append(f"Missing file: {m}")
    else:
        print("  ✓ 所有資料檔案存在")

    # 2. Check zero-sum
    print("\n2. 零和檢驗...")
    try:
        pnl_repo = PnlRepository(DEFAULT_PATHS)
        realized_total = pnl_repo.get_total_realized()
        unrealized_total = pnl_repo.get_total_final_unrealized()
        net = realized_total + unrealized_total

        print(f"  已實現：{realized_total/1e8:+.2f}億")
        print(f"  未實現：{unrealized_total/1e8:+.2f}億")
        print(f"  合計：{net/1e8:+.4f}億")

        threshold = abs(realized_total) * 0.005  # 0.5%
        if abs(net) < threshold:
            print("  ✓ 零和檢驗通過")
        else:
            print("  ✗ 零和檢驗失敗")
            errors.append("Zero-sum check failed")
    except Exception as e:
        print(f"  ✗ 錯誤：{e}")
        errors.append(str(e))

    # 3. Check broker count
    print("\n3. 檢查券商數量...")
    try:
        index_repo = IndexMapRepository(DEFAULT_PATHS)
        brokers = index_repo.get_brokers()
        print(f"  券商數：{len(brokers)}")
        if len(brokers) >= 900:
            print("  ✓ 券商數量正常")
        else:
            print("  ⚠ 券商數量偏低")
    except Exception as e:
        print(f"  ✗ 錯誤：{e}")
        errors.append(str(e))

    # 4. Check Merrill (sanity check)
    print("\n4. 基準券商驗證 (1440 美林)...")
    try:
        service = RankingService(
            paths=DEFAULT_PATHS,
            config=RankingReportConfig(permutation_count=50),
        )
        result = service.analyze_single_broker("1440")
        if result:
            print(f"  已實現 PNL：{result.realized_pnl/1e8:.2f}億")
            print(f"  執行 Alpha：{result.exec_alpha*100:.4f}%")

            if abs(result.realized_pnl/1e8 - 97.84) < 0.5:
                print("  ✓ 美林指標一致")
            else:
                print("  ✗ 美林指標不一致")
                errors.append("Merrill baseline mismatch")
        else:
            print("  ✗ 找不到美林資料")
            errors.append("Merrill not found")
    except Exception as e:
        print(f"  ✗ 錯誤：{e}")
        errors.append(str(e))

    # Summary
    print("\n" + "=" * 50)
    if errors:
        print(f"❌ 發現 {len(errors)} 個問題")
        return 1
    else:
        print("✅ 所有驗證通過")
        return 0


def main(argv: list[str] | None = None) -> int:
    """Main entry point for CLI."""
    parser = argparse.ArgumentParser(
        prog="pnl_analytics",
        description="PNL Analytics - Broker Performance Analysis System",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # ranking command
    ranking_parser = subparsers.add_parser(
        "ranking",
        help="Generate broker ranking report",
    )
    ranking_parser.add_argument(
        "-o", "--output",
        default="ranking_report",
        help="Output filename (without extension)",
    )
    ranking_parser.add_argument(
        "-f", "--formats",
        default="csv,parquet",
        help="Output formats (comma-separated: csv,parquet,xlsx)",
    )
    ranking_parser.add_argument(
        "--min-days",
        type=int,
        default=20,
        help="Minimum trading days for timing analysis",
    )
    ranking_parser.add_argument(
        "--permutations",
        type=int,
        default=200,
        help="Number of permutations for p-value",
    )

    # query command
    query_parser = subparsers.add_parser(
        "query",
        help="Query specific broker metrics",
    )
    query_parser.add_argument(
        "broker",
        help="Broker code (e.g., 1440)",
    )
    query_parser.add_argument(
        "--permutations",
        type=int,
        default=200,
        help="Number of permutations for p-value",
    )

    # scorecard command
    scorecard_parser = subparsers.add_parser(
        "scorecard",
        help="Generate broker scorecard",
    )
    scorecard_parser.add_argument(
        "broker",
        help="Broker code (e.g., 1440)",
    )

    # verify command
    verify_parser = subparsers.add_parser(
        "verify",
        help="Verify data integrity",
    )

    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 0

    commands = {
        "ranking": cmd_ranking,
        "query": cmd_query,
        "scorecard": cmd_scorecard,
        "verify": cmd_verify,
    }

    return commands[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
