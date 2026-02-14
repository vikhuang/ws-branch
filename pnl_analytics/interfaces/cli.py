"""Command Line Interface for PNL Analytics.

Provides CLI access to analytics functions:
- ranking: Show broker ranking
- query: Query specific broker
- verify: Verify data integrity

Usage:
    python -m pnl_analytics ranking [--output FILE]
    python -m pnl_analytics query BROKER
    python -m pnl_analytics verify
"""

import argparse
import sys
from pathlib import Path

from pnl_analytics import __version__
from pnl_analytics.infrastructure import DEFAULT_PATHS
from pnl_analytics.application import (
    RankingService,
    RankingReportConfig,
    BrokerAnalyzer,
)


def cmd_ranking(args: argparse.Namespace) -> int:
    """Show broker ranking."""
    print(f"PNL Analytics v{__version__}")
    print("=" * 60)

    config = RankingReportConfig(
        output_dir=Path(args.output).parent if args.output else Path("."),
        output_formats=tuple(args.formats.split(",")),
    )

    service = RankingService(paths=DEFAULT_PATHS, config=config)

    # Get summary
    summary = service.get_summary()
    print(f"券商數：{summary['broker_count']}")
    print(f"總交易筆數：{summary['total_trades']:,}")
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
    print(f"{'排名':<4} {'券商':<8} {'名稱':<12} {'PNL':>12} {'勝率':>8}")
    print("-" * 50)

    for row in df.head(10).iter_rows(named=True):
        pnl_yi = row["total_pnl"] / 1e8
        name = row.get("name", "")[:10]
        win_rate = row["win_rate"] * 100
        print(f"{row['rank']:<4} {row['broker']:<8} {name:<12} "
              f"{pnl_yi:>+10.2f}億 {win_rate:>7.1f}%")

    print()
    print("【PNL 排名 Bottom 5】")
    print("-" * 50)

    for row in df.tail(5).iter_rows(named=True):
        pnl_yi = row["total_pnl"] / 1e8
        name = row.get("name", "")[:10]
        win_rate = row["win_rate"] * 100
        print(f"{row['rank']:<4} {row['broker']:<8} {name:<12} "
              f"{pnl_yi:>+10.2f}億 {win_rate:>7.1f}%")

    return 0


def cmd_query(args: argparse.Namespace) -> int:
    """Query specific broker."""
    analyzer = BrokerAnalyzer(paths=DEFAULT_PATHS)
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

    print("【勝率】")
    print(f"  獲利筆數：{result.win_count:,}")
    print(f"  虧損筆數：{result.loss_count:,}")
    print(f"  總交易筆數：{result.trade_count:,}")
    print(f"  勝率：{result.win_rate*100:.1f}%")

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


def cmd_verify(args: argparse.Namespace) -> int:
    """Verify data integrity."""
    from pnl_analytics.infrastructure.repositories import RankingRepository

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
        print("  ✓ 資料目錄存在")

    # 2. Check broker_ranking.parquet
    print("\n2. 檢查排名資料...")
    try:
        ranking_repo = RankingRepository(DEFAULT_PATHS)
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
    symbols = DEFAULT_PATHS.list_symbols()
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
    if DEFAULT_PATHS.close_prices.exists():
        import polars as pl
        price_df = pl.read_parquet(DEFAULT_PATHS.close_prices)
        print(f"  價格記錄數：{len(price_df):,}")
        print(f"  股票數：{price_df['symbol_id'].n_unique()}")
        print("  ✓ 價格資料存在")
    else:
        print("  ✗ 缺少價格資料")
        errors.append("Missing price data")

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
        description="PNL Analytics - Broker Performance Analysis",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
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

    # verify command
    subparsers.add_parser("verify", help="Verify data integrity")

    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 0

    commands = {
        "ranking": cmd_ranking,
        "query": cmd_query,
        "verify": cmd_verify,
    }

    return commands[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
