"""Alpha Analysis v2: Lot-level Alpha calculation using closed_trades.parquet.

Correct Alpha calculation:
- Compare trade return to MARKET RETURN during the SAME holding period
- NOT to holding until period end

For each closed trade:
  trade_return = (sell_price - buy_price) / buy_price
  benchmark_return = (close[sell_date] - close[buy_date]) / close[buy_date]
  alpha = trade_return - benchmark_return

For shorts:
  trade_return = (buy_price - sell_price) / buy_price  (profit if covered lower)
  benchmark_return = (close[buy_date] - close[sell_date]) / close[buy_date]  (what short-to-close would make)
  alpha = trade_return - benchmark_return
"""

import json
from pathlib import Path

import polars as pl


def main():
    print("載入資料...")

    # Load closed trades
    trades_df = pl.read_parquet("closed_trades.parquet")
    print(f"平倉交易筆數：{len(trades_df):,}")

    # Load price data - need close prices by date
    price_df = pl.read_parquet("price_master.parquet")
    price_by_date = {row["date"]: row["close_price"] for row in price_df.iter_rows(named=True)}

    # Load broker names
    with open("broker_names.json") as f:
        broker_names = json.load(f)

    # Add close prices for buy_date and sell_date
    trades_df = trades_df.with_columns([
        pl.col("buy_date").map_elements(
            lambda d: price_by_date.get(d, 0.0), return_dtype=pl.Float64
        ).alias("close_at_buy"),
        pl.col("sell_date").map_elements(
            lambda d: price_by_date.get(d, 0.0), return_dtype=pl.Float64
        ).alias("close_at_sell"),
    ])

    # Filter out trades with missing price data
    valid_trades = trades_df.filter(
        (pl.col("close_at_buy") > 0) & (pl.col("close_at_sell") > 0)
    )
    print(f"有效交易筆數（有收盤價資料）：{len(valid_trades):,}")

    # Calculate Alpha for each trade
    valid_trades = valid_trades.with_columns([
        # Trade return (actual execution prices)
        pl.when(pl.col("trade_type") == "long")
        .then((pl.col("sell_price") - pl.col("buy_price")) / pl.col("buy_price"))
        .otherwise((pl.col("buy_price") - pl.col("sell_price")) / pl.col("buy_price"))
        .alias("trade_return"),

        # Benchmark return (close-to-close during same period)
        # For long: what would holding the stock from buy_date to sell_date return?
        # For short: what would shorting from buy_date to sell_date return?
        pl.when(pl.col("trade_type") == "long")
        .then((pl.col("close_at_sell") - pl.col("close_at_buy")) / pl.col("close_at_buy"))
        .otherwise((pl.col("close_at_buy") - pl.col("close_at_sell")) / pl.col("close_at_buy"))
        .alias("benchmark_return"),

        # Trade value (for weighting)
        (pl.col("shares") * pl.col("buy_price")).alias("trade_value"),
    ]).with_columns([
        # Alpha = trade_return - benchmark_return
        (pl.col("trade_return") - pl.col("benchmark_return")).alias("alpha"),

        # Alpha in dollar terms
        ((pl.col("trade_return") - pl.col("benchmark_return")) * pl.col("trade_value")).alias("alpha_dollars"),
    ])

    # Aggregate by broker
    broker_alpha = (
        valid_trades
        .group_by("broker")
        .agg([
            pl.col("trade_value").sum().alias("total_trade_value"),
            pl.col("alpha_dollars").sum().alias("total_alpha_dollars"),
            pl.col("realized_pnl").sum().alias("total_realized_pnl"),
            pl.len().alias("trade_count"),
            (pl.col("trade_type") == "long").sum().alias("long_trades"),
            (pl.col("trade_type") == "short").sum().alias("short_trades"),
        ])
        .with_columns([
            # Weighted average alpha
            (pl.col("total_alpha_dollars") / pl.col("total_trade_value")).alias("weighted_alpha"),
            # Broker name
            pl.col("broker").map_elements(
                lambda x: broker_names.get(x, ""), return_dtype=pl.String
            ).alias("name"),
        ])
        .sort("weighted_alpha", descending=True)
    )

    # Filter for meaningful activity (at least 1000萬 trade value)
    broker_alpha_filtered = broker_alpha.filter(pl.col("total_trade_value") >= 10_000_000)

    print(f"\n總券商數：{len(broker_alpha)}")
    print(f"交易額 >= 1000萬 的券商：{len(broker_alpha_filtered)}")

    # === Top Alpha ===
    print(f"\n{'='*90}")
    print("【Alpha 排行 Top 20】（正 Alpha = 執行價格優於收盤價）")
    print(f"{'='*90}")
    print(f"\n{'排名':<4} {'券商':<10} {'名稱':<18} {'Alpha':>10} {'Alpha金額':>14} {'交易額':>12} {'平倉筆數':>10}")
    print("-" * 90)

    for i, row in enumerate(broker_alpha_filtered.head(20).iter_rows(named=True), 1):
        print(f"{i:<4} {row['broker']:<10} {row['name']:<18} "
              f"{row['weighted_alpha']*100:>+9.2f}% "
              f"{row['total_alpha_dollars']/1e8:>+13.2f}億 "
              f"{row['total_trade_value']/1e8:>11.2f}億 "
              f"{row['trade_count']:>10,}")

    # === Bottom Alpha ===
    print(f"\n{'='*90}")
    print("【Alpha 排行 Bottom 20】（負 Alpha = 執行價格劣於收盤價）")
    print(f"{'='*90}")
    print(f"\n{'排名':<4} {'券商':<10} {'名稱':<18} {'Alpha':>10} {'Alpha金額':>14} {'交易額':>12} {'平倉筆數':>10}")
    print("-" * 90)

    for i, row in enumerate(broker_alpha_filtered.tail(20).reverse().iter_rows(named=True), 1):
        print(f"{i:<4} {row['broker']:<10} {row['name']:<18} "
              f"{row['weighted_alpha']*100:>+9.2f}% "
              f"{row['total_alpha_dollars']/1e8:>+13.2f}億 "
              f"{row['total_trade_value']/1e8:>11.2f}億 "
              f"{row['trade_count']:>10,}")

    # === Summary Statistics ===
    print(f"\n{'='*90}")
    print("【統計摘要】")
    print(f"{'='*90}")

    alphas = broker_alpha_filtered["weighted_alpha"].to_list()
    positive_alpha = sum(1 for a in alphas if a > 0)

    print(f"\n券商數（交易額 >= 1000萬）：{len(alphas)}")
    print(f"Alpha > 0（執行價優於收盤）：{positive_alpha} ({positive_alpha/len(alphas)*100:.1f}%)")
    print(f"Alpha < 0（執行價劣於收盤）：{len(alphas) - positive_alpha} ({(len(alphas)-positive_alpha)/len(alphas)*100:.1f}%)")
    print(f"平均 Alpha：{sum(alphas)/len(alphas)*100:.4f}%")
    print(f"中位數 Alpha：{sorted(alphas)[len(alphas)//2]*100:.4f}%")

    total_alpha_dollars = broker_alpha_filtered["total_alpha_dollars"].sum()
    print(f"\n全市場 Alpha 金額總和：{total_alpha_dollars/1e8:+.2f} 億")

    # === Focus Brokers ===
    print(f"\n{'='*90}")
    print("【特別關注券商】")
    print(f"{'='*90}")

    focus_brokers = ["961H", "6380", "6450", "126L", "9100", "8890", "1440", "1030"]

    for broker in focus_brokers:
        row = broker_alpha.filter(pl.col("broker") == broker)
        if len(row) == 0:
            continue
        row = row.to_dicts()[0]

        print(f"\n【{row['broker']}】{row['name']}")
        print(f"  交易額：{row['total_trade_value']/1e8:.2f} 億")
        print(f"  平倉筆數：{row['trade_count']:,}（做多 {row['long_trades']:,} / 做空 {row['short_trades']:,}）")
        print(f"  已實現 PNL：{row['total_realized_pnl']/1e8:+.2f} 億")
        print(f"  Alpha：{row['weighted_alpha']*100:+.4f}%")
        print(f"  Alpha 金額：{row['total_alpha_dollars']/1e8:+.4f} 億")

        if row['weighted_alpha'] > 0:
            print(f"  → 執行價格優於市場收盤價")
        else:
            print(f"  → 執行價格劣於市場收盤價")

    # === Trade-level analysis for top broker ===
    print(f"\n{'='*90}")
    print("【Alpha 最高券商的交易分析】")
    print(f"{'='*90}")

    top_broker = broker_alpha_filtered.head(1).to_dicts()[0]["broker"]
    top_trades = valid_trades.filter(pl.col("broker") == top_broker).sort("sell_date")

    print(f"\n券商：{top_broker} {broker_names.get(top_broker, '')}")
    print(f"交易筆數：{len(top_trades)}")

    # Sample trades
    print(f"\n最近 10 筆平倉：")
    print(f"{'類型':<6} {'賣出日':<12} {'買入日':<12} {'執行買價':>8} {'執行賣價':>8} {'收盤買':>8} {'收盤賣':>8} {'Alpha':>8}")
    print("-" * 85)

    for row in top_trades.tail(10).iter_rows(named=True):
        print(f"{row['trade_type']:<6} {row['sell_date']:<12} {row['buy_date']:<12} "
              f"{row['buy_price']:>8.1f} {row['sell_price']:>8.1f} "
              f"{row['close_at_buy']:>8.1f} {row['close_at_sell']:>8.1f} "
              f"{row['alpha']*100:>+7.2f}%")


if __name__ == "__main__":
    main()
