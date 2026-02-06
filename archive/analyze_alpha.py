"""Alpha Analysis: Compare broker returns to Buy & Hold benchmark.

Calculates excess return (alpha) for each broker using FIFO-based PNL.
"""

import json
from pathlib import Path

import numpy as np
import polars as pl


def load_data():
    """Load all required data."""
    trade_df = pl.read_parquet("daily_trade_summary.parquet")
    price_df = pl.read_parquet("price_master.parquet")
    realized = np.load("realized_pnl.npy")
    unrealized = np.load("unrealized_pnl.npy")

    with open("index_maps.json") as f:
        maps = json.load(f)

    with open("broker_names.json") as f:
        broker_names = json.load(f)

    return trade_df, price_df, realized, unrealized, maps, broker_names


def main():
    print("載入資料...")
    trade_df, price_df, realized, unrealized, maps, broker_names = load_data()

    # Get price range
    prices = price_df.sort("date")
    price_list = prices.select(["date", "close_price"]).to_dicts()

    start_date = price_list[0]["date"]
    end_date = price_list[-1]["date"]
    start_price = price_list[0]["close_price"]
    end_price = price_list[-1]["close_price"]

    # Buy & Hold return
    buy_hold_return = (end_price - start_price) / start_price

    print(f"\n{'='*80}")
    print("【基準：Buy & Hold】")
    print(f"{'='*80}")
    print(f"期間：{start_date} ~ {end_date}")
    print(f"起始股價：{start_price:.1f}")
    print(f"結束股價：{end_price:.1f}")
    print(f"Buy & Hold 報酬率：{buy_hold_return*100:.1f}%")

    # Calculate alpha for each broker
    results = []
    sym_idx = 0  # Single stock

    for broker in maps["brokers"]:
        broker_idx = maps["brokers"][broker]
        broker_trades = trade_df.filter(pl.col("broker") == broker)

        if len(broker_trades) == 0:
            continue

        # Trading stats
        total_buy_amount = broker_trades["buy_amount"].sum() or 0
        total_sell_amount = broker_trades["sell_amount"].sum() or 0
        total_buy_shares = broker_trades["buy_shares"].sum() or 0
        total_sell_shares = broker_trades["sell_shares"].sum() or 0

        # Skip if no meaningful activity
        if total_buy_amount == 0 and total_sell_amount == 0:
            continue

        # PNL from FIFO
        total_realized = float(realized[sym_idx, :, broker_idx].sum())
        final_unrealized = float(unrealized[sym_idx, -1, broker_idx])
        total_pnl = total_realized + final_unrealized

        # Net position
        net_shares = total_buy_shares - total_sell_shares

        # Determine if primarily long or short
        if total_buy_amount >= total_sell_amount:
            # Long-biased: use buy amount as capital base
            capital_base = total_buy_amount
            position_type = "做多"
        else:
            # Short-biased: use sell amount as capital base
            capital_base = total_sell_amount
            position_type = "做空"

        if capital_base == 0:
            continue

        # Broker return
        broker_return = total_pnl / capital_base

        # Alpha = broker return - buy & hold return
        # For shorts, if they profited from decline, alpha should be positive
        # But we need to adjust: shorting during a bull market and losing is "expected"
        # So for shorts, compare to negative of buy & hold?
        # Actually, simpler: just compare raw returns

        # For a fair comparison:
        # - Long position: should compare to +buy_hold_return
        # - Short position: should compare to -buy_hold_return (since shorting in bull = loss expected)

        if position_type == "做多":
            expected_return = buy_hold_return
        else:
            # If shorting, the "benchmark" is losing money at the rate of buy_hold
            # So if buy_hold = +300%, a short would "expect" -300%
            # Alpha = actual - expected = actual - (-300%) = actual + 300%
            expected_return = -buy_hold_return

        alpha = broker_return - expected_return

        results.append({
            "broker": broker,
            "name": broker_names.get(broker, ""),
            "position_type": position_type,
            "total_buy_amount": total_buy_amount,
            "total_sell_amount": total_sell_amount,
            "net_shares": net_shares,
            "capital_base": capital_base,
            "total_pnl": total_pnl,
            "broker_return": broker_return,
            "expected_return": expected_return,
            "alpha": alpha,
        })

    # Convert to DataFrame
    df = pl.DataFrame(results)

    # Filter for meaningful activity (at least 1000萬 capital)
    df_filtered = df.filter(pl.col("capital_base") >= 10_000_000)

    print(f"\n總券商數：{len(df)}")
    print(f"資本額 >= 1000萬 的券商：{len(df_filtered)}")

    # === Top Alpha (做多) ===
    print(f"\n{'='*80}")
    print("【做多 Alpha 排行】（資本額 >= 1000萬）")
    print(f"{'='*80}")
    print(f"基準報酬（Buy & Hold）：{buy_hold_return*100:.1f}%")
    print()

    long_df = df_filtered.filter(pl.col("position_type") == "做多").sort("alpha", descending=True)

    print(f"{'排名':<4} {'券商':<12} {'名稱':<16} {'報酬率':>10} {'Alpha':>10} {'資本額':>12} {'總PNL':>14}")
    print("-" * 95)

    for i, row in enumerate(long_df.head(20).iter_rows(named=True), 1):
        print(f"{i:<4} {row['broker']:<12} {row['name']:<16} "
              f"{row['broker_return']*100:>+9.1f}% {row['alpha']*100:>+9.1f}% "
              f"{row['capital_base']/1e8:>10.2f}億 {row['total_pnl']/1e8:>12.2f}億")

    # === Bottom Alpha (做多) ===
    print(f"\n{'='*80}")
    print("【做多 Alpha 最差】")
    print(f"{'='*80}")

    print(f"{'排名':<4} {'券商':<12} {'名稱':<16} {'報酬率':>10} {'Alpha':>10} {'資本額':>12} {'總PNL':>14}")
    print("-" * 95)

    for i, row in enumerate(long_df.tail(20).reverse().iter_rows(named=True), 1):
        print(f"{i:<4} {row['broker']:<12} {row['name']:<16} "
              f"{row['broker_return']*100:>+9.1f}% {row['alpha']*100:>+9.1f}% "
              f"{row['capital_base']/1e8:>10.2f}億 {row['total_pnl']/1e8:>12.2f}億")

    # === Top Alpha (做空) ===
    short_df = df_filtered.filter(pl.col("position_type") == "做空").sort("alpha", descending=True)

    if len(short_df) > 0:
        print(f"\n{'='*80}")
        print("【做空 Alpha 排行】")
        print(f"{'='*80}")
        print(f"做空基準報酬（= -Buy & Hold）：{-buy_hold_return*100:.1f}%")
        print("（在牛市做空的「預期」是虧錢，如果虧得比預期少或反而賺錢，Alpha 為正）")
        print()

        print(f"{'排名':<4} {'券商':<12} {'名稱':<16} {'報酬率':>10} {'Alpha':>10} {'資本額':>12} {'總PNL':>14}")
        print("-" * 95)

        for i, row in enumerate(short_df.head(20).iter_rows(named=True), 1):
            print(f"{i:<4} {row['broker']:<12} {row['name']:<16} "
                  f"{row['broker_return']*100:>+9.1f}% {row['alpha']*100:>+9.1f}% "
                  f"{row['capital_base']/1e8:>10.2f}億 {row['total_pnl']/1e8:>12.2f}億")

    # === Summary Statistics ===
    print(f"\n{'='*80}")
    print("【統計摘要】")
    print(f"{'='*80}")

    long_alphas = long_df["alpha"].to_list()
    if long_alphas:
        positive_alpha = sum(1 for a in long_alphas if a > 0)
        print(f"\n做多券商（資本額 >= 1000萬）：{len(long_alphas)} 個")
        print(f"  Alpha > 0（打敗大盤）：{positive_alpha} 個 ({positive_alpha/len(long_alphas)*100:.1f}%)")
        print(f"  Alpha < 0（輸給大盤）：{len(long_alphas) - positive_alpha} 個")
        print(f"  平均 Alpha：{sum(long_alphas)/len(long_alphas)*100:.1f}%")
        print(f"  中位數 Alpha：{sorted(long_alphas)[len(long_alphas)//2]*100:.1f}%")

    # === 特別關注的券商 ===
    print(f"\n{'='*80}")
    print("【特別關注券商詳細】")
    print(f"{'='*80}")

    focus_brokers = ["961H", "6380", "6450", "126L", "9100", "8890"]

    for broker in focus_brokers:
        row = df.filter(pl.col("broker") == broker)
        if len(row) == 0:
            continue
        row = row.to_dicts()[0]

        print(f"\n【{row['broker']}】{row['name']}")
        print(f"  類型：{row['position_type']}")
        print(f"  資本額：{row['capital_base']/1e8:.2f} 億")
        print(f"  總 PNL：{row['total_pnl']/1e8:+.2f} 億")
        print(f"  報酬率：{row['broker_return']*100:+.1f}%")
        print(f"  基準報酬：{row['expected_return']*100:+.1f}%")
        print(f"  Alpha：{row['alpha']*100:+.1f}%")

        if row['alpha'] > 0:
            print(f"  → 打敗 Buy & Hold {row['alpha']*100:.1f} 個百分點")
        else:
            print(f"  → 落後 Buy & Hold {-row['alpha']*100:.1f} 個百分點")


if __name__ == "__main__":
    main()
