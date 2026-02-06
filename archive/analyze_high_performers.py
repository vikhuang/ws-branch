"""Deep dive analysis of high-performing brokers.

Analyze 961H (富邦總行) and other high win-rate brokers to verify
they're not just lucky.
"""

import json
from pathlib import Path
from collections import defaultdict

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


def analyze_broker_detail(
    broker_code: str,
    broker_name: str,
    trade_df: pl.DataFrame,
    price_df: pl.DataFrame,
    realized: np.ndarray,
    unrealized: np.ndarray,
    maps: dict,
):
    """Detailed analysis of a single broker."""

    print(f"\n{'='*80}")
    print(f"【{broker_code}】{broker_name}")
    print("="*80)

    broker_trades = trade_df.filter(pl.col("broker") == broker_code).sort("date")

    if len(broker_trades) == 0:
        print("無交易資料")
        return

    # === Basic Stats ===
    trading_days = len(broker_trades)
    dates = broker_trades["date"].to_list()
    first_trade = dates[0]
    last_trade = dates[-1]

    total_buy = broker_trades["buy_shares"].sum()
    total_sell = broker_trades["sell_shares"].sum()
    total_buy_amount = broker_trades["buy_amount"].sum()
    total_sell_amount = broker_trades["sell_amount"].sum()
    total_volume = total_buy + total_sell

    print(f"\n【基本統計】")
    print(f"交易期間：{first_trade} ~ {last_trade}")
    print(f"交易天數：{trading_days} 天")
    print(f"總買入：{total_buy:,} 張 ({total_buy_amount/1e8:.2f} 億)")
    print(f"總賣出：{total_sell:,} 張 ({total_sell_amount/1e8:.2f} 億)")
    print(f"總交易量：{total_volume:,} 張")
    print(f"累計淨買：{total_buy - total_sell:+,} 張")

    # Average per day
    avg_daily_volume = total_volume / trading_days
    print(f"日均交易量：{avg_daily_volume:,.0f} 張")

    # === PNL Stats ===
    broker_idx = maps["brokers"].get(broker_code)
    if broker_idx is None:
        print("無法找到 broker index")
        return

    sym_idx = 0
    total_realized = float(realized[sym_idx, :, broker_idx].sum())
    final_unrealized = float(unrealized[sym_idx, -1, broker_idx])
    total_pnl = total_realized + final_unrealized

    print(f"\n【損益統計】")
    print(f"已實現損益：{total_realized:+,.0f} ({total_realized/1e8:+.2f} 億)")
    print(f"未實現損益：{final_unrealized:+,.0f} ({final_unrealized/1e8:+.2f} 億)")
    print(f"總損益：{total_pnl:+,.0f} ({total_pnl/1e8:+.2f} 億)")
    print(f"每張報酬：{total_pnl/total_volume:+,.1f}")

    # === Monthly Breakdown ===
    print(f"\n【月度明細】")

    date_to_idx = maps["dates"]
    monthly_realized = defaultdict(float)
    monthly_volume = defaultdict(int)

    for row in broker_trades.iter_rows(named=True):
        month = row["date"][:7]
        monthly_volume[month] += (row["buy_shares"] or 0) + (row["sell_shares"] or 0)

    for date, idx in date_to_idx.items():
        month = date[:7]
        monthly_realized[month] += realized[sym_idx, idx, broker_idx]

    months = sorted(monthly_realized.keys())
    positive_months = 0
    negative_months = 0
    zero_months = 0

    print(f"{'月份':<10} {'已實現PNL':>15} {'交易量':>12} {'結果':<8}")
    print("-" * 50)

    for month in months:
        pnl = monthly_realized[month]
        vol = monthly_volume.get(month, 0)

        if pnl > 0:
            result = "✓ 獲利"
            positive_months += 1
        elif pnl < 0:
            result = "✗ 虧損"
            negative_months += 1
        else:
            result = "- 持平"
            zero_months += 1

        if vol > 0:  # Only show months with trading
            print(f"{month:<10} {pnl:>+15,.0f} {vol:>12,} {result}")

    print("-" * 50)
    traded_months = positive_months + negative_months
    win_rate = positive_months / traded_months * 100 if traded_months > 0 else 0
    print(f"獲利月數：{positive_months} / {traded_months} = {win_rate:.1f}%")

    # === Trading Pattern Analysis ===
    print(f"\n【交易模式分析】")

    # Buy vs Sell ratio by month
    monthly_net = defaultdict(int)
    for row in broker_trades.iter_rows(named=True):
        month = row["date"][:7]
        monthly_net[month] += (row["buy_shares"] or 0) - (row["sell_shares"] or 0)

    buy_months = sum(1 for m, net in monthly_net.items() if net > 0)
    sell_months = sum(1 for m, net in monthly_net.items() if net < 0)
    print(f"淨買月數：{buy_months}")
    print(f"淨賣月數：{sell_months}")

    # Check for consistent direction
    if buy_months > sell_months * 2:
        print("→ 趨勢：持續買入（偏多）")
    elif sell_months > buy_months * 2:
        print("→ 趨勢：持續賣出（偏空）")
    else:
        print("→ 趨勢：雙向操作")

    # Daily trading frequency
    total_days_in_period = (pl.Series([last_trade]).str.to_date().item() -
                            pl.Series([first_trade]).str.to_date().item()).days
    trading_frequency = trading_days / total_days_in_period * 100 if total_days_in_period > 0 else 0
    print(f"交易頻率：{trading_frequency:.1f}% (期間 {total_days_in_period} 天中有 {trading_days} 天交易)")

    # === Price Timing Analysis ===
    print(f"\n【買賣時機分析】")

    # Get price data
    prices = price_df.sort("date")
    price_dict = {row["date"]: row["close_price"] for row in prices.iter_rows(named=True)}

    buy_prices = []
    sell_prices = []

    for row in broker_trades.iter_rows(named=True):
        date = row["date"]
        if date in price_dict:
            price = price_dict[date]
            if row["buy_shares"] and row["buy_shares"] > 0:
                buy_prices.extend([price] * row["buy_shares"])
            if row["sell_shares"] and row["sell_shares"] > 0:
                sell_prices.extend([price] * row["sell_shares"])

    if buy_prices and sell_prices:
        avg_buy_price = sum(buy_prices) / len(buy_prices)
        avg_sell_price = sum(sell_prices) / len(sell_prices)
        price_diff = avg_sell_price - avg_buy_price

        print(f"加權平均買入價：{avg_buy_price:,.1f}")
        print(f"加權平均賣出價：{avg_sell_price:,.1f}")
        print(f"價差：{price_diff:+,.1f} ({price_diff/avg_buy_price*100:+.2f}%)")

        if price_diff > 0:
            print("→ 結論：賣出價高於買入價（擇時正確）")
        else:
            print("→ 結論：賣出價低於買入價（擇時錯誤或尚未平倉）")

    # === Streak Analysis ===
    print(f"\n【連續獲利/虧損分析】")

    # Calculate daily PNL
    daily_pnl = []
    for date, idx in sorted(date_to_idx.items()):
        pnl = realized[sym_idx, idx, broker_idx]
        if pnl != 0:
            daily_pnl.append((date, pnl))

    if daily_pnl:
        # Find longest winning streak
        max_win_streak = 0
        max_lose_streak = 0
        current_win = 0
        current_lose = 0

        for date, pnl in daily_pnl:
            if pnl > 0:
                current_win += 1
                current_lose = 0
                max_win_streak = max(max_win_streak, current_win)
            else:
                current_lose += 1
                current_win = 0
                max_lose_streak = max(max_lose_streak, current_lose)

        print(f"最長連續獲利：{max_win_streak} 天")
        print(f"最長連續虧損：{max_lose_streak} 天")

        # Win rate by day
        win_days = sum(1 for _, pnl in daily_pnl if pnl > 0)
        total_pnl_days = len(daily_pnl)
        print(f"日勝率：{win_days}/{total_pnl_days} = {win_days/total_pnl_days*100:.1f}%")


def main():
    print("載入資料...")
    trade_df, price_df, realized, unrealized, maps, broker_names = load_data()

    # Target brokers to analyze
    targets = [
        ("961H", "富邦總行"),
        ("6380", broker_names.get("6380", "光和")),
        ("6450", broker_names.get("6450", "永全")),
        ("126L", broker_names.get("126L", "宏遠-台中")),
        ("918u", broker_names.get("918u", "群益金鼎-瑞豐")),
        ("779u", broker_names.get("779u", "國票-長城")),
        ("9100", broker_names.get("9100", "群益金鼎")),
    ]

    for code, name in targets:
        analyze_broker_detail(
            code, name, trade_df, price_df, realized, unrealized, maps
        )

    # === Comparison Summary ===
    print("\n" + "="*80)
    print("【綜合比較】")
    print("="*80)
    print(f"\n{'代碼':<8} {'名稱':<20} {'交易天數':>10} {'日均量':>10} {'月勝率':>10} {'總PNL':>15}")
    print("-" * 85)

    for code, name in targets:
        broker_trades = trade_df.filter(pl.col("broker") == code)
        trading_days = len(broker_trades)

        if trading_days == 0:
            continue

        total_volume = broker_trades.select(
            (pl.col("buy_shares") + pl.col("sell_shares")).sum()
        ).item()
        avg_daily = total_volume / trading_days

        broker_idx = maps["brokers"].get(code)
        if broker_idx is None:
            continue

        sym_idx = 0
        total_realized = realized[sym_idx, :, broker_idx].sum()
        final_unrealized = unrealized[sym_idx, -1, broker_idx]
        total_pnl = total_realized + final_unrealized

        # Monthly win rate
        date_to_idx = maps["dates"]
        monthly_pnl = defaultdict(float)
        for date, idx in date_to_idx.items():
            month = date[:7]
            monthly_pnl[month] += realized[sym_idx, idx, broker_idx]

        months_with_trade = [m for m, pnl in monthly_pnl.items() if pnl != 0]
        positive = sum(1 for m in months_with_trade if monthly_pnl[m] > 0)
        win_rate = positive / len(months_with_trade) * 100 if months_with_trade else 0

        print(f"{code:<8} {name:<20} {trading_days:>10,} {avg_daily:>10,.0f} {win_rate:>9.1f}% {total_pnl:>15,.0f}")


if __name__ == "__main__":
    main()
