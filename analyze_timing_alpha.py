"""Timing Alpha Analysis: Measuring market timing ability.

Key insight from previous error:
- v1 error: Comparing to "end price" (meaningless for day trades)
- v2 fix: Compare to "same period market return" (close-to-close)
- New error: Using "avg_position × total_return" mixes different time periods
- Fix: Use demeaned daily decisions × daily returns

Correct Timing Alpha:
    demean_net_buy = net_buy[t] - avg(net_buy)
    timing_alpha = Σ(demean_net_buy[t-1] × return[t])

Interpretation:
- If broker buys same amount every day: timing_alpha = 0 (no timing skill)
- If broker buys more before up days: timing_alpha > 0 (good timing)
- If broker buys more before down days: timing_alpha < 0 (bad timing)

Relationship to Lead correlation:
- Lead = corr(net_buy, return) = directional consistency (normalized)
- Timing Alpha ≈ Lead × std(net_buy) × std(return) × n = cumulative contribution
"""

import json
from collections import defaultdict
import math

import polars as pl


def load_data():
    """Load all required data."""
    trade_df = pl.read_parquet("daily_trade_summary.parquet")
    price_df = pl.read_parquet("price_master.parquet")

    with open("index_maps.json") as f:
        maps = json.load(f)

    with open("broker_names.json") as f:
        broker_names = json.load(f)

    return trade_df, price_df, maps, broker_names


def calculate_returns(price_df: pl.DataFrame) -> dict[str, float]:
    """Calculate daily returns."""
    prices = price_df.sort("date")
    dates = prices["date"].to_list()
    closes = prices["close_price"].to_list()

    returns = {}
    for i in range(1, len(dates)):
        if closes[i-1] > 0:
            returns[dates[i]] = (closes[i] - closes[i-1]) / closes[i-1]

    return returns


def pearson_correlation(x: list, y: list) -> float:
    """Calculate Pearson correlation coefficient."""
    n = len(x)
    if n < 10:
        return 0.0

    mean_x = sum(x) / n
    mean_y = sum(y) / n

    numerator = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
    denom_x = math.sqrt(sum((xi - mean_x)**2 for xi in x))
    denom_y = math.sqrt(sum((yi - mean_y)**2 for yi in y))

    if denom_x == 0 or denom_y == 0:
        return 0.0

    return numerator / (denom_x * denom_y)


def analyze_broker_timing(
    broker: str,
    trade_df: pl.DataFrame,
    returns: dict[str, float],
    all_dates: list[str],
) -> dict | None:
    """Analyze timing alpha for a single broker."""

    broker_trades = trade_df.filter(pl.col("broker") == broker)

    if len(broker_trades) == 0:
        return None

    # Build daily net buy dict (default 0 for non-trading days)
    net_buys_raw = {}
    for row in broker_trades.iter_rows(named=True):
        net_buys_raw[row["date"]] = (row["buy_shares"] or 0) - (row["sell_shares"] or 0)

    # For timing analysis, we need net_buy series aligned with dates
    # Only consider dates where we have return data
    valid_dates = [d for d in all_dates if d in returns]

    # Get net_buy for each valid date (0 if no trade)
    net_buys = [net_buys_raw.get(d, 0) for d in valid_dates]
    daily_returns = [returns[d] for d in valid_dates]

    if len(valid_dates) < 20:
        return None

    # === Lead Correlation ===
    # corr(net_buy[t-1], return[t])
    lead_x = net_buys[:-1]  # net_buy from day 0 to n-2
    lead_y = daily_returns[1:]  # return from day 1 to n-1

    lead_corr = pearson_correlation(lead_x, lead_y)

    # === Timing Alpha (Demeaned) ===
    # Σ((net_buy[t-1] - avg) × return[t])
    avg_net_buy = sum(net_buys) / len(net_buys)

    timing_alpha = 0.0
    for i in range(1, len(valid_dates)):
        demean_nb = net_buys[i-1] - avg_net_buy
        timing_alpha += demean_nb * daily_returns[i]

    # === Normalize Timing Alpha ===
    # Divide by std(net_buy) to make it comparable across brokers
    # This gives us something like: sum of (z-score × return)
    variance_nb = sum((nb - avg_net_buy)**2 for nb in net_buys) / len(net_buys)
    std_nb = math.sqrt(variance_nb) if variance_nb > 0 else 1

    normalized_timing = timing_alpha / std_nb if std_nb > 0 else 0

    # === Trading stats ===
    trading_days = len(broker_trades)
    total_buy = broker_trades["buy_shares"].sum() or 0
    total_sell = broker_trades["sell_shares"].sum() or 0
    total_volume = total_buy + total_sell
    cumulative_net = sum(net_buys)

    # Skip if too little activity
    if trading_days < 20 or total_volume < 100:
        return None

    return {
        "broker": broker,
        "trading_days": trading_days,
        "total_volume": total_volume,
        "cumulative_net": cumulative_net,
        "avg_net_buy": avg_net_buy,
        "std_net_buy": std_nb,
        "lead_corr": lead_corr,
        "timing_alpha": timing_alpha,
        "normalized_timing": normalized_timing,
    }


def main():
    print("載入資料...")
    trade_df, price_df, maps, broker_names = load_data()
    returns = calculate_returns(price_df)

    # Get sorted dates
    all_dates = sorted(maps["dates"].keys())
    print(f"分析期間：{all_dates[0]} ~ {all_dates[-1]}")
    print(f"交易日數：{len(all_dates)}")

    # Market stats
    total_return = sum(returns.values())
    avg_return = total_return / len(returns)
    print(f"累積日報酬總和：{total_return*100:.1f}%")
    print(f"平均日報酬：{avg_return*100:.4f}%")

    # Analyze all brokers
    print(f"\n分析 {len(maps['brokers'])} 個券商...")
    results = []

    for broker in maps["brokers"]:
        result = analyze_broker_timing(broker, trade_df, returns, all_dates)
        if result:
            result["name"] = broker_names.get(broker, "")
            results.append(result)

    print(f"有效券商數（交易日>=20, 交易量>=100）：{len(results)}")

    df = pl.DataFrame(results)

    # === Report 1: 核心概念解釋 ===
    print(f"\n{'='*90}")
    print("【擇時 Alpha 分析框架】")
    print("="*90)
    print("""
公式：timing_alpha = Σ((net_buy[t-1] - avg_net_buy) × return[t])

解讀：
  - 正值：在高報酬日「買得比平均多」→ 正確擇時
  - 負值：在高報酬日「買得比平均少」→ 錯誤擇時
  - 零值：每天買賣相同，或與報酬無關 → 無擇時能力

與 Lead 相關的關係：
  - Lead = corr(net_buy, return)：方向一致性（-1 到 +1）
  - Timing Alpha ≈ Lead × std(net_buy) × n：累積貢獻（絕對值）
""")

    # === Report 2: Timing Alpha 排行 ===
    print(f"\n{'='*90}")
    print("【Timing Alpha Top 20】（正確擇時：高報酬日買得多）")
    print("="*90)
    print(f"\n{'排名':<4} {'券商':<10} {'名稱':<14} {'Timing Alpha':>14} {'正規化':>12} {'Lead':>8} {'累計淨買':>12}")
    print("-" * 90)

    top_timing = df.sort("timing_alpha", descending=True).head(20)
    for i, row in enumerate(top_timing.iter_rows(named=True), 1):
        print(f"{i:<4} {row['broker']:<10} {row['name']:<14} "
              f"{row['timing_alpha']:>+13,.0f} {row['normalized_timing']:>+11,.1f} "
              f"{row['lead_corr']:>+7.4f} {row['cumulative_net']:>+11,}")

    print(f"\n【Timing Alpha Bottom 20】（錯誤擇時：高報酬日買得少）")
    print(f"\n{'排名':<4} {'券商':<10} {'名稱':<14} {'Timing Alpha':>14} {'正規化':>12} {'Lead':>8} {'累計淨買':>12}")
    print("-" * 90)

    bottom_timing = df.sort("timing_alpha").head(20)
    for i, row in enumerate(bottom_timing.iter_rows(named=True), 1):
        print(f"{i:<4} {row['broker']:<10} {row['name']:<14} "
              f"{row['timing_alpha']:>+13,.0f} {row['normalized_timing']:>+11,.1f} "
              f"{row['lead_corr']:>+7.4f} {row['cumulative_net']:>+11,}")

    # === Report 3: Lead vs Timing Alpha 關係 ===
    print(f"\n{'='*90}")
    print("【Lead 相關 vs Timing Alpha 關係】")
    print("="*90)

    leads = df["lead_corr"].to_list()
    timings = df["timing_alpha"].to_list()
    norm_timings = df["normalized_timing"].to_list()

    corr_lead_timing = pearson_correlation(leads, timings)
    corr_lead_norm = pearson_correlation(leads, norm_timings)

    print(f"\n Lead 相關 vs Timing Alpha：{corr_lead_timing:+.4f}")
    print(f" Lead 相關 vs 正規化 Timing：{corr_lead_norm:+.4f}")

    print(f"""
解讀：
  - 相關 ≈ 1：Timing Alpha 主要由 Lead 決定（方向 × 波動）
  - 相關 < 1：有些券商 Lead 高但 Timing 低（方向對但交易量小）
            有些券商 Lead 低但 Timing 高（方向不穩但押注大）
""")

    # === Report 4: 大型券商分析 ===
    print(f"\n{'='*90}")
    print("【大型券商擇時分析】（交易量前20）")
    print("="*90)

    large_brokers = df.sort("total_volume", descending=True).head(20)

    print(f"\n{'券商':<10} {'名稱':<12} {'交易量':>12} {'Timing':>12} {'Lead':>8} {'累計淨買':>12} {'方向':<6}")
    print("-" * 85)

    for row in large_brokers.iter_rows(named=True):
        direction = "做多" if row["cumulative_net"] > 0 else "做空" if row["cumulative_net"] < 0 else "中性"
        print(f"{row['broker']:<10} {row['name']:<12} "
              f"{row['total_volume']:>12,} {row['timing_alpha']:>+11,.0f} "
              f"{row['lead_corr']:>+7.4f} {row['cumulative_net']:>+11,} "
              f"{direction}")

    # === Report 5: 統計摘要 ===
    print(f"\n{'='*90}")
    print("【統計摘要】")
    print("="*90)

    positive_timing = sum(1 for t in timings if t > 0)
    positive_lead = sum(1 for l in leads if l > 0.05)
    negative_lead = sum(1 for l in leads if l < -0.05)
    neutral_lead = len(leads) - positive_lead - negative_lead

    print(f"\nTiming Alpha 分布：")
    print(f"  正值（正確擇時）：{positive_timing} ({positive_timing/len(timings)*100:.1f}%)")
    print(f"  負值（錯誤擇時）：{len(timings) - positive_timing} ({(len(timings)-positive_timing)/len(timings)*100:.1f}%)")

    print(f"\nLead 相關分布：")
    print(f"  正向 (>+0.05)：{positive_lead} ({positive_lead/len(leads)*100:.1f}%)")
    print(f"  中性 (-0.05~+0.05)：{neutral_lead} ({neutral_lead/len(leads)*100:.1f}%)")
    print(f"  負向 (<-0.05)：{negative_lead} ({negative_lead/len(leads)*100:.1f}%)")

    avg_timing = sum(timings) / len(timings)
    median_timing = sorted(timings)[len(timings)//2]
    avg_lead = sum(leads) / len(leads)

    print(f"\n數值統計：")
    print(f"  Timing Alpha：平均 {avg_timing:+,.0f}，中位數 {median_timing:+,.0f}")
    print(f"  Lead 相關：平均 {avg_lead:+.4f}")

    # === Key Insight ===
    print(f"\n{'='*90}")
    print("【關鍵結論】")
    print("="*90)

    # High Lead + High Timing
    high_both = df.filter((pl.col("lead_corr") > 0.05) & (pl.col("timing_alpha") > 10000))

    print(f"\n同時具備：Lead > +5% 且 Timing Alpha > +10,000：{len(high_both)} 個")

    if len(high_both) > 0:
        print(f"\n【值得關注的券商】")
        for row in high_both.sort("timing_alpha", descending=True).head(10).iter_rows(named=True):
            print(f"  {row['broker']} {row['name']}: Lead={row['lead_corr']:+.3f}, Timing={row['timing_alpha']:+,.0f}")

    # Sanity check: correlation should be high
    if corr_lead_timing > 0.8:
        print(f"\n→ Lead 與 Timing Alpha 高度相關（{corr_lead_timing:.2f}）")
        print(f"  這符合預期：Timing Alpha ≈ Lead × 交易波動")
    else:
        print(f"\n→ Lead 與 Timing Alpha 相關較低（{corr_lead_timing:.2f}）")
        print(f"  表示「方向判斷」和「押注大小」有獨立的資訊")


if __name__ == "__main__":
    main()
