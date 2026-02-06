"""Broker Scorecard: Integrated analysis of all metrics.

Consolidates all evaluation dimensions:
1. Direction: Long/Short position
2. PNL: Realized + Unrealized profit/loss
3. Execution Alpha: Trade price vs close price
4. Timing Alpha: Market timing ability
5. Statistical Significance: Permutation test p-value
6. Trading Style: Trend-following vs contrarian
"""

import json
import random
import math
from pathlib import Path

import numpy as np
import polars as pl


def load_all_data():
    """Load all required data files."""
    trade_df = pl.read_parquet("daily_trade_summary.parquet")
    price_df = pl.read_parquet("price_master.parquet")
    closed_trades = pl.read_parquet("closed_trades.parquet")
    realized = np.load("realized_pnl.npy")
    unrealized = np.load("unrealized_pnl.npy")

    with open("index_maps.json") as f:
        maps = json.load(f)

    with open("broker_names.json") as f:
        broker_names = json.load(f)

    return trade_df, price_df, closed_trades, realized, unrealized, maps, broker_names


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


def calculate_timing_alpha(net_buys: list[int], daily_returns: list[float]) -> float:
    """Calculate timing alpha from aligned series."""
    if len(net_buys) < 2:
        return 0.0

    avg_net_buy = sum(net_buys) / len(net_buys)

    timing_alpha = 0.0
    for i in range(1, len(net_buys)):
        demean_nb = net_buys[i-1] - avg_net_buy
        timing_alpha += demean_nb * daily_returns[i]

    return timing_alpha


def permutation_test(net_buys: list[int], daily_returns: list[float], n_perm: int = 500) -> float:
    """Run permutation test, return p-value."""
    real_alpha = calculate_timing_alpha(net_buys, daily_returns)

    n_extreme = 0
    for _ in range(n_perm):
        shuffled = net_buys.copy()
        random.shuffle(shuffled)
        sim_alpha = calculate_timing_alpha(shuffled, daily_returns)
        if abs(sim_alpha) >= abs(real_alpha):
            n_extreme += 1

    return n_extreme / n_perm


def analyze_broker(
    broker: str,
    trade_df: pl.DataFrame,
    price_df: pl.DataFrame,
    closed_trades: pl.DataFrame,
    realized: np.ndarray,
    unrealized: np.ndarray,
    maps: dict,
    returns: dict[str, float],
    all_dates: list[str],
    price_dict: dict[str, float],
) -> dict | None:
    """Complete analysis of a single broker."""

    broker_trades = trade_df.filter(pl.col("broker") == broker)
    if len(broker_trades) == 0:
        return None

    # === 1. Basic Stats ===
    trading_days = len(broker_trades)
    total_buy = broker_trades["buy_shares"].sum() or 0
    total_sell = broker_trades["sell_shares"].sum() or 0
    total_volume = total_buy + total_sell
    buy_amount = broker_trades["buy_amount"].sum() or 0
    sell_amount = broker_trades["sell_amount"].sum() or 0
    total_amount = buy_amount + sell_amount

    if trading_days < 20 or total_volume < 1000:
        return None

    # === 2. Direction ===
    cumulative_net = total_buy - total_sell
    direction = "做多" if cumulative_net > 0 else "做空" if cumulative_net < 0 else "中性"

    # === 3. PNL ===
    broker_idx = maps["brokers"].get(broker)
    if broker_idx is None:
        return None

    sym_idx = 0
    total_realized = float(realized[sym_idx, :, broker_idx].sum())
    final_unrealized = float(unrealized[sym_idx, -1, broker_idx])
    total_pnl = total_realized + final_unrealized

    # === 4. Execution Alpha ===
    broker_closed = closed_trades.filter(pl.col("broker") == broker)

    if len(broker_closed) > 0:
        # Add close prices
        broker_closed = broker_closed.with_columns([
            pl.col("buy_date").map_elements(
                lambda d: price_dict.get(d, 0.0), return_dtype=pl.Float64
            ).alias("close_at_buy"),
            pl.col("sell_date").map_elements(
                lambda d: price_dict.get(d, 0.0), return_dtype=pl.Float64
            ).alias("close_at_sell"),
        ])

        valid_closed = broker_closed.filter(
            (pl.col("close_at_buy") > 0) & (pl.col("close_at_sell") > 0)
        )

        if len(valid_closed) > 0:
            # Calculate alpha for each trade
            valid_closed = valid_closed.with_columns([
                pl.when(pl.col("trade_type") == "long")
                .then((pl.col("sell_price") - pl.col("buy_price")) / pl.col("buy_price"))
                .otherwise((pl.col("buy_price") - pl.col("sell_price")) / pl.col("buy_price"))
                .alias("trade_return"),

                pl.when(pl.col("trade_type") == "long")
                .then((pl.col("close_at_sell") - pl.col("close_at_buy")) / pl.col("close_at_buy"))
                .otherwise((pl.col("close_at_buy") - pl.col("close_at_sell")) / pl.col("close_at_buy"))
                .alias("benchmark_return"),

                (pl.col("shares") * pl.col("buy_price")).alias("trade_value"),
            ]).with_columns([
                (pl.col("trade_return") - pl.col("benchmark_return")).alias("alpha"),
            ])

            total_trade_value = valid_closed["trade_value"].sum()
            alpha_dollars = (valid_closed["alpha"] * valid_closed["trade_value"]).sum()
            exec_alpha = alpha_dollars / total_trade_value if total_trade_value > 0 else 0
            trade_count = len(valid_closed)
        else:
            exec_alpha = 0.0
            trade_count = 0
    else:
        exec_alpha = 0.0
        trade_count = 0

    # === 5. Timing Alpha ===
    net_buys_raw = {}
    for row in broker_trades.iter_rows(named=True):
        net_buys_raw[row["date"]] = (row["buy_shares"] or 0) - (row["sell_shares"] or 0)

    valid_dates = [d for d in all_dates if d in returns]
    net_buys = [net_buys_raw.get(d, 0) for d in valid_dates]
    daily_returns = [returns[d] for d in valid_dates]

    timing_alpha = calculate_timing_alpha(net_buys, daily_returns)

    # === 6. Lead Correlation ===
    lead_x = net_buys[:-1]
    lead_y = daily_returns[1:]
    lead_corr = pearson_correlation(lead_x, lead_y)

    # === 7. Lag Correlation (trading style) ===
    lag_x = daily_returns[:-1]
    lag_y = net_buys[1:]
    lag_corr = pearson_correlation(lag_x, lag_y)

    if lag_corr > 0.05:
        style = "順勢"
    elif lag_corr < -0.05:
        style = "逆勢"
    else:
        style = "中性"

    # === 8. Permutation Test (simplified for speed) ===
    p_value = permutation_test(net_buys, daily_returns, n_perm=200)

    if p_value < 0.05 and timing_alpha > 0:
        timing_sig = "⭐ 顯著正向"
    elif p_value < 0.05 and timing_alpha < 0:
        timing_sig = "⚠️ 顯著負向"
    else:
        timing_sig = "不顯著"

    return {
        "broker": broker,
        "trading_days": trading_days,
        "total_volume": total_volume,
        "total_amount": total_amount,
        "direction": direction,
        "cumulative_net": cumulative_net,
        "total_pnl": total_pnl,
        "exec_alpha": exec_alpha,
        "trade_count": trade_count,
        "timing_alpha": timing_alpha,
        "lead_corr": lead_corr,
        "lag_corr": lag_corr,
        "style": style,
        "p_value": p_value,
        "timing_sig": timing_sig,
    }


def main():
    print("載入資料...")
    trade_df, price_df, closed_trades, realized, unrealized, maps, broker_names = load_all_data()
    returns = calculate_returns(price_df)
    all_dates = sorted(maps["dates"].keys())
    price_dict = {r["date"]: r["close_price"] for r in price_df.iter_rows(named=True)}

    # Market stats
    first_price = price_df.sort("date").head(1)["close_price"].item()
    last_price = price_df.sort("date").tail(1)["close_price"].item()
    market_return = (last_price - first_price) / first_price

    print(f"分析期間：{all_dates[0]} ~ {all_dates[-1]}")
    print(f"市場報酬：{market_return*100:.1f}%")

    # === Analyze all brokers ===
    print(f"\n分析 {len(maps['brokers'])} 個券商...")
    results = []

    for i, broker in enumerate(maps["brokers"]):
        if i % 100 == 0:
            print(f"  進度：{i}/{len(maps['brokers'])}...")

        result = analyze_broker(
            broker, trade_df, price_df, closed_trades,
            realized, unrealized, maps, returns, all_dates, price_dict
        )
        if result:
            result["name"] = broker_names.get(broker, "")
            results.append(result)

    print(f"\n有效券商數：{len(results)}")

    df = pl.DataFrame(results)

    # === 指標說明 ===
    print(f"\n{'='*100}")
    print("【券商評估指標體系】")
    print("="*100)
    print("""
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│  維度          │ 指標              │ 意義                           │ 好的表現              │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│  1. 方向       │ 累計淨買          │ 做多 or 做空                    │ 牛市做多、熊市做空     │
│  2. 獲利       │ 總 PNL            │ 最終賺了多少                    │ 正值越大越好          │
│  3. 執行品質   │ 執行 Alpha        │ 成交價 vs 收盤價                │ 正值 = 執行優          │
│  4. 擇時能力   │ Timing Alpha      │ 高報酬日買得多？                │ 正值 = 擇時對          │
│  5. 統計顯著   │ p-value           │ 是運氣還是實力？                │ < 0.05 = 真的有能力    │
│  6. 交易風格   │ Lag 相關          │ 順勢追蹤 or 逆勢操作            │ 無好壞，只是風格       │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
""")

    # === 大型券商完整評估 ===
    print(f"\n{'='*100}")
    print("【大型券商完整評估】（交易額前 20）")
    print("="*100)

    large = df.sort("total_amount", descending=True).head(20)

    print(f"\n{'券商':<8} {'名稱':<10} {'方向':<4} {'PNL':>12} {'執行Alpha':>9} {'Timing':>10} {'p-value':>8} {'顯著性':<12} {'風格':<4}")
    print("-" * 100)

    for row in large.iter_rows(named=True):
        print(f"{row['broker']:<8} {row['name']:<10} {row['direction']:<4} "
              f"{row['total_pnl']/1e8:>+11.2f}億 {row['exec_alpha']*100:>+8.2f}% "
              f"{row['timing_alpha']:>+9,.0f} {row['p_value']:>8.3f} {row['timing_sig']:<12} {row['style']:<4}")

    # === 綜合評分最高的券商 ===
    print(f"\n{'='*100}")
    print("【綜合表現最佳】（PNL > 0、Timing 顯著正向、執行 Alpha > 0）")
    print("="*100)

    best = df.filter(
        (pl.col("total_pnl") > 0) &
        (pl.col("p_value") < 0.05) &
        (pl.col("timing_alpha") > 0) &
        (pl.col("exec_alpha") > 0)
    ).sort("total_pnl", descending=True)

    if len(best) > 0:
        print(f"\n符合條件：{len(best)} 個")
        print(f"\n{'排名':<4} {'券商':<8} {'名稱':<12} {'方向':<4} {'PNL':>12} {'執行Alpha':>9} {'Timing':>10} {'p-value':>8}")
        print("-" * 85)

        for i, row in enumerate(best.head(15).iter_rows(named=True), 1):
            print(f"{i:<4} {row['broker']:<8} {row['name']:<12} {row['direction']:<4} "
                  f"{row['total_pnl']/1e8:>+11.2f}億 {row['exec_alpha']*100:>+8.2f}% "
                  f"{row['timing_alpha']:>+9,.0f} {row['p_value']:>8.3f}")
    else:
        print("\n沒有券商同時滿足所有條件。")

    # === 反指標（可反著做）===
    print(f"\n{'='*100}")
    print("【反指標】（Timing 顯著負向，可考慮反著做）")
    print("="*100)

    contrarian = df.filter(
        (pl.col("p_value") < 0.05) &
        (pl.col("timing_alpha") < 0)
    ).sort("timing_alpha")

    if len(contrarian) > 0:
        print(f"\n符合條件：{len(contrarian)} 個")
        print(f"\n{'排名':<4} {'券商':<8} {'名稱':<12} {'方向':<4} {'Timing':>12} {'p-value':>8} {'PNL':>12}")
        print("-" * 75)

        for i, row in enumerate(contrarian.head(10).iter_rows(named=True), 1):
            print(f"{i:<4} {row['broker']:<8} {row['name']:<12} {row['direction']:<4} "
                  f"{row['timing_alpha']:>+11,.0f} {row['p_value']:>8.3f} {row['total_pnl']/1e8:>+11.2f}億")

    # === 統計摘要 ===
    print(f"\n{'='*100}")
    print("【統計摘要】")
    print("="*100)

    total = len(df)
    profitable = len(df.filter(pl.col("total_pnl") > 0))
    positive_exec = len(df.filter(pl.col("exec_alpha") > 0))
    positive_timing = len(df.filter(pl.col("timing_alpha") > 0))
    significant = len(df.filter(pl.col("p_value") < 0.05))
    sig_positive = len(df.filter((pl.col("p_value") < 0.05) & (pl.col("timing_alpha") > 0)))
    sig_negative = len(df.filter((pl.col("p_value") < 0.05) & (pl.col("timing_alpha") < 0)))

    print(f"""
總券商數：{total}

【方向判斷】
  獲利（PNL > 0）：{profitable} ({profitable/total*100:.1f}%)
  虧損（PNL < 0）：{total - profitable} ({(total-profitable)/total*100:.1f}%)

【執行品質】
  執行 Alpha > 0：{positive_exec} ({positive_exec/total*100:.1f}%)
  執行 Alpha < 0：{total - positive_exec} ({(total-positive_exec)/total*100:.1f}%)

【擇時能力】
  Timing Alpha > 0：{positive_timing} ({positive_timing/total*100:.1f}%)
  Timing Alpha < 0：{total - positive_timing} ({(total-positive_timing)/total*100:.1f}%)

【統計顯著】（p < 0.05）
  顯著正向：{sig_positive} ({sig_positive/total*100:.1f}%) ← 真的有擇時能力
  顯著負向：{sig_negative} ({sig_negative/total*100:.1f}%) ← 反指標
  不顯著：{total - significant} ({(total-significant)/total*100:.1f}%) ← 運氣
""")


if __name__ == "__main__":
    random.seed(42)
    main()
