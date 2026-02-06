"""Permutation Test for Timing Alpha.

Question: Is this broker's timing skill real, or just luck?

Method:
1. Calculate real Timing Alpha
2. Shuffle trading dates randomly (keep trade sizes, shuffle timing)
3. Calculate simulated Timing Alpha
4. Repeat N times
5. See where real value falls in the distribution

If real Timing Alpha is in top 5% of simulations → statistically significant (p < 0.05)
"""

import json
import random
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


def permutation_test(
    net_buys: list[int],
    daily_returns: list[float],
    n_permutations: int = 1000,
) -> tuple[float, float, list[float]]:
    """Run permutation test for timing alpha.

    Returns: (real_alpha, p_value, simulated_alphas)
    """
    # Real timing alpha
    real_alpha = calculate_timing_alpha(net_buys, daily_returns)

    # Permutation test: shuffle net_buys, keep returns fixed
    simulated_alphas = []

    for _ in range(n_permutations):
        # Shuffle net_buys (randomize timing of trades)
        shuffled_nb = net_buys.copy()
        random.shuffle(shuffled_nb)

        sim_alpha = calculate_timing_alpha(shuffled_nb, daily_returns)
        simulated_alphas.append(sim_alpha)

    # Calculate p-value (two-tailed)
    # How often is |simulated| >= |real|?
    n_extreme = sum(1 for s in simulated_alphas if abs(s) >= abs(real_alpha))
    p_value = n_extreme / n_permutations

    return real_alpha, p_value, simulated_alphas


def analyze_broker(
    broker: str,
    trade_df: pl.DataFrame,
    returns: dict[str, float],
    all_dates: list[str],
    n_permutations: int = 1000,
) -> dict | None:
    """Analyze a broker with permutation test."""

    broker_trades = trade_df.filter(pl.col("broker") == broker)

    if len(broker_trades) == 0:
        return None

    # Build daily net buy dict
    net_buys_raw = {}
    for row in broker_trades.iter_rows(named=True):
        net_buys_raw[row["date"]] = (row["buy_shares"] or 0) - (row["sell_shares"] or 0)

    # Align with dates that have returns
    valid_dates = [d for d in all_dates if d in returns]
    net_buys = [net_buys_raw.get(d, 0) for d in valid_dates]
    daily_returns = [returns[d] for d in valid_dates]

    # Skip if too little activity
    trading_days = len([nb for nb in net_buys if nb != 0])
    if trading_days < 20:
        return None

    # Run permutation test
    real_alpha, p_value, simulated = permutation_test(
        net_buys, daily_returns, n_permutations
    )

    # Calculate percentile
    n_below = sum(1 for s in simulated if s < real_alpha)
    percentile = n_below / len(simulated) * 100

    # Stats
    cumulative_net = sum(net_buys)
    total_volume = sum(abs(nb) for nb in net_buys)

    return {
        "broker": broker,
        "trading_days": trading_days,
        "total_volume": total_volume,
        "cumulative_net": cumulative_net,
        "real_alpha": real_alpha,
        "p_value": p_value,
        "percentile": percentile,
        "sim_mean": sum(simulated) / len(simulated),
        "sim_std": (sum((s - sum(simulated)/len(simulated))**2 for s in simulated) / len(simulated)) ** 0.5,
    }


def main():
    print("載入資料...")
    trade_df, price_df, maps, broker_names = load_data()
    returns = calculate_returns(price_df)
    all_dates = sorted(maps["dates"].keys())

    print(f"分析期間：{all_dates[0]} ~ {all_dates[-1]}")

    # === 先分析幾個重點券商 ===
    print(f"\n{'='*90}")
    print("【重點券商 Permutation Test】（1000 次模擬）")
    print("="*90)

    focus_brokers = [
        ("1480", "美商高盛"),
        ("8440", "摩根大通"),
        ("1650", "新加坡商瑞銀"),
        ("1440", "美林"),
        ("9100", "群益金鼎"),
        ("1590", "花旗環球"),
        ("9800", "元大"),
        ("9268", "凱基-台北"),
    ]

    print("""
Permutation Test 原理：
  1. 計算真實的 Timing Alpha
  2. 隨機打亂交易日期 1000 次，計算模擬的 Timing Alpha
  3. 看真實值在模擬分布中的位置

解讀：
  - p < 0.05：擇時能力統計顯著（不太可能是運氣）
  - p > 0.05：可能只是運氣
  - Percentile > 95%：擇時能力顯著為正
  - Percentile < 5%：擇時能力顯著為負
""")

    for broker_code, broker_name in focus_brokers:
        result = analyze_broker(broker_code, trade_df, returns, all_dates, n_permutations=1000)

        if result is None:
            continue

        print(f"\n【{broker_code}】{broker_name}")
        print(f"  累計淨買：{result['cumulative_net']:+,} 張（{'做多' if result['cumulative_net'] > 0 else '做空'}）")
        print(f"  真實 Timing Alpha：{result['real_alpha']:+,.0f}")
        print(f"  模擬平均：{result['sim_mean']:+,.0f}（標準差：{result['sim_std']:,.0f}）")
        print(f"  Percentile：{result['percentile']:.1f}%")
        print(f"  p-value：{result['p_value']:.3f}")

        if result['p_value'] < 0.01:
            print(f"  → ⭐⭐ 高度顯著（p < 0.01）：擇時能力非常不可能是運氣")
        elif result['p_value'] < 0.05:
            print(f"  → ⭐ 顯著（p < 0.05）：擇時能力不太可能是運氣")
        elif result['p_value'] < 0.10:
            print(f"  → 邊緣顯著（p < 0.10）：可能有擇時能力，但證據不夠強")
        else:
            print(f"  → 不顯著（p >= 0.10）：擇時可能只是運氣")

    # === 全部券商分析 ===
    print(f"\n{'='*90}")
    print("【全部券商 Permutation Test】（500 次模擬，加速計算）")
    print("="*90)

    all_results = []
    total = len(maps["brokers"])

    for i, broker in enumerate(maps["brokers"]):
        if i % 100 == 0:
            print(f"  進度：{i}/{total}...")

        result = analyze_broker(broker, trade_df, returns, all_dates, n_permutations=500)
        if result:
            result["name"] = broker_names.get(broker, "")
            all_results.append(result)

    print(f"\n有效券商數：{len(all_results)}")

    # Convert to DataFrame
    df = pl.DataFrame(all_results)

    # === 統計顯著的券商 ===
    print(f"\n{'='*90}")
    print("【統計顯著的擇時能力】（p < 0.05）")
    print("="*90)

    significant = df.filter(pl.col("p_value") < 0.05)
    positive_sig = significant.filter(pl.col("real_alpha") > 0).sort("real_alpha", descending=True)
    negative_sig = significant.filter(pl.col("real_alpha") < 0).sort("real_alpha")

    print(f"\n統計顯著的券商：{len(significant)} / {len(all_results)} ({len(significant)/len(all_results)*100:.1f}%)")
    print(f"  正向顯著（擇時正確）：{len(positive_sig)}")
    print(f"  負向顯著（擇時錯誤）：{len(negative_sig)}")

    # Top positive significant
    print(f"\n【正向顯著 Top 15】（擇時能力真的好）")
    print(f"{'排名':<4} {'券商':<10} {'名稱':<14} {'Timing Alpha':>14} {'p-value':>10} {'Percentile':>12}")
    print("-" * 75)

    for i, row in enumerate(positive_sig.head(15).iter_rows(named=True), 1):
        print(f"{i:<4} {row['broker']:<10} {row['name']:<14} "
              f"{row['real_alpha']:>+13,.0f} {row['p_value']:>9.3f} {row['percentile']:>11.1f}%")

    # Top negative significant
    print(f"\n【負向顯著 Top 15】（擇時能力真的差）")
    print(f"{'排名':<4} {'券商':<10} {'名稱':<14} {'Timing Alpha':>14} {'p-value':>10} {'Percentile':>12}")
    print("-" * 75)

    for i, row in enumerate(negative_sig.head(15).iter_rows(named=True), 1):
        print(f"{i:<4} {row['broker']:<10} {row['name']:<14} "
              f"{row['real_alpha']:>+13,.0f} {row['p_value']:>9.3f} {row['percentile']:>11.1f}%")

    # === 預期 vs 實際 ===
    print(f"\n{'='*90}")
    print("【預期 vs 實際】")
    print("="*90)

    # Under null hypothesis, we expect 5% to be significant by chance
    expected_sig = len(all_results) * 0.05
    actual_sig = len(significant)

    print(f"""
如果所有券商都沒有擇時能力（純靠運氣）：
  - 預期 {expected_sig:.0f} 個會在 p < 0.05（純粹因為機率）
  - 實際 {actual_sig} 個達到 p < 0.05

{'→ 實際顯著數量超過預期，部分券商可能真的有擇時能力' if actual_sig > expected_sig * 1.5 else '→ 實際與預期接近，大部分「顯著」可能只是運氣'}
""")

    # === 大型券商的顯著性 ===
    print(f"\n{'='*90}")
    print("【大型券商擇時顯著性檢定】（交易量前20）")
    print("="*90)

    large = df.sort("total_volume", descending=True).head(20)

    print(f"\n{'券商':<10} {'名稱':<12} {'Timing Alpha':>12} {'p-value':>10} {'顯著？':<8} {'方向':<6}")
    print("-" * 70)

    for row in large.iter_rows(named=True):
        sig_mark = "⭐⭐" if row['p_value'] < 0.01 else "⭐" if row['p_value'] < 0.05 else ""
        direction = "做多" if row["cumulative_net"] > 0 else "做空"
        print(f"{row['broker']:<10} {row['name']:<12} "
              f"{row['real_alpha']:>+11,.0f} {row['p_value']:>9.3f} {sig_mark:<8} {direction}")


if __name__ == "__main__":
    random.seed(42)  # For reproducibility
    main()
