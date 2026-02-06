"""Predictive Ability Analysis: Which brokers show consistent judgment?

New framework based on user feedback:
- Question: Who can "predict" price? (not "influence")
- Filter: Trading days >= 50, avg daily volume >= 10 shares
- Metrics:
  1. Lead coefficient stability (first half vs second half)
  2. Monthly win rate (% of months with positive realized PNL)
  3. Risk-adjusted return (total PNL / total volume)
"""

import json
from pathlib import Path
from collections import defaultdict
import math

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

    return trade_df, price_df, realized, unrealized, maps


def calculate_returns(price_df: pl.DataFrame) -> dict[str, float]:
    """Calculate daily returns for the stock."""
    prices = price_df.sort("date")
    dates = prices["date"].to_list()
    closes = prices["close_price"].to_list()

    returns = {}
    for i in range(1, len(dates)):
        if closes[i-1] > 0:
            returns[dates[i]] = (closes[i] - closes[i-1]) / closes[i-1]

    return returns


def pearson_correlation(x: list, y: list) -> tuple[float, float]:
    """Calculate Pearson correlation and p-value."""
    n = len(x)
    if n < 10:
        return 0.0, 1.0

    mean_x = sum(x) / n
    mean_y = sum(y) / n

    numerator = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
    denom_x = math.sqrt(sum((xi - mean_x)**2 for xi in x))
    denom_y = math.sqrt(sum((yi - mean_y)**2 for yi in y))

    if denom_x == 0 or denom_y == 0:
        return 0.0, 1.0

    r = numerator / (denom_x * denom_y)

    # t-test for correlation significance
    if abs(r) >= 1:
        return r, 0.0
    t_stat = r * math.sqrt(n - 2) / math.sqrt(1 - r**2)
    # Approximate p-value using normal distribution for large n
    p_value = 2 * (1 - 0.5 * (1 + math.erf(abs(t_stat) / math.sqrt(2))))

    return r, p_value


def analyze_broker(
    broker: str,
    trade_df: pl.DataFrame,
    returns: dict[str, float],
    realized: np.ndarray,
    unrealized: np.ndarray,
    maps: dict,
) -> dict | None:
    """Analyze a single broker's predictive ability."""

    broker_trades = trade_df.filter(pl.col("broker") == broker).sort("date")

    if len(broker_trades) == 0:
        return None

    # Calculate trading stats
    trading_days = len(broker_trades)
    total_volume = broker_trades.select(
        (pl.col("buy_shares") + pl.col("sell_shares")).sum()
    ).item()
    avg_daily_volume = total_volume / trading_days if trading_days > 0 else 0

    # Filter: trading days >= 50, avg volume >= 10
    if trading_days < 50 or avg_daily_volume < 10:
        return None

    # Build daily net buy series
    dates = broker_trades["date"].to_list()
    net_buys = {}
    for row in broker_trades.iter_rows(named=True):
        net_buys[row["date"]] = (row["buy_shares"] or 0) - (row["sell_shares"] or 0)

    # Sort dates for time series analysis
    all_dates = sorted(set(dates) & set(returns.keys()))

    if len(all_dates) < 20:
        return None

    # === Lead Coefficient Analysis ===
    # Lead: net_buy[t-1] vs return[t]
    lead_x, lead_y = [], []
    for i in range(1, len(all_dates)):
        prev_date = all_dates[i-1]
        curr_date = all_dates[i]
        if prev_date in net_buys and curr_date in returns:
            lead_x.append(net_buys[prev_date])
            lead_y.append(returns[curr_date])

    lead_corr, lead_p = pearson_correlation(lead_x, lead_y)

    # Split into first half and second half for stability check
    mid = len(all_dates) // 2
    first_half_dates = all_dates[:mid]
    second_half_dates = all_dates[mid:]

    # First half lead coefficient
    lead_x1, lead_y1 = [], []
    for i in range(1, len(first_half_dates)):
        prev_date = first_half_dates[i-1]
        curr_date = first_half_dates[i]
        if prev_date in net_buys and curr_date in returns:
            lead_x1.append(net_buys[prev_date])
            lead_y1.append(returns[curr_date])
    lead_corr_h1, _ = pearson_correlation(lead_x1, lead_y1)

    # Second half lead coefficient
    lead_x2, lead_y2 = [], []
    for i in range(1, len(second_half_dates)):
        prev_date = second_half_dates[i-1]
        curr_date = second_half_dates[i]
        if prev_date in net_buys and curr_date in returns:
            lead_x2.append(net_buys[prev_date])
            lead_y2.append(returns[curr_date])
    lead_corr_h2, _ = pearson_correlation(lead_x2, lead_y2)

    # Stability: same sign and both > 0.05 or both < -0.05
    lead_stable = (lead_corr_h1 * lead_corr_h2 > 0) and (abs(lead_corr_h1) > 0.05 and abs(lead_corr_h2) > 0.05)

    # === Monthly Win Rate ===
    broker_idx = maps["brokers"].get(broker)
    if broker_idx is None:
        return None

    # Group realized PNL by month
    sym_idx = 0  # Single stock (2345)
    date_to_idx = maps["dates"]
    monthly_pnl = defaultdict(float)

    for date, idx in date_to_idx.items():
        month = date[:7]  # "YYYY-MM"
        monthly_pnl[month] += realized[sym_idx, idx, broker_idx]

    months_with_data = [m for m, pnl in monthly_pnl.items() if pnl != 0]
    positive_months = sum(1 for m in months_with_data if monthly_pnl[m] > 0)
    monthly_win_rate = positive_months / len(months_with_data) if months_with_data else 0

    # === Risk-Adjusted Return ===
    total_realized = realized[sym_idx, :, broker_idx].sum()
    final_unrealized = unrealized[sym_idx, -1, broker_idx]
    total_pnl = total_realized + final_unrealized

    # PNL per share traded
    pnl_per_share = total_pnl / total_volume if total_volume > 0 else 0

    # === Cumulative Net Position ===
    cumulative_net_buy = sum(net_buys.values())

    return {
        "broker": broker,
        "trading_days": trading_days,
        "avg_daily_volume": avg_daily_volume,
        "total_volume": total_volume,
        "lead_corr": lead_corr,
        "lead_p": lead_p,
        "lead_corr_h1": lead_corr_h1,
        "lead_corr_h2": lead_corr_h2,
        "lead_stable": lead_stable,
        "monthly_win_rate": monthly_win_rate,
        "months_traded": len(months_with_data),
        "total_pnl": total_pnl,
        "pnl_per_share": pnl_per_share,
        "cumulative_net_buy": cumulative_net_buy,
    }


if __name__ == "__main__":
    print("Loading data...")
    trade_df, price_df, realized, unrealized, maps = load_data()
    returns = calculate_returns(price_df)

    print(f"Analyzing {len(maps['brokers'])} brokers...")

    results = []
    for broker in maps["brokers"]:
        result = analyze_broker(broker, trade_df, returns, realized, unrealized, maps)
        if result:
            results.append(result)

    print(f"\nFiltered to {len(results)} brokers (trading days >= 50, avg volume >= 10)")

    # Convert to DataFrame for analysis
    df = pl.DataFrame(results)

    # === Report 1: Lead Coefficient Stability ===
    print("\n" + "="*80)
    print("【分析一】Lead 係數穩定性（前後半年一致）")
    print("="*80)
    print("篩選條件：Lead 係數前後半年同號且 |r| > 0.05")
    print()

    stable_leads = df.filter(pl.col("lead_stable") == True).sort("lead_corr", descending=True)

    if len(stable_leads) > 0:
        print(f"符合條件：{len(stable_leads)} 個分點\n")
        print(f"{'券商':<15} {'Lead(全)':<10} {'Lead(H1)':<10} {'Lead(H2)':<10} {'p-value':<10} {'方向':<8}")
        print("-" * 70)
        for row in stable_leads.head(20).iter_rows(named=True):
            direction = "看多領先" if row["lead_corr"] > 0 else "看空領先"
            print(f"{row['broker']:<15} {row['lead_corr']:>+.4f}    {row['lead_corr_h1']:>+.4f}    {row['lead_corr_h2']:>+.4f}    {row['lead_p']:<.4f}    {direction}")
    else:
        print("沒有分點的 Lead 係數在前後半年都穩定。")

    # === Report 2: Monthly Win Rate ===
    print("\n" + "="*80)
    print("【分析二】月度勝率（正 PNL 月份比例）")
    print("="*80)
    print("篩選條件：交易月份 >= 6 個月")
    print()

    win_rate_df = (
        df.filter(pl.col("months_traded") >= 6)
        .sort("monthly_win_rate", descending=True)
    )

    print(f"{'券商':<15} {'勝率':<10} {'交易月數':<10} {'總PNL':<15} {'累計淨買':<12}")
    print("-" * 70)
    for row in win_rate_df.head(20).iter_rows(named=True):
        print(f"{row['broker']:<15} {row['monthly_win_rate']*100:>5.1f}%     {row['months_traded']:<10} {row['total_pnl']:>12,.0f}  {row['cumulative_net_buy']:>10,.0f}")

    # === Report 3: Risk-Adjusted Return (PNL per share) ===
    print("\n" + "="*80)
    print("【分析三】風險調整報酬（每張交易賺多少）")
    print("="*80)
    print("公式：總 PNL / 總交易量")
    print()

    pnl_df = df.sort("pnl_per_share", descending=True)

    print("=== Top 15 最高 ===")
    print(f"{'券商':<15} {'每張報酬':<12} {'總交易量':<12} {'總PNL':<15} {'交易日':<8}")
    print("-" * 70)
    for row in pnl_df.head(15).iter_rows(named=True):
        print(f"{row['broker']:<15} {row['pnl_per_share']:>+10,.1f}  {row['total_volume']:>10,.0f}  {row['total_pnl']:>12,.0f}  {row['trading_days']:>6}")

    print("\n=== Bottom 15 最低 ===")
    print(f"{'券商':<15} {'每張報酬':<12} {'總交易量':<12} {'總PNL':<15} {'交易日':<8}")
    print("-" * 70)
    for row in pnl_df.tail(15).reverse().iter_rows(named=True):
        print(f"{row['broker']:<15} {row['pnl_per_share']:>+10,.1f}  {row['total_volume']:>10,.0f}  {row['total_pnl']:>12,.0f}  {row['trading_days']:>6}")

    # === Report 4: Combined Score ===
    print("\n" + "="*80)
    print("【分析四】綜合評分（Lead穩定 + 高勝率 + 高報酬）")
    print("="*80)

    # Normalize metrics to 0-1 scale
    max_pnl_per_share = df["pnl_per_share"].max()
    min_pnl_per_share = df["pnl_per_share"].min()
    pnl_range = max_pnl_per_share - min_pnl_per_share if max_pnl_per_share != min_pnl_per_share else 1

    scored = df.with_columns([
        # Lead stability bonus (0 or 0.3)
        pl.when(pl.col("lead_stable") & (pl.col("lead_corr") > 0))
        .then(0.3)
        .otherwise(0.0)
        .alias("lead_score"),

        # Monthly win rate (0-0.4)
        (pl.col("monthly_win_rate") * 0.4).alias("win_rate_score"),

        # PNL per share normalized (0-0.3)
        ((pl.col("pnl_per_share") - min_pnl_per_share) / pnl_range * 0.3).alias("pnl_score"),
    ]).with_columns([
        (pl.col("lead_score") + pl.col("win_rate_score") + pl.col("pnl_score")).alias("total_score")
    ]).sort("total_score", descending=True)

    print("\n篩選條件：月度勝率 >= 50% 且 PNL > 0")
    print()

    top_brokers = scored.filter(
        (pl.col("monthly_win_rate") >= 0.5) & (pl.col("total_pnl") > 0)
    )

    print(f"{'排名':<4} {'券商':<15} {'總分':<8} {'Lead穩定':<10} {'勝率':<8} {'每張報酬':<12} {'總PNL':<15}")
    print("-" * 90)
    for i, row in enumerate(top_brokers.head(20).iter_rows(named=True), 1):
        stable_mark = "✓" if row["lead_stable"] and row["lead_corr"] > 0 else ""
        print(f"{i:<4} {row['broker']:<15} {row['total_score']:.3f}    {stable_mark:<10} {row['monthly_win_rate']*100:>5.1f}%   {row['pnl_per_share']:>+10,.1f}  {row['total_pnl']:>12,.0f}")

    # === Summary Stats ===
    print("\n" + "="*80)
    print("【統計摘要】")
    print("="*80)

    profitable = df.filter(pl.col("total_pnl") > 0)
    losing = df.filter(pl.col("total_pnl") < 0)

    print(f"總分析分點數：{len(df)}")
    print(f"獲利分點數：{len(profitable)} ({len(profitable)/len(df)*100:.1f}%)")
    print(f"虧損分點數：{len(losing)} ({len(losing)/len(df)*100:.1f}%)")
    print(f"Lead 係數穩定（正向）：{len(df.filter((pl.col('lead_stable')) & (pl.col('lead_corr') > 0)))} 個")
    print(f"月度勝率 >= 60%：{len(df.filter(pl.col('monthly_win_rate') >= 0.6))} 個")

    # Save detailed results (convert numpy types to native Python)
    output_path = Path("predictive_analysis_results.json")
    serializable_results = []
    for r in results:
        sr = {}
        for k, v in r.items():
            if hasattr(v, 'item'):  # numpy scalar
                sr[k] = v.item()
            else:
                sr[k] = v
        serializable_results.append(sr)

    with open(output_path, "w") as f:
        json.dump(serializable_results, f, indent=2, ensure_ascii=False)
    print(f"\n詳細結果已儲存至：{output_path}")
