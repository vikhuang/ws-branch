"""Generate complete broker ranking report with all metrics.

Output: ranking_report.csv with all brokers and their complete evaluation.
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

    # Load broker names: merge broker_names.json + 證券商基本資料.xls
    import xlrd

    # Start with broker_names.json
    with open("broker_names.json") as f:
        broker_names = json.load(f)

    # Override/add from 證券商基本資料.xls (official source)
    try:
        wb = xlrd.open_workbook("證券商基本資料.xls")
        sheet = wb.sheet_by_index(0)
        for r in range(1, sheet.nrows):
            code = str(sheet.cell_value(r, 0)).strip()
            name = str(sheet.cell_value(r, 1)).strip()
            if code and name:
                broker_names[code] = name
    except Exception as e:
        print(f"警告：無法載入證券商基本資料.xls: {e}")

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
        return None

    mean_x = sum(x) / n
    mean_y = sum(y) / n

    numerator = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
    denom_x = math.sqrt(sum((xi - mean_x)**2 for xi in x))
    denom_y = math.sqrt(sum((yi - mean_y)**2 for yi in y))

    if denom_x == 0 or denom_y == 0:
        return None

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


def permutation_test(net_buys: list[int], daily_returns: list[float], n_perm: int = 200) -> float:
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

    broker_idx = maps["brokers"].get(broker)
    if broker_idx is None:
        return None

    broker_trades = trade_df.filter(pl.col("broker") == broker)

    # === Basic Stats ===
    trading_days = len(broker_trades)
    total_buy = broker_trades["buy_shares"].sum() or 0
    total_sell = broker_trades["sell_shares"].sum() or 0
    total_volume = total_buy + total_sell
    buy_amount = broker_trades["buy_amount"].sum() or 0
    sell_amount = broker_trades["sell_amount"].sum() or 0
    total_amount = buy_amount + sell_amount

    # === Direction ===
    cumulative_net = total_buy - total_sell
    direction = "做多" if cumulative_net > 0 else "做空" if cumulative_net < 0 else "中性"

    # === PNL ===
    sym_idx = 0
    total_realized = float(realized[sym_idx, :, broker_idx].sum())
    final_unrealized = float(unrealized[sym_idx, -1, broker_idx])
    total_pnl = total_realized + final_unrealized

    # === Execution Alpha ===
    broker_closed = closed_trades.filter(pl.col("broker") == broker)
    exec_alpha = None
    trade_count = 0

    if len(broker_closed) > 0:
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
            exec_alpha = alpha_dollars / total_trade_value if total_trade_value > 0 else None
            trade_count = len(valid_closed)

    # === Timing Alpha & Lead/Lag ===
    timing_alpha = None
    lead_corr = None
    lag_corr = None
    p_value = None

    if trading_days >= 20:
        net_buys_raw = {}
        for row in broker_trades.iter_rows(named=True):
            net_buys_raw[row["date"]] = (row["buy_shares"] or 0) - (row["sell_shares"] or 0)

        valid_dates = [d for d in all_dates if d in returns]
        net_buys = [net_buys_raw.get(d, 0) for d in valid_dates]
        daily_returns = [returns[d] for d in valid_dates]

        timing_alpha = calculate_timing_alpha(net_buys, daily_returns)

        # Lead correlation
        lead_x = net_buys[:-1]
        lead_y = daily_returns[1:]
        lead_corr = pearson_correlation(lead_x, lead_y)

        # Lag correlation
        lag_x = daily_returns[:-1]
        lag_y = net_buys[1:]
        lag_corr = pearson_correlation(lag_x, lag_y)

        # Permutation test
        p_value = permutation_test(net_buys, daily_returns, n_perm=200)

    # Style
    if lag_corr is not None:
        if lag_corr > 0.05:
            style = "順勢"
        elif lag_corr < -0.05:
            style = "逆勢"
        else:
            style = "中性"
    else:
        style = None

    # Significance
    if p_value is not None and timing_alpha is not None:
        if p_value < 0.05 and timing_alpha > 0:
            timing_sig = "顯著正向"
        elif p_value < 0.05 and timing_alpha < 0:
            timing_sig = "顯著負向"
        else:
            timing_sig = "不顯著"
    else:
        timing_sig = None

    return {
        "broker": broker,
        "trading_days": trading_days,
        "total_buy_shares": total_buy,
        "total_sell_shares": total_sell,
        "total_volume": total_volume,
        "buy_amount": buy_amount,
        "sell_amount": sell_amount,
        "total_amount": total_amount,
        "cumulative_net": cumulative_net,
        "direction": direction,
        "realized_pnl": total_realized,
        "unrealized_pnl": final_unrealized,
        "total_pnl": total_pnl,
        "exec_alpha": exec_alpha,
        "trade_count": trade_count,
        "timing_alpha": timing_alpha,
        "lead_corr": lead_corr,
        "lag_corr": lag_corr,
        "style": style,
        "p_value": p_value,
        "timing_significance": timing_sig,
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

    # Create DataFrame
    df = pl.DataFrame(results)

    # Sort by total_pnl descending
    df = df.sort("total_pnl", descending=True)

    # Add rank
    df = df.with_row_index("rank", offset=1)

    # Reorder columns
    df = df.select([
        "rank",
        "broker",
        "name",
        "direction",
        "total_pnl",
        "realized_pnl",
        "unrealized_pnl",
        "exec_alpha",
        "timing_alpha",
        "p_value",
        "timing_significance",
        "lead_corr",
        "lag_corr",
        "style",
        "trading_days",
        "total_volume",
        "total_amount",
        "cumulative_net",
        "trade_count",
    ])

    # Save to CSV
    output_path = Path("ranking_report.csv")
    df.write_csv(output_path)
    print(f"\n已輸出：{output_path}")
    print(f"總筆數：{len(df)}")

    # Also save to parquet for faster loading
    df.write_parquet("ranking_report.parquet")
    print(f"已輸出：ranking_report.parquet")

    # Save to Excel with two sheets using xlsxwriter
    import xlsxwriter

    workbook = xlsxwriter.Workbook("ranking_report.xlsx")

    # Sheet 1: Simplified (with proper unit labels)
    ws1 = workbook.add_worksheet("摘要")
    simple_cols = ["rank", "broker", "name", "total_pnl", "realized_pnl", "unrealized_pnl"]
    simple_headers = ["排名", "券商代碼", "券商名稱", "總PNL(元)", "已實現PNL(元)", "未實現PNL(元)"]
    for col_idx, header in enumerate(simple_headers):
        ws1.write(0, col_idx, header)
    for row_idx, row in enumerate(df.select(simple_cols).iter_rows(), 1):
        for col_idx, val in enumerate(row):
            ws1.write(row_idx, col_idx, val)

    # Sheet 2: Full data (with proper unit labels)
    ws2 = workbook.add_worksheet("完整資料")
    full_headers = [
        "排名", "券商代碼", "券商名稱", "方向", "總PNL(元)", "已實現PNL(元)", "未實現PNL(元)",
        "執行Alpha", "擇時Alpha", "p值", "擇時顯著性", "Lead相關", "Lag相關", "風格",
        "交易天數", "總交易量(股)", "總交易額(元)", "累計淨買(股)", "平倉筆數"
    ]
    for col_idx, header in enumerate(full_headers):
        ws2.write(0, col_idx, header)
    for row_idx, row in enumerate(df.iter_rows(), 1):
        for col_idx, val in enumerate(row):
            ws2.write(row_idx, col_idx, val)

    workbook.close()
    print(f"已輸出：ranking_report.xlsx（含兩個分頁）")

    # Print summary
    print(f"\n{'='*100}")
    print("【排名報告摘要】")
    print("="*100)

    print(f"\n【欄位說明】")
    print("""
rank                排名（依 total_pnl 排序）
broker              券商代碼
name                券商名稱
direction           方向（做多/做空/中性）
total_pnl           總損益 = 已實現 + 未實現（元）
realized_pnl        已實現損益（已平倉）（元）
unrealized_pnl      未實現損益（未平倉部位）（元）
exec_alpha          執行 Alpha（成交價 vs 收盤價）
timing_alpha        擇時 Alpha（進出時機貢獻）
p_value             統計顯著性（< 0.05 為顯著）
timing_significance 擇時顯著性判定
lead_corr           Lead 相關（預測隔日能力）
lag_corr            Lag 相關（順勢/逆勢程度）
style               交易風格
trading_days        交易天數
total_volume        總交易量（股）
total_amount        總交易額（元）
cumulative_net      累計淨買（股）
trade_count         平倉交易筆數
""")

    # Top 10 and Bottom 10
    print(f"\n【PNL Top 10】")
    print(f"{'排名':<6} {'券商':<10} {'名稱':<14} {'方向':<6} {'已實現':>14} {'未實現':>14} {'總PNL':>14}")
    print("-" * 90)
    for row in df.head(10).iter_rows(named=True):
        print(f"{row['rank']:<6} {row['broker']:<10} {row['name']:<14} {row['direction']:<6} "
              f"{row['realized_pnl']/1e8:>+13.2f}億 {row['unrealized_pnl']/1e8:>+13.2f}億 "
              f"{row['total_pnl']/1e8:>+13.2f}億")

    print(f"\n【PNL Bottom 10】")
    print(f"{'排名':<6} {'券商':<10} {'名稱':<14} {'方向':<6} {'已實現':>14} {'未實現':>14} {'總PNL':>14}")
    print("-" * 90)
    for row in df.tail(10).reverse().iter_rows(named=True):
        print(f"{row['rank']:<6} {row['broker']:<10} {row['name']:<14} {row['direction']:<6} "
              f"{row['realized_pnl']/1e8:>+13.2f}億 {row['unrealized_pnl']/1e8:>+13.2f}億 "
              f"{row['total_pnl']/1e8:>+13.2f}億")

    # Stats
    total_market_pnl = df["total_pnl"].sum()
    total_realized = df["realized_pnl"].sum()
    total_unrealized = df["unrealized_pnl"].sum()

    print(f"\n【市場總計】")
    print(f"  已實現 PNL 總和：{total_realized/1e8:+,.2f} 億")
    print(f"  未實現 PNL 總和：{total_unrealized/1e8:+,.2f} 億")
    print(f"  總 PNL 總和：{total_market_pnl/1e8:+,.2f} 億")


if __name__ == "__main__":
    random.seed(42)
    main()
