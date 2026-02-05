"""Query Module: Full-year ranking for brokers.

Generates Top 10 rankings for:
- Query A: 一年勝率 (win rate)
- Query B: 一年金額 (total volume)
- Query C: 一年獲利 (total PNL)
"""

import json
import sys
from pathlib import Path

import numpy as np
import polars as pl


def load_data(
    realized_path: Path = Path("realized_pnl.npy"),
    unrealized_path: Path = Path("unrealized_pnl.npy"),
    trade_path: Path = Path("daily_trade_summary.parquet"),
    maps_path: Path = Path("index_maps.json"),
    broker_names_path: Path = Path("broker_names.json"),
) -> tuple[np.ndarray, pl.DataFrame, dict, dict]:
    """Load all required data."""
    realized = np.load(realized_path)
    unrealized = np.load(unrealized_path)
    total_pnl = realized + unrealized

    trade_df = pl.read_parquet(trade_path)

    with open(maps_path) as f:
        maps = json.load(f)

    with open(broker_names_path) as f:
        broker_names = json.load(f)

    return total_pnl, trade_df, maps, broker_names


def calculate_rankings(
    total_pnl: np.ndarray,
    trade_df: pl.DataFrame,
    maps: dict,
    broker_names: dict,
    top_n: int = 10,
    min_active_days: int = 50,
) -> dict[str, pl.DataFrame]:
    """Calculate full-year rankings.

    Args:
        total_pnl: 3D tensor of total PNL (symbols, dates, brokers)
        trade_df: Daily trade summary DataFrame
        maps: Dimension index maps
        broker_names: Broker code to name mapping
        top_n: Number of top brokers to return (default 10)
        min_active_days: Minimum active days for win rate ranking (default 50)

    Returns:
        Dict with 'win_rate', 'volume', 'profit' DataFrames
    """
    dates_list = sorted(maps["dates"].keys())
    start_date = dates_list[0]
    end_date = dates_list[-1]

    print(f"期間: {start_date} ~ {end_date} ({len(dates_list)} 天)")
    print(f"勝率篩選: 至少 {min_active_days} 天活躍")

    # Reverse maps for lookup
    idx_to_broker = {v: k for k, v in maps["brokers"].items()}

    def get_broker_name(code: str) -> str:
        return broker_names.get(code, code)

    results = {}

    # For single symbol (2345), sym_idx = 0
    sym_idx = 0

    # === Query A: 一年勝率 (至少 min_active_days 天) ===
    all_pnl = total_pnl[sym_idx, :, :]  # (all_dates, brokers)
    wins = (all_pnl > 0).sum(axis=0)  # (brokers,)
    active_days = (all_pnl != 0).sum(axis=0)  # (brokers,)

    # Avoid division by zero
    win_rate = np.where(active_days > 0, wins / active_days, 0)

    # Get top N by win rate (only consider brokers with >= min_active_days)
    active_mask = active_days >= min_active_days
    broker_indices = np.arange(len(win_rate))

    # Sort by win rate descending, then by active_days descending for ties
    sorted_indices = sorted(
        [i for i in broker_indices if active_mask[i]],
        key=lambda i: (-win_rate[i], -active_days[i])
    )[:top_n]

    win_rate_data = []
    for rank, broker_idx in enumerate(sorted_indices, 1):
        broker_code = idx_to_broker[broker_idx]
        win_rate_data.append({
            "rank": rank,
            "broker": broker_code,
            "broker_name": get_broker_name(broker_code),
            "win_rate": f"{win_rate[broker_idx] * 100:.1f}%",
            "wins": int(wins[broker_idx]),
            "active_days": int(active_days[broker_idx]),
        })

    results["win_rate"] = pl.DataFrame(win_rate_data)

    # === Query B: 一年金額 ===
    volume_df = (
        trade_df
        .group_by("broker")
        .agg(
            (pl.col("buy_amount") + pl.col("sell_amount")).sum().alias("total_volume")
        )
        .sort("total_volume", descending=True)
        .head(top_n)
        .with_row_index("rank", offset=1)
        .with_columns(
            pl.col("broker").map_elements(get_broker_name, return_dtype=pl.String).alias("broker_name")
        )
        .select(["rank", "broker", "broker_name", "total_volume"])
    )

    results["volume"] = volume_df

    # === Query C: 一年獲利 ===
    total_profit = all_pnl.sum(axis=0)  # Sum over all dates (brokers,)

    sorted_profit_indices = np.argsort(total_profit)[::-1][:top_n]

    profit_data = []
    for rank, broker_idx in enumerate(sorted_profit_indices, 1):
        broker_code = idx_to_broker[broker_idx]
        profit_data.append({
            "rank": rank,
            "broker": broker_code,
            "broker_name": get_broker_name(broker_code),
            "total_profit": float(total_profit[broker_idx]),
        })

    results["profit"] = pl.DataFrame(profit_data)

    return results


def export_to_excel(
    results: dict[str, pl.DataFrame],
    output_path: Path = Path("ranking_report.xlsx"),
) -> None:
    """Export results to Excel with multiple sheets."""
    import xlsxwriter

    workbook = xlsxwriter.Workbook(output_path)

    for sheet_name, df in [
        ("勝率_QueryA", results["win_rate"]),
        ("金額_QueryB", results["volume"]),
        ("獲利_QueryC", results["profit"]),
    ]:
        worksheet = workbook.add_worksheet(sheet_name)

        # Write header
        for col_idx, col_name in enumerate(df.columns):
            worksheet.write(0, col_idx, col_name)

        # Write data
        for row_idx, row in enumerate(df.iter_rows(), 1):
            for col_idx, value in enumerate(row):
                worksheet.write(row_idx, col_idx, value)

    workbook.close()
    print(f"\nExported to {output_path}")


def main():
    """Main function."""
    # Load data
    total_pnl, trade_df, maps, broker_names = load_data()

    # Calculate rankings
    results = calculate_rankings(total_pnl, trade_df, maps, broker_names)

    # Print results
    print("\n=== Query A: 一年勝率 Top 10 ===")
    print(results["win_rate"])

    print("\n=== Query B: 一年金額 Top 10 ===")
    print(results["volume"])

    print("\n=== Query C: 一年獲利 Top 10 ===")
    print(results["profit"])

    # Export to Excel
    export_to_excel(results)


if __name__ == "__main__":
    main()
