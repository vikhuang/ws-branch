"""Query Module: Full-year ranking for brokers.

Generates Top 10 rankings for:
- Query A: 一年金額 (total volume)
- Query B: 一年獲利 (total PNL = cumulative realized + final unrealized)
"""

import json
from pathlib import Path

import numpy as np
import polars as pl


def load_data(
    realized_path: Path = Path("realized_pnl.npy"),
    unrealized_path: Path = Path("unrealized_pnl.npy"),
    trade_path: Path = Path("daily_trade_summary.parquet"),
    maps_path: Path = Path("index_maps.json"),
    broker_names_path: Path = Path("broker_names.json"),
) -> tuple[np.ndarray, np.ndarray, pl.DataFrame, dict, dict]:
    """Load all required data."""
    realized = np.load(realized_path)
    unrealized = np.load(unrealized_path)

    trade_df = pl.read_parquet(trade_path)

    with open(maps_path) as f:
        maps = json.load(f)

    with open(broker_names_path) as f:
        broker_names = json.load(f)

    return realized, unrealized, trade_df, maps, broker_names


def calculate_rankings(
    realized: np.ndarray,
    unrealized: np.ndarray,
    trade_df: pl.DataFrame,
    maps: dict,
    broker_names: dict,
    top_n: int = 10,
) -> dict[str, pl.DataFrame]:
    """Calculate full-year rankings.

    Args:
        realized: 3D tensor of realized PNL (symbols, dates, brokers)
        unrealized: 3D tensor of unrealized PNL (symbols, dates, brokers)
        trade_df: Daily trade summary DataFrame
        maps: Dimension index maps
        broker_names: Broker code to name mapping
        top_n: Number of top brokers to return (default 10)

    Returns:
        Dict with 'volume', 'profit' DataFrames
    """
    dates_list = sorted(maps["dates"].keys())
    start_date = dates_list[0]
    end_date = dates_list[-1]

    print(f"期間: {start_date} ~ {end_date} ({len(dates_list)} 天)")

    # Reverse maps for lookup
    idx_to_broker = {v: k for k, v in maps["brokers"].items()}

    def get_broker_name(code: str) -> str:
        return broker_names.get(code, code)

    results = {}

    # For single symbol (2345), sym_idx = 0
    sym_idx = 0

    # === Query A: 一年金額 ===
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

    # === Query B: 一年獲利 ===
    # Correct calculation: cumulative realized PNL + final day unrealized PNL
    realized_cumsum = realized[sym_idx, :, :].sum(axis=0)  # Sum over all dates (brokers,)
    unrealized_final = unrealized[sym_idx, -1, :]  # Last day only (brokers,)
    total_profit = realized_cumsum + unrealized_final

    sorted_profit_indices = np.argsort(total_profit)[::-1][:top_n]

    profit_data = []
    for rank, broker_idx in enumerate(sorted_profit_indices, 1):
        broker_code = idx_to_broker[broker_idx]
        profit_data.append({
            "rank": rank,
            "broker": broker_code,
            "broker_name": get_broker_name(broker_code),
            "realized_pnl": float(realized_cumsum[broker_idx]),
            "unrealized_pnl": float(unrealized_final[broker_idx]),
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
        ("金額_QueryA", results["volume"]),
        ("獲利_QueryB", results["profit"]),
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
    realized, unrealized, trade_df, maps, broker_names = load_data()

    # Calculate rankings
    results = calculate_rankings(realized, unrealized, trade_df, maps, broker_names)

    # Print results
    print("\n=== Query A: 一年金額 Top 10 ===")
    print(results["volume"])

    print("\n=== Query B: 一年獲利 Top 10 ===")
    print(results["profit"])

    # Export to Excel
    export_to_excel(results)


if __name__ == "__main__":
    main()
