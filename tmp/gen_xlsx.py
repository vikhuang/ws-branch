"""Generate rolling PNL ranking xlsx for 5 stocks, both 3yr and 5d windows.

- 2023-01 ~ 2025-12: monthly snapshots (last trading day of each month)
- 2026-01 ~ now: daily snapshots (every trading day)
- Uses merged pnl_daily data
"""

import json
from datetime import date
from pathlib import Path

import polars as pl

STOCKS = {
    "2308": "台達電",
    "2313": "華通",
    "2345": "智邦",
    "2360": "致茂",
    "2383": "台光電",
    "3017": "奇鋐",
    "6285": "啟碁",
    "8046": "南電",
    "8358": "金居",
}

WINDOWS = {
    "3y": 3 * 252,  # ~3 years of trading days
    "5d": 5,
}

DATA_DIR = Path("data/pnl_daily_merged")
OUTPUT_ROOT = Path("tmp")


def load_broker_names() -> dict[str, str]:
    with open("broker_names.json") as f:
        return json.load(f)


def get_snapshot_dates(all_dates: list[date]) -> list[date]:
    """Get snapshot dates: monthly for 2023-2025, daily for 2026."""
    snapshots = []

    # Monthly: last trading day of each month, 2023-01 ~ 2025-12
    monthly_dates = [d for d in all_dates if date(2023, 1, 1) <= d <= date(2025, 12, 31)]
    by_month: dict[tuple[int, int], date] = {}
    for d in monthly_dates:
        key = (d.year, d.month)
        if key not in by_month or d > by_month[key]:
            by_month[key] = d
    snapshots.extend(sorted(by_month.values()))

    # Daily: every trading day in 2026
    daily_dates = [d for d in all_dates if d.year == 2026]
    snapshots.extend(sorted(daily_dates))

    return snapshots


def compute_rolling_ranking(
    df: pl.DataFrame,
    all_dates: list[date],
    snapshot_date: date,
    window_days: int,
    broker_names: dict[str, str],
) -> pl.DataFrame:
    """Compute rolling PNL ranking at a specific date with given window."""
    idx = None
    for i, d in enumerate(all_dates):
        if d <= snapshot_date:
            idx = i
    if idx is None:
        return pl.DataFrame()

    start_idx = max(0, idx - window_days + 1)
    window_dates = all_dates[start_idx : idx + 1]

    # Baseline unrealized (day before window)
    if start_idx > 0:
        baseline_date = all_dates[start_idx - 1]
        baseline_df = (
            df.filter(pl.col("date") == baseline_date)
            .select(["broker", pl.col("unrealized_pnl").alias("_base")])
        )
    else:
        baseline_df = pl.DataFrame(schema={"broker": pl.Utf8, "_base": pl.Float64})

    window_df = df.filter(pl.col("date").is_in(window_dates))
    if len(window_df) == 0:
        return pl.DataFrame()

    agg = (
        window_df.sort("date")
        .group_by("broker")
        .agg([
            pl.col("realized_pnl").sum(),
            pl.col("unrealized_pnl").last(),
        ])
        .join(baseline_df, on="broker", how="left")
        .with_columns(pl.col("_base").fill_null(0.0))
        .with_columns(
            (pl.col("unrealized_pnl") - pl.col("_base")).alias("unrealized_pnl")
        )
        .drop("_base")
        .with_columns(
            (pl.col("realized_pnl") + pl.col("unrealized_pnl")).alias("total_pnl")
        )
        .sort("total_pnl", descending=True)
        .with_row_index("rank", offset=1)
    )

    agg = agg.with_columns([
        pl.col("broker")
        .map_elements(lambda b: broker_names.get(b, ""), return_dtype=pl.Utf8)
        .alias("名稱"),
        (pl.col("total_pnl") / 1e8).alias("總PNL(億)"),
        (pl.col("realized_pnl") / 1e8).alias("已實現(億)"),
        (pl.col("unrealized_pnl") / 1e8).alias("未實現(億)"),
    ]).select([
        pl.col("rank").alias("排名"),
        pl.col("broker").alias("券商"),
        "名稱",
        "總PNL(億)",
        "已實現(億)",
        "未實現(億)",
    ])

    return agg


def process_stock(symbol: str, stock_name: str, broker_names: dict[str, str]):
    path = DATA_DIR / f"{symbol}.parquet"
    if not path.exists():
        print(f"  {symbol}: pnl_daily not found, skipping")
        return

    df = pl.read_parquet(path)
    all_dates = sorted(df["date"].unique().to_list())

    snapshot_dates = get_snapshot_dates(all_dates)
    if not snapshot_dates:
        print(f"  {symbol}: no snapshot dates")
        return

    for window_name, window_days in WINDOWS.items():
        out_dir = OUTPUT_ROOT / f"{symbol}_{window_name}_windows"
        out_dir.mkdir(parents=True, exist_ok=True)

        count = 0
        for snap_date in snapshot_dates:
            ranking = compute_rolling_ranking(df, all_dates, snap_date, window_days, broker_names)
            if len(ranking) == 0:
                continue

            out_path = out_dir / f"{symbol}_rolling_{snap_date.isoformat()}.xlsx"
            ranking.write_excel(
                out_path,
                worksheet=f"{stock_name}{symbol}",
            )
            count += 1

        print(f"  {symbol} ({stock_name}) {window_name}: {count} files → {out_dir}")


def main():
    broker_names = load_broker_names()
    print(f"Broker names: {len(broker_names)} entries")
    print()

    for symbol, name in STOCKS.items():
        process_stock(symbol, name, broker_names)


if __name__ == "__main__":
    main()
