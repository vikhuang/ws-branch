"""Build 3D tensor from Parquet."""

import json
import sys
from pathlib import Path

import numpy as np
import polars as pl


def build_tensor(input_path: Path) -> None:
    """Convert Parquet to 3D NumPy tensor."""
    df = pl.read_parquet(input_path)

    # Build index mappings
    symbols = df["symbol_id"].unique().sort().to_list()
    dates = df["date"].unique().sort().to_list()
    brokers = df["broker"].unique().sort().to_list()

    symbol_map = {s: i for i, s in enumerate(symbols)}
    date_map = {d: i for i, d in enumerate(dates)}
    broker_map = {b: i for i, b in enumerate(brokers)}

    # Initialize tensor
    tensor = np.zeros(
        (len(symbols), len(dates), len(brokers)), dtype=np.float32
    )

    # Fill tensor
    for row in df.iter_rows(named=True):
        s_idx = symbol_map[row["symbol_id"]]
        d_idx = date_map[row["date"]]
        b_idx = broker_map[row["broker"]]
        tensor[s_idx, d_idx, b_idx] = row["pnl"]

    # Save
    np.save("pnl_tensor.npy", tensor)
    with open("index_maps.json", "w") as f:
        json.dump({"symbols": symbols, "dates": dates, "brokers": brokers}, f)

    print(f"Tensor shape: {tensor.shape}")
    print(f"Memory: {tensor.nbytes / 1e6:.1f} MB")
    print(f"Non-zero: {np.count_nonzero(tensor)} ({100*np.count_nonzero(tensor)/tensor.size:.2f}%)")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tensor.py <input.parquet>")
        sys.exit(1)
    build_tensor(Path(sys.argv[1]))
