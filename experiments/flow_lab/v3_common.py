"""v3 研究腳本共用:逐年快取的 primitives 與 cosines(2025 納入後多年並用)。

快取一律按年分檔(/tmp/v3_phase1_primitives_{year}.parquet 等),多年 = 逐年
讀後 concat;缺哪年算哪年。早於分年制的 2026 快取(無年份後綴)自動沿用。
"""

from __future__ import annotations

import shutil
from pathlib import Path

import polars as pl

PRIM = "/tmp/v3_phase1_primitives"
COS = "/tmp/v3_phase4_cosines"


def _path(prefix: str, year: int) -> Path:
    p = Path(f"{prefix}_{year}.parquet")
    legacy = Path(f"{prefix}.parquet")
    if not p.exists() and year == 2026 and legacy.exists():
        shutil.copy(legacy, p)
    return p


def primitives(years: list[int]) -> pl.DataFrame:
    """Phase 1 gated primitives(含 log_gross / log_n),多年 concat。"""
    parts = []
    for y in years:
        p = _path(PRIM, y)
        if not p.exists():
            from v3_phase1_geometry import _gated_primitives
            df = _gated_primitives(y).with_columns(
                pl.col("gross_amt").log10().alias("log_gross"),
                pl.col("n_symbols").log10().alias("log_n"))
            df.write_parquet(p)
        parts.append(pl.read_parquet(p))
    return pl.concat(parts).sort("broker", "date")


def cosines(years: list[int]) -> pl.DataFrame:
    """Phase 4 席位日 × 官方桶 cosine,多年 concat。"""
    parts = []
    for y in years:
        p = _path(COS, y)
        if not p.exists():
            from v3_phase4_calibration import build_cosines
            build_cosines(y).write_parquet(p)
        parts.append(pl.read_parquet(p))
    return pl.concat(parts).sort("broker", "date")


def years_arg(argv: list[str], default: str = "2026") -> list[int]:
    """`python x.py 2025,2026` → [2025, 2026]。"""
    return [int(y) for y in (argv[1] if len(argv) > 1 else default).split(",")]
