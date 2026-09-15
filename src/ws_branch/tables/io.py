"""IO 殼:全 repo 唯一知道「表的檔案在哪」的地方。

效能規約(條文化,違者 review 打回):
- lazy 進、sink 出;禁止全史 collect
- 按年分割(year=YYYY.parquet, zstd);增量 = 只重建當年分割
- 重活(建表/大 join)序列執行,不並行——2026-09-15 OOM 事故教訓
"""

from __future__ import annotations

import datetime
from pathlib import Path

import polars as pl

REPO = Path(__file__).resolve().parents[3]
DATA_DIR = REPO / "data"


def table_dir(name: str) -> Path:
    return DATA_DIR / name


def year_path(name: str, year: int) -> Path:
    return table_dir(name) / f"year={year}.parquet"


def existing_years(name: str) -> list[int]:
    d = table_dir(name)
    if not d.exists():
        return []
    return sorted(int(p.stem.split("=")[1]) for p in d.glob("year=*.parquet"))


def sink_year(lf: pl.LazyFrame, name: str, year: int) -> Path:
    out = year_path(name, year)
    out.parent.mkdir(parents=True, exist_ok=True)
    lf.sink_parquet(out, compression="zstd")
    return out


def scan(name: str, *, start: str | None = None, end: str | None = None,
         date_col: str = "date") -> pl.LazyFrame:
    """讀表的唯一入口(含研究區)。start/end 為 ISO 日期字串。"""
    lf = pl.scan_parquet(table_dir(name) / "*.parquet")
    if start:
        lf = lf.filter(pl.col(date_col) >= datetime.date.fromisoformat(start))
    if end:
        lf = lf.filter(pl.col(date_col) <= datetime.date.fromisoformat(end))
    return lf
