"""通用執行器:對 registry 內任何表做 build / verify。序列執行,不並行。"""

from __future__ import annotations

import datetime

import polars as pl

from ws_branch.tables import io
from ws_branch.tables.registry import TABLES, Table


def _resolve(name: str) -> Table:
    if name not in TABLES:
        raise KeyError(f"unknown table {name!r}; available: {sorted(TABLES)}")
    return TABLES[name]


def build(name: str, year: int | None = None, force: bool = False) -> None:
    t = _resolve(name)
    this_year = datetime.date.today().year
    years = [year] if year else list(range(t.first_year, this_year + 1))
    for y in years:
        out = io.year_path(t.name, y)
        # 存在即跳過(含當年)——當年的增量更新之後以明確的 --incr 語意提供,
        # 不做隱性重建(20 分鐘級的意外成本,2026-09-15 實付過)
        if out.exists() and not force:
            print(f"{t.name} {y}: exists, skip(--force 重建)")
            continue
        io.sink_year(t.build_year(y), t.name, y)
        stat = (pl.scan_parquet(out)
                .select(pl.len().alias("n"),
                        pl.col(t.date_col).n_unique().alias("days"))
                .collect())
        print(f"{t.name} {y}: {stat[0, 'n']:,} rows, {stat[0, 'days']} 交易日 "
              f"→ {out.name}", flush=True)


def verify(name: str, n_samples: int = 60, seed: int = 20260915) -> None:
    _resolve(name).verify(n_samples, seed)
