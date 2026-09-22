"""通用執行器:對 registry 內任何表做 build / verify。

兩條硬規矩(皆為 2026-09-15 實付事故):
1. 序列執行,不並行(OOM,exit 137)
2. **重活一年一程序**:polars 的已用記憶體不還 OS,多年共用單一程序
   會在數分鐘內堆出 90GB 壓縮頁塞爆 swap——多年建表一律逐年 spawn
   子程序,做完即退、記憶體歸還
"""

from __future__ import annotations

import datetime
import json
import subprocess
import sys

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
    if year is None:
        # 多年 = 逐年子程序(規矩 2);單年才在本程序執行
        for y in range(t.first_year, this_year + 1):
            cmd = [sys.executable, "-m", "ws_branch", "build",
                   "--table", name, "--year", str(y)]
            if force:
                cmd.append("--force")
            subprocess.run(cmd, check=True)
        return
    years = [year]
    if t.frozen:
        print(f"[frozen] {t.name} 已退役(Step F):此次建表僅供歷史重現,新消費端請接 v3 表")
    for y in years:
        out = io.year_path(t.name, y)
        # 存在即跳過(含當年)——當年的增量更新之後以明確的 --incr 語意提供,
        # 不做隱性重建(20 分鐘級的意外成本,2026-09-15 實付過)
        if out.exists() and not force:
            print(f"{t.name} {y}: exists, skip(--force 重建)")
            continue
        io.sink_year(t.build_year(y), t.name, y)
        if t.after_sink is not None:   # 表專屬收尾(T1 清逐月 part),runner 不認識任何表的內部
            t.after_sink(y)
        if t.manifest is not None:
            mpath = out.with_suffix(".manifest.json")
            mpath.write_text(json.dumps(t.manifest(y), ensure_ascii=False, indent=2))
            print(f"  manifest → {mpath.name}")
        stat = (pl.scan_parquet(out)
                .select(pl.len().alias("n"),
                        pl.col(t.date_col).n_unique().alias("days"))
                .collect())
        print(f"{t.name} {y}: {stat[0, 'n']:,} rows, {stat[0, 'days']} 交易日 "
              f"→ {out.name}", flush=True)


def verify(name: str, n_samples: int = 60, seed: int = 20260915) -> None:
    _resolve(name).verify(n_samples, seed)
