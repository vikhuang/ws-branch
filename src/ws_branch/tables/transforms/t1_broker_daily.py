"""T1 主表轉換:分點×股票×日(canonical 聚合)。

純轉換薄層:三個資料陷阱(單位股/千分位逗號/dash coalesce)已封在
ws-core `broker_tx_daily_scan`,此處只負責**逐日聚合 + 逐月落 part + 串流串接**。

為什麼逐日(2026-09-21 記憶體事故 ④)
------------------------------------
group key 含 date,而 date = 一個 raw 檔——聚合從不跨檔。但把整年 243 檔
scan 成一條 lazy 鏈再 group_by,polars 要為整年 1.5 億 group 維持雜湊表:
實測 1 天 0.15s / 0.54GB,1 月 2.8s / **8.2GB**(不是 23 × 0.5),1 年 20 分鐘
+ 7GB swap。逐日 collect 後結果**恆等**(2026-07 逐列比對 exact equal),月峰值約 2.5GB,
且天然支援之後的每日增量。
"""

from __future__ import annotations

import datetime
import glob
import re
import resource
import shutil
import time
from pathlib import Path

import polars as pl
from ws_core import broker_tx_daily_scan
from ws_core.paths import fugle_dir

from ws_branch.tables import io


def _peak_rss_gb() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 2**30


def parts_dir(year: int) -> Path:
    return io.table_dir("t1_broker_daily") / f"_parts_{year}"


_SHARD = re.compile(r"broker_tx_(\d{4})(\d{2})(\d{2})\.parquet$")


def raw_days(year: int, month: int) -> list[datetime.date]:
    """該月 raw 分片實際存在的日期(分片檔名尾碼 = 台北交易日)。

    以檔案為準、不以交易日曆為準:哪天有資料是 raw 說了算(broker_tx 09-14
    缺席那種情況不會被日曆造出假的空日)。
    """
    out = []
    for f in glob.glob(f"{fugle_dir()}/broker_tx/broker_tx_{year}{month:02d}*.parquet"):
        m = _SHARD.search(f)
        if m:
            out.append(datetime.date(*map(int, m.groups())))
    return sorted(out)


def build_month(year: int, month: int) -> pl.DataFrame:
    """該月每個 raw 日各自聚合後 concat(逐日 = 有界記憶體)。"""
    frames = [broker_tx_daily_scan(start=str(d), end=str(d)).collect()
              for d in raw_days(year, month)]
    frames = [f for f in frames if f.height]
    return pl.concat(frames) if frames else pl.DataFrame()


def build_year(year: int) -> pl.LazyFrame:
    """逐月寫 part,回傳串接所有 part 的 LazyFrame(runner 再 sink 成年檔)。

    part 目錄在年檔 sink 完成後由 runner 刪除;中途失敗會留下 part 供檢視。
    """
    pdir = parts_dir(year)
    shutil.rmtree(pdir, ignore_errors=True)
    pdir.mkdir(parents=True)
    parts: list[Path] = []
    for m in range(1, 13):
        t0 = time.time()
        df = build_month(year, m)
        if df.height == 0:
            continue
        out = pdir / f"{year}-{m:02d}.parquet"
        df.write_parquet(out, compression="zstd")
        parts.append(out)
        print(f"  t1 {year}-{m:02d}: {df.height:,} 列 {time.time() - t0:.1f}s "
              f"峰值 RSS {_peak_rss_gb():.2f} GB", flush=True)
    if not parts:
        raise ValueError(f"t1_broker_daily {year}:raw 無任何交易日資料")
    return pl.concat([pl.scan_parquet(p) for p in parts])
