"""U2 處置期散戶化程度 → 右臂(家族第八問;強制關卡一次跑齊)。

單一鎖定定義:retailization = 處置窗 [sg,eg] 內 C0+C3 分點 gross share。
outcome/clean/in_band join redteam_f22_ev_weighted.parquet(版控產物)。
關卡:all / clean / first-event-per-stock 去重 × 各自 date-blocked perm,
two-sided,無變體。舊制 = 機制檔案。
"""
from __future__ import annotations

import datetime
from pathlib import Path

import polars as pl

from src.data_layer.calendar import trading_calendar
from scripts.research.dispo_broker_overnight.redteam_f22_events import date_blocked_ic

ROOT = Path(__file__).resolve().parents[3]
CACHE = ROOT / "experiments/dispo_broker_overnight/cache"
NEW_REGIME = datetime.date(2026, 8, 11)


def main() -> None:
    cal = trading_calendar()
    d2g = dict(zip(cal["date"].to_list(), cal["gidx"].to_list()))
    ev = pl.read_parquet(CACHE / "events.parquet")
    clusters = (pl.read_parquet(ROOT / "experiments/broker_taxonomy/broker_clusters.parquet")
                .select("broker_name", "cluster"))
    bd = (pl.scan_parquet(CACHE / "broker_daily.parquet")
          .with_columns((pl.col("buy_dollar_priced") + pl.col("sell_dollar_priced"))
                        .alias("gross"))
          .group_by("symbol_id", "tdate", "broker_name")
          .agg(pl.col("gross").sum())
          .collect()
          .with_columns(pl.col("tdate").replace_strict(d2g, default=None).alias("g"))
          .drop_nulls("g")
          .join(clusters, on="broker_name", how="left")
          .with_columns(pl.col("cluster").is_in([0, 3]).fill_null(False)
                        .alias("is_retail")))
    feats = []
    for r in ev.iter_rows(named=True):
        w = bd.filter((pl.col("symbol_id") == r["stock_id"])
                      & pl.col("g").is_between(r["sg"], r["eg"]))
        tot = w["gross"].sum()
        if not tot:
            continue
        feats.append({"event_id": r["event_id"], "stock_id": r["stock_id"],
                      "retailization": w.filter(pl.col("is_retail"))["gross"].sum() / tot})
    base = (pl.read_parquet(CACHE / "redteam_f22_ev_weighted.parquet")
            .select("event_id", "disposal_end", "in_band", "fwd_overlaps_other", "arm")
            .join(pl.DataFrame(feats), on="event_id", how="inner")
            .filter(pl.col("arm").is_finite()
                    & (pl.col("disposal_end") < NEW_REGIME)))
    dedup = base.sort("disposal_end").unique(subset=["stock_id"], keep="first")
    for name, g in [("all", base),
                    ("clean", base.filter(~pl.col("fwd_overlaps_other"))),
                    ("declustered(first-event-per-stock)", dedup),
                    ("declustered+clean", dedup.filter(~pl.col("fwd_overlaps_other")))]:
        ic, p, nd = date_blocked_ic(g, "retailization", "arm")
        print(f"[{name}] n={g.height} date-blocked({nd}日) IC={ic:+.4f} p={p:.3f}")


if __name__ == "__main__":
    main()
