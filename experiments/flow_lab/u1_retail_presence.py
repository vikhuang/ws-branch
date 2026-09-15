"""U1 散戶在處置期間會不會不見?(user 提問,descriptive)

四階段剖面:baseline [sg-40,sg-21] / run-up [sg-10,sg-1] / 處置 [sg,eg] /
解禁後 [eg+1,eg+5]。每事件每階段:
- 散戶密集份額(兩套正典定義並陳:cluster C0+C3 gross share;
  name-based cohort retail gross share)
- C1 虎尾幫份額(對照)、HQ(無 dash 總公司)份額
- 每日總 gross(絕對量,對 baseline 指數化)
跨事件取中位數,全期 / 新制(disposal_start>=2026-08-18)分列。
"""
from __future__ import annotations

import datetime
from pathlib import Path

import polars as pl

from src.data_layer.broker_loader import classify_broker_cohort
from src.data_layer.calendar import trading_calendar

ROOT = Path(__file__).resolve().parents[3]
CACHE = ROOT / "experiments/dispo_broker_overnight/cache"
PHASES = [("baseline", -40, -21), ("runup", -10, -1)]  # 相對 sg;處置/post 另算


def main() -> None:
    cal = trading_calendar()
    d2g = dict(zip(cal["date"].to_list(), cal["gidx"].to_list()))
    ev = pl.read_parquet(CACHE / "events.parquet").with_columns(
        pl.col("disposal_start").cast(pl.Date))
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
          .join(clusters, on="broker_name", how="left"))
    cohort_map = {n: classify_broker_cohort(n)
                  for n in bd["broker_name"].unique().to_list()}
    bd = bd.with_columns(
        pl.col("broker_name").replace_strict(cohort_map, default="unknown")
        .alias("cohort"),
        pl.col("cluster").is_in([0, 3]).fill_null(False).alias("is_retail_cl"),
        (pl.col("cluster") == 1).fill_null(False).alias("is_c1"),
        (~pl.col("broker_name").str.contains("-")).alias("is_hq"))

    rows = []
    for r in ev.iter_rows(named=True):
        sg, eg = r["sg"], r["eg"]
        windows = ([(n, sg + lo, sg + hi) for n, lo, hi in PHASES]
                   + [("dispo", sg, eg), ("post", eg + 1, eg + 5)])
        ev_rows = {}
        ok = True
        for name, glo, ghi in windows:
            w = bd.filter((pl.col("symbol_id") == r["stock_id"])
                          & pl.col("g").is_between(glo, ghi))
            tot = w["gross"].sum()
            n_days = w["g"].n_unique()
            if not tot or n_days < 3:
                ok = False
                break
            ev_rows[name] = {
                "retail_cl": w.filter(pl.col("is_retail_cl"))["gross"].sum() / tot,
                "retail_co": w.filter(pl.col("cohort") == "retail")["gross"].sum() / tot,
                "c1": w.filter(pl.col("is_c1"))["gross"].sum() / tot,
                "hq": w.filter(pl.col("is_hq"))["gross"].sum() / tot,
                "gross_pd": tot / n_days,
            }
        if not ok:
            continue
        base_pd = ev_rows["baseline"]["gross_pd"]
        for name, v in ev_rows.items():
            rows.append({"event_id": r["event_id"],
                         "new_regime": r["disposal_start"] >= datetime.date(2026, 8, 18),
                         "phase": name, **v,
                         "gross_idx": v["gross_pd"] / base_pd})
    f = pl.DataFrame(rows)
    print(f"事件數(四窗齊全): {f['event_id'].n_unique()}")
    order = {"baseline": 0, "runup": 1, "dispo": 2, "post": 3}

    def report(g: pl.DataFrame, tag: str) -> None:
        out = (g.group_by("phase")
               .agg(pl.col("retail_cl").median().round(3),
                    pl.col("retail_co").median().round(3),
                    pl.col("c1").median().round(4),
                    pl.col("hq").median().round(3),
                    pl.col("gross_idx").median().round(2))
               .with_columns(pl.col("phase").replace_strict(order).alias("o"))
               .sort("o").drop("o"))
        print(f"\n== {tag}(事件中位數) ==")
        print(out)

    report(f.filter(~pl.col("new_regime")), "舊制(2025-01~2026-08-08 起處置)")
    report(f.filter(pl.col("new_regime")), "新制(2026-08-18+ 起處置)")


if __name__ == "__main__":
    main()
