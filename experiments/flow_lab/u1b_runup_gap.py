"""U1b run-up 份額缺口分解:baseline→run-up 散戶份額 -2.7pp 被誰吃走?

同 u1 四階段框架,份額欄位擴為完整分割:C0/C1/C2/C3/未分群(cluster null),
以及 HQ 內部(外資 HQ 含香港上海匯豐 / 本土 HQ)。全期(舊制)事件中位數。
"""
from __future__ import annotations

from pathlib import Path

import polars as pl

from src.data_layer.calendar import trading_calendar

ROOT = Path(__file__).resolve().parents[3]
CACHE = ROOT / "experiments/dispo_broker_overnight/cache"
FOREIGN_HQ = ["台灣摩根士丹利", "美商高盛", "摩根大通", "香港上海匯豐", "花旗環球",
              "美林", "瑞銀", "台灣匯立", "法銀巴黎", "港商法國興業", "港商野村",
              "港商麥格理", "瑞士信貸", "德意志", "大和國泰"]
PHASES = [("baseline", -40, -21), ("runup", -10, -1)]


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
          .with_columns(
              (~pl.col("broker_name").str.contains("-")).alias("is_hq"),
              pl.col("broker_name").str.contains_any(FOREIGN_HQ).alias("is_foreign")))

    rows = []
    for r in ev.iter_rows(named=True):
        sg, eg = r["sg"], r["eg"]
        windows = ([(n, sg + lo, sg + hi) for n, lo, hi in PHASES]
                   + [("dispo", sg, eg), ("post", eg + 1, eg + 5)])
        for name, glo, ghi in windows:
            w = bd.filter((pl.col("symbol_id") == r["stock_id"])
                          & pl.col("g").is_between(glo, ghi))
            tot = w["gross"].sum()
            if not tot or w["g"].n_unique() < 3:
                continue
            share = {f"c{k}": w.filter(pl.col("cluster") == k)["gross"].sum() / tot
                     for k in range(4)}
            rows.append({
                "event_id": r["event_id"], "phase": name, **share,
                "unmatched": w.filter(pl.col("cluster").is_null())["gross"].sum() / tot,
                "hq_foreign": w.filter(pl.col("is_hq") & pl.col("is_foreign"))["gross"]
                .sum() / tot,
                "hq_domestic": w.filter(pl.col("is_hq") & ~pl.col("is_foreign"))["gross"]
                .sum() / tot,
            })
    f = pl.DataFrame(rows)
    order = {"baseline": 0, "runup": 1, "dispo": 2, "post": 3}
    cols = ["c0", "c1", "c2", "c3", "unmatched", "hq_foreign", "hq_domestic"]
    print(f"事件×階段列數: {f.height}")
    print(f.group_by("phase")
          .agg([pl.col(c).median().round(4) for c in cols])
          .with_columns(pl.col("phase").replace_strict(order).alias("o"))
          .sort("o").drop("o"))


if __name__ == "__main__":
    main()
