"""U3 分點 cohort 對帳官方三大法人(tej_shareholding)。

股票日層級,universe = broker_daily cache(462 檔處置股,2024-10~2026-09-10):
- 外資: cohort=institutional 席位淨股數 vs qfii_ex(官方外資買賣超,股)
- 自營: cohort=prop 席位 vs dlrp_ex+dlrh_ex
- 投信: fund_ex 與各 cohort 淨額的 corr(找有無名字 proxy)
報 Pearson/Spearman corr、gross 覆蓋率(席位買進/官方買進)分佈。
單位:broker_daily=股,tej buy/sell=千股(TEJ 慣例,先驗證再定)。
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl

from src.data_layer.broker_loader import classify_broker_cohort

ROOT = Path(__file__).resolve().parents[3]
CACHE = ROOT / "experiments/dispo_broker_overnight/cache"
SH = Path("~/r20/data/tej/shareholding.parquet").expanduser()


def rank(a: np.ndarray) -> np.ndarray:
    return np.argsort(np.argsort(a)).astype(float)


def main() -> None:
    bd = pl.scan_parquet(CACHE / "broker_daily.parquet").collect()
    cohort_map = {n: classify_broker_cohort(n)
                  for n in bd["broker_name"].unique().to_list()}
    per = (bd.with_columns(
        pl.col("broker_name").replace_strict(cohort_map, default="unknown")
        .alias("cohort"),
        (pl.col("buy_sh") - pl.col("sell_sh")).alias("net_sh"))
        .group_by("symbol_id", "tdate", "cohort")
        .agg(pl.col("net_sh").sum(), pl.col("buy_sh").sum(), pl.col("sell_sh").sum())
        .pivot(values=["net_sh", "buy_sh"], index=["symbol_id", "tdate"], on="cohort"))
    sh = (pl.scan_parquet(SH)
          .filter(pl.col("coid").is_in(set(bd["symbol_id"].unique().to_list()))
                  & (pl.col("mdate") >= pl.date(2024, 10, 1)))
          .select("coid", "mdate", "qfii_buy", "qfii_sell", "qfii_ex",
                  "fund_ex", "dlrp_ex", "dlrh_ex")
          .collect()
          .rename({"coid": "symbol_id", "mdate": "tdate"}))
    j = per.join(sh, on=["symbol_id", "tdate"], how="inner")
    print(f"join 股票日: {j.height}")

    # 單位偵察:TEJ qfii 欄位可能是千股——用中位數比值定標
    m = j.filter((pl.col("qfii_buy") > 0) & (pl.col("buy_sh_institutional") > 0))
    ratio = (m["buy_sh_institutional"] / m["qfii_buy"]).median()
    scale = 1000 if 500 < ratio < 2000 else 1
    print(f"外資席位買進/qfii_buy 中位比值 = {ratio:.1f} → TEJ 單位判定 = "
          f"{'千股' if scale == 1000 else '股'}")

    for label, seat_col, off_expr in [
        ("外資: institutional 席位 vs qfii_ex", "net_sh_institutional",
         pl.col("qfii_ex") * scale),
        ("自營: prop 席位 vs dlrp_ex+dlrh_ex", "net_sh_prop",
         (pl.col("dlrp_ex") + pl.col("dlrh_ex")) * scale),
    ]:
        g = (j.with_columns(off_expr.alias("official"))
             .select(pl.col(seat_col).fill_null(0).alias("seat"), "official")
             .drop_nulls())
        x, y = g["seat"].to_numpy(), g["official"].to_numpy()
        act = (np.abs(x) > 0) | (np.abs(y) > 0)
        x, y = x[act], y[act]
        pe = float(np.corrcoef(x, y)[0, 1])
        sp = float(np.corrcoef(rank(x), rank(y))[0, 1])
        cov = np.median(np.abs(x[np.abs(y) > 1000]) / np.abs(y[np.abs(y) > 1000]))
        print(f"{label}: n={len(x)} Pearson={pe:+.3f} Spearman={sp:+.3f} "
              f"|席位/官方| 中位={cov:.2f}")

    print("\n投信 fund_ex 與各 cohort 淨額 corr(找名字 proxy):")
    for c in ["institutional", "prop", "retail", "mixed"]:
        col = f"net_sh_{c}"
        if col not in j.columns:
            continue
        g = (j.with_columns((pl.col("fund_ex") * scale).alias("official"))
             .select(pl.col(col).fill_null(0).alias("seat"), "official").drop_nulls())
        act = g.filter(pl.col("official").abs() > 0)
        pe = float(np.corrcoef(act["seat"].to_numpy(), act["official"].to_numpy())[0, 1])
        print(f"  {c:14s}: Pearson={pe:+.3f} (n={act.height})")


if __name__ == "__main__":
    main()
