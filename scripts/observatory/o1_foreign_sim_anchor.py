"""O1 驗證錨(docs/OBSERVATORY_2026-09.md §4 L-A):外資名字席位的
foreign_sim 分佈是否顯著高於母體——重現 U3(classify_broker_cohort
"institutional" vs 官方 qfii_ex Pearson +0.973)。

一次性分析腳本,非套件模組:讀已建好的 t4_broker_features,不寫回任何表。
"""

from __future__ import annotations

import math

import polars as pl

from ws_branch.measure.identity import classify_broker_cohort
from ws_branch.tables import io


def _welch_t(a: pl.Series, b: pl.Series) -> tuple[float, float]:
    """Welch t-test 統計量 + 近似 p 值(常態近似,樣本數大時足夠;無 scipy 依賴)。"""
    n1, n2 = a.len(), b.len()
    m1, m2 = a.mean(), b.mean()
    v1, v2 = a.var(ddof=1), b.var(ddof=1)
    se = math.sqrt(v1 / n1 + v2 / n2)
    t = (m1 - m2) / se
    p = math.erfc(abs(t) / math.sqrt(2))  # 兩尾,大樣本用常態近似 t 分布
    return t, p


def _cohens_d(a: pl.Series, b: pl.Series) -> float:
    n1, n2 = a.len(), b.len()
    v1, v2 = a.var(ddof=1), b.var(ddof=1)
    pooled = math.sqrt(((n1 - 1) * v1 + (n2 - 1) * v2) / (n1 + n2 - 2))
    return (a.mean() - b.mean()) / pooled


def run(year: int = 2026) -> None:
    t4 = io.scan("t4_broker_features", start=f"{year}-01-01", end=f"{year}-12-31").collect()

    names = t4.select("broker_name").unique()
    cohort_map = names.with_columns(
        pl.col("broker_name")
        .map_elements(classify_broker_cohort, return_dtype=pl.String)
        .alias("cohort")
    )
    tagged = t4.join(cohort_map, on="broker_name", how="left")

    for col in ["foreign_sim_buy", "foreign_sim_sell", "fund_sim_buy", "fund_sim_sell"]:
        non_null = tagged.filter(pl.col(col).is_not_null())
        pop = non_null[col]
        foreign = non_null.filter(pl.col("cohort") == "institutional")[col]
        other = non_null.filter(pl.col("cohort") != "institutional")[col]
        print(f"\n=== {col} ===")
        print(f"  母體(全部,n={pop.len():,}):mean={pop.mean():.4f} median={pop.median():.4f}")
        print(f"  外資名字席位(institutional,n={foreign.len():,}):"
              f"mean={foreign.mean():.4f} median={foreign.median():.4f}")
        print(f"  非外資席位(n={other.len():,}):"
              f"mean={other.mean():.4f} median={other.median():.4f}")
        if foreign.len() >= 2 and other.len() >= 2:
            diff = foreign.mean() - other.mean()
            t, p = _welch_t(foreign, other)
            d = _cohens_d(foreign, other)
            print(f"  差(外資 − 非外資)= {diff:+.4f}  Welch t={t:.2f} p={p:.2e} "
                  f"Cohen's d={d:.3f}")

    cohort_counts = tagged.group_by("cohort").agg(pl.len().alias("broker_days"))
    print("\n=== cohort 分布(分點日計數)===")
    print(cohort_counts.sort("broker_days", descending=True))

    support = tagged.filter(pl.col("foreign_sim_buy").is_not_null())
    print(f"\n達 _MIN_SUPPORT(≥5 檔購物籃)才有值的分點日:"
          f"{support.height:,} / {tagged.height:,} "
          f"({support.height / tagged.height:.1%})")


if __name__ == "__main__":
    run()
