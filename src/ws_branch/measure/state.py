"""席位 trait / 共同日狀態 / 席位特有狀態的**讀時**計算(純函數,零 IO)。

架構文件 §6 的三段:X = trait(α_b)+ 共同日效應(γ_t)+ 席位特有異常(ε)。
Phase 2 用交替投影在全樣本上**聯合估計**——那是 retrospective 描述,不能進
as-of 產品(§4.5:baseline 只能用當時可得資料)。本模組給讀本用的是三個
**因果的讀時代理**,每個都只用 ≤ t 的資料:

- **trait(平常)**:席位自身最近 `window` 個活躍日的**落後**均值
  (`baseline.rolling_baseline`,不含當日)。Phase 2 E5:ε 的分點內自相關
  0.16-0.27,性格會漂移,所以用滾動窗不用全期固定效果。
- **規模/廣度效應(f)**:席位今天的 log_gross、log_n 相對自身平常的變化,
  在當日橫斷面上對「今日 − 平常」做 OLS(只用當日與之前資料),係數 × 該席位
  的變化 = 這個席位因為今天做大/做小、碰多/碰少而「應該」出現的偏移。
  §6 的 f(size, breadth) 是對水準的條件化;讀時版用**變化量**,因為 trait 已
  吸收水準。沒有這一段,席位縮小交易範圍造成的集中度上升會被叫成「特有」
  (2026-09-21 外部審查指正)。
- **共同日狀態(市況)**:扣掉規模/廣度效應後,當日所有有 baseline 席位偏離的
  橫斷面中位數——γ_t 的讀時代理(所有席位當天共同的偏移;中位數對大席位
  長尾穩健)。與交替投影估的 γ_t 不同:那是全樣本聯合估計、不可 as-of。
- **席位特有狀態(z)**:(今日 − 平常 − 規模效應 − 市況)/ 自身落後標準差。
  sd=0、歷史不足、或當日有 baseline 的席位不足 min_periods 家 → null
  (§7:不得因沒歷史就標成最高異常)。

輸入是 T4 v3(`t4_broker_measure`)的歷史切片:全部席位、涵蓋目標日前至少
`window` 個活躍日。輸出只回目標日的列。
"""

from __future__ import annotations

import datetime

import polars as pl

from ws_branch.measure import baseline, guards

DEFAULT_PRIMITIVES = ("top5_share", "directional_ratio", "cos_market_buy")


def seat_state(
    history: pl.DataFrame, *, date: datetime.date,
    primitives: tuple[str, ...] = DEFAULT_PRIMITIVES,
    window: int = 60, min_periods: int = 20,
) -> pl.DataFrame:
    """T4 v3 歷史切片 → 目標日每席位的 raw / 平常 / 規模效應 / 市況 / 特有 z。

    history:broker, date, gross_amt, n_symbols + primitives(T4 v3 列;須含 date 當日
    與其前的活躍日)。
    回傳欄:broker、`{p}`(raw)、`{p}_trait`、`{p}_size`(規模/廣度效應)、`{p}_day`、
    `{p}_state`、`{p}_n`。`{p}_day` 對同一天所有席位相同;有 baseline 的席位不足
    `min_periods` 家時 `_size/_day/_state` 皆 null(橫斷面迴歸與中位數都需要樣本)。
    """
    import numpy as np

    guards.require_columns(history, ["broker", "date", "gross_amt", "n_symbols", *primitives],
                           who="seat_state(history)")
    guards.require_unique_key(history, ["broker", "date"], who="seat_state(history)")
    if history.filter(pl.col("date") == date).height == 0:
        raise ValueError(f"seat_state:history 不含目標日 {date}")
    if history.filter(pl.col("date") > date).height:
        raise ValueError(f"seat_state:history 含目標日 {date} 之後的資料(as-of 違規)")
    df = history.select("broker", "date", *primitives,
                        pl.col("gross_amt").log10().alias("_lg"),
                        pl.col("n_symbols").cast(pl.Float64).log10().alias("_ln"))
    for c in (*primitives, "_lg", "_ln"):
        df = baseline.rolling_baseline(df, value_col=c, window=window,
                                       min_periods=min_periods, prefix=f"_{c}")
    today = df.filter(pl.col("date") == date).with_columns(
        (pl.col("_lg") - pl.col("__lg_mean")).alias("_dsize"),
        (pl.col("_ln") - pl.col("__ln_mean")).alias("_dbreadth"))
    out = today.select("broker")
    for p in primitives:
        t = today.with_columns((pl.col(p) - pl.col(f"_{p}_mean")).alias("_dev"))
        fit = t.filter(pl.col("_dev").is_not_null() & pl.col("_dsize").is_not_null()
                       & pl.col("_dbreadth").is_not_null())
        if fit.height >= min_periods:
            design = np.column_stack([np.ones(fit.height), fit["_dsize"].to_numpy(),
                                      fit["_dbreadth"].to_numpy()])
            beta, *_ = np.linalg.lstsq(design, fit["_dev"].to_numpy(), rcond=None)
            size_eff = pl.lit(float(beta[1])) * pl.col("_dsize") + pl.lit(float(beta[2])) * pl.col("_dbreadth")
            t = t.with_columns(size_eff.alias("_size"), (pl.col("_dev") - size_eff).alias("_cond"))
            day = t["_cond"].median()
        else:
            t = t.with_columns(pl.lit(None, dtype=pl.Float64).alias("_size"),
                               pl.lit(None, dtype=pl.Float64).alias("_cond"))
            day = None
        cols = t.select(
            "broker", pl.col(p),
            pl.col(f"_{p}_mean").alias(f"{p}_trait"),
            pl.col("_size").alias(f"{p}_size"),
            pl.lit(day, dtype=pl.Float64).alias(f"{p}_day"),
            pl.when((pl.col(f"_{p}_sd") > 1e-9) & pl.lit(day is not None))
            .then((pl.col("_cond") - pl.lit(day, dtype=pl.Float64)) / pl.col(f"_{p}_sd"))
            .otherwise(None).alias(f"{p}_state"),
            pl.col(f"_{p}_n").alias(f"{p}_n"))
        out = out.join(cols, on="broker", how="left")
    return out
