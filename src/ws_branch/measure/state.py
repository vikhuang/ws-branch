"""席位 trait / 共同日狀態 / 席位特有狀態的**讀時**計算(純函數,零 IO)。

架構文件 §6 的三段:X = trait(α_b)+ 共同日效應(γ_t)+ 席位特有異常(ε)。
Phase 2 用交替投影在全樣本上**聯合估計**——那是 retrospective 描述,不能進
as-of 產品(§4.5:baseline 只能用當時可得資料)。本模組給讀本用的是三個
**因果的讀時代理**,每個都只用 ≤ t 的資料:

- **trait(平常)**:席位自身最近 `window` 個活躍日的**落後**均值
  (`baseline.rolling_baseline`,不含當日)。Phase 2 E5:ε 的分點內自相關
  0.16-0.27,性格會漂移,所以用滾動窗不用全期固定效果。
- **共同日狀態(市況)**:當日**所有有 baseline 的席位**「今日 − 自身平常」的
  橫斷面中位數。這是 γ_t 的讀時代理:γ_t 定義為所有席位當天共同的偏移,
  中位數對大席位的長尾穩健;與交替投影估的 γ_t 不同之處是它不同時扣 f(size,
  breadth)——讀本要的是「今天全市場往哪偏」,不是變異分解。
- **席位特有狀態(z)**:(今日 − 平常 − 市況)/ 自身落後標準差。市況扣掉後
  剩下的才是「這個席位今天自己的事」;sd=0 或歷史不足 → null(§7:不得因
  沒歷史就標成最高異常)。

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
    """T4 v3 歷史切片 → 目標日每席位的 raw / 平常 / 市況 / 特有 z(每個 primitive 四欄)。

    history:broker, date + primitives(T4 v3 列;須含 date 當日與其前的活躍日)。
    回傳欄:broker、`{p}`(raw)、`{p}_trait`、`{p}_day`、`{p}_state`、`{p}_n`。
    `{p}_day` 對同一天所有席位相同;有 baseline 的席位不足 `min_periods` 家時為 null
    (橫斷面中位數本身也需要樣本)。
    """
    guards.require_columns(history, ["broker", "date", *primitives], who="seat_state(history)")
    guards.require_unique_key(history, ["broker", "date"], who="seat_state(history)")
    if history.filter(pl.col("date") == date).height == 0:
        raise ValueError(f"seat_state:history 不含目標日 {date}")
    if history.filter(pl.col("date") > date).height:
        raise ValueError(f"seat_state:history 含目標日 {date} 之後的資料(as-of 違規)")
    df = history.select("broker", "date", *primitives)
    for p in primitives:
        df = baseline.rolling_baseline(df, value_col=p, window=window,
                                       min_periods=min_periods, prefix=f"_{p}")
    today = df.filter(pl.col("date") == date)
    out = today.select("broker")
    for p in primitives:
        dev = pl.col(p) - pl.col(f"_{p}_mean")           # 今日 − 平常(有 baseline 才非 null)
        with_dev = today.with_columns(dev.alias("_dev"))
        n_based = with_dev["_dev"].drop_nulls().len()
        day = with_dev["_dev"].median() if n_based >= min_periods else None
        cols = with_dev.select(
            "broker", pl.col(p),
            pl.col(f"_{p}_mean").alias(f"{p}_trait"),
            pl.lit(day, dtype=pl.Float64).alias(f"{p}_day"),
            pl.when((pl.col(f"_{p}_sd") > 1e-9) & pl.lit(day is not None))
            .then((pl.col("_dev") - pl.lit(day, dtype=pl.Float64)) / pl.col(f"_{p}_sd"))
            .otherwise(None).alias(f"{p}_state"),
            pl.col(f"_{p}_n").alias(f"{p}_n"))
        out = out.join(cols, on="broker", how="left")
    return out
