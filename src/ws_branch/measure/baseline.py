"""滾動 baseline 與 state anomaly(純函數,零 IO)。

Phase 2 實證(findings/v3_phase2_trait_state.md E5):扣掉年度固定效果 α_b
之後,殘差的分點內 lag-1 自相關仍有 **0.16-0.27**(top5_share 最高),代表
**席位性格會漂移**,單一年度的固定效果吃不乾淨。故架構文件 §6 要的
「席位特有異常」在產品端必須用**滾動窗**定義,不能用全期固定效果。

兩條時間紀律(§4.5):

1. **嚴格落後**:baseline 只用 t 之前的資料(`closed="left"`),今天不進自己
   的基準。違反會在 as-of 產品裡製造前視。
2. **最小樣本**:不足 min_periods 的分點日回傳 null,不用縮短的窗硬算——
   新席位、久未交易的席位會因此沒有 state,這是正確行為,不得因為「沒有
   歷史」就標成最高異常(§7 明令)。

窗以**分點自身的活躍日**計(row-based),不是曆日:出勤不規律的席位用曆日
窗會被缺勤日稀釋。代價是不同席位的窗實際跨越的曆日長度不同,引用時要一併
揭露 `baseline_n`。
"""

from __future__ import annotations

import polars as pl

from ws_branch.measure import guards


def rolling_baseline(
    df: pl.DataFrame,
    *,
    value_col: str,
    group_col: str = "broker",
    order_col: str = "date",
    window: int = 60,
    min_periods: int = 20,
    prefix: str | None = None,
) -> pl.DataFrame:
    """每個席位在自身最近 `window` 個活躍日上的落後均值/標準差/樣本數。

    回傳新增 `{prefix}_mean` / `{prefix}_sd` / `{prefix}_n`(prefix 預設為
    value_col)。**不含當日**——`closed="left"` 由 shift(1) 實現。
    """
    p = prefix or value_col
    # row-based 滾動窗把每一列當一個活躍日:重複的 (group, order) 會讓同一天
    # 被數兩次、窗長度悄悄縮短,且不報錯——當場擋下(guards A 類)。
    guards.require_unique_key(df, [group_col, order_col], who="rolling_baseline")
    ordered = df.sort(group_col, order_col)  # 回傳已排序,原順序不保留
    lagged = pl.col(value_col).shift(1).over(group_col)
    return ordered.with_columns(
        lagged.rolling_mean(window_size=window, min_samples=min_periods)
        .over(group_col).alias(f"{p}_mean"),
        lagged.rolling_std(window_size=window, min_samples=min_periods)
        .over(group_col).alias(f"{p}_sd"),
        lagged.is_not_null().cast(pl.Int32)
        .rolling_sum(window_size=window, min_samples=1)
        .over(group_col).alias(f"{p}_n"),
    )


def state_anomaly(
    df: pl.DataFrame,
    *,
    value_col: str,
    prefix: str | None = None,
    out: str | None = None,
    min_sd: float = 1e-9,
) -> pl.DataFrame:
    """(今日 − 落後均值) / 落後標準差。

    標準差為 0(該席位在窗內完全沒變動)時回傳 null,不回傳 inf——零變異下
    「偏離幾個標準差」無定義。差值本身另存 `{out}_raw`,供不想標準化的讀法。
    """
    p = prefix or value_col
    o = out or f"{value_col}_z"
    diff = pl.col(value_col) - pl.col(f"{p}_mean")
    return df.with_columns(
        diff.alias(f"{o}_raw"),
        pl.when(pl.col(f"{p}_sd") > min_sd)
        .then(diff / pl.col(f"{p}_sd")).otherwise(None).alias(o),
    )
