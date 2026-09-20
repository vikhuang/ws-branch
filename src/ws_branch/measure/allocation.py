"""資本配置形狀的量測(純函數,零 IO)。

回答「錢怎麼分」而不是「碰了哪些」——Phase 1 實證分點日碰股中位 614 檔,
presence 幾乎無資訊(架構文件 §3.1)。

兩個定義上的硬規矩(§4.2):

1. **全 universe cosine**:分子與兩側 norm 都在共同 universe 上,分點未交易
   的股票視為零(故參考向量的 norm 含分點沒碰的股票)。O1 探針用的是
   support-restricted 版(參考向量只取分點碰過的部分),兩者差一個
   ‖ref_support‖ / ‖ref_full‖ 因子,對大籃子分點 ≈1、小籃子 <1,**會改變
   跨分點排序**。兩者不可混用,本模組只提供全 universe 版。
2. **前一交易日用交易日曆推導**,不用固定曆日緩衝:2026 農曆年連休 12 個
   曆日,固定 7/14 天的緩衝哪年會不夠純看農曆。呼叫端傳入日曆,本模組不讀檔。

零 norm(分點當日該側完全無成交)回傳 null 並附原因,不回傳 0——0 代表
「完全不像」,「沒得比」是另一種狀態。
"""

from __future__ import annotations

import datetime

import polars as pl


def prev_trading_day_map(trading_days: list[datetime.date]) -> pl.DataFrame:
    """交易日曆 → (date, prev_date) 對照。首日的 prev_date 為 null。

    呼叫端負責提供涵蓋所需區間的日曆(ws-core `tradedays`),**且必須比
    目標區間早至少一個交易日**,否則區間首日會誤判成「無前一日」。
    """
    days = sorted(set(trading_days))
    return pl.DataFrame({"date": days}).with_columns(
        pl.col("date").shift(1).alias("prev_date"))


def amount_cosine(
    flow: pl.DataFrame,
    reference: pl.DataFrame,
    *,
    amount_col: str,
    ref_col: str,
    out: str,
    group_cols: tuple[str, ...] = ("broker", "date"),
    ref_keys: tuple[str, ...] = ("symbol_id", "date"),
) -> pl.DataFrame:
    """分點配置向量 vs 參考向量的全 universe 金額 cosine。

    flow:group_cols + symbol_id + date + amount_col(**已套 universe gate**)。
    reference:ref_keys + ref_col(同一 universe 的參考向量,如官方外資買入
    金額或全市場成交金額)。

    cos = Σ_s a_s·r_s / (‖a‖ · ‖r_full‖),其中 ‖r_full‖ 在**整個 universe**
    上計算(每日一個純量),不只分點碰過的部分。
    """
    ref_norm = (reference.group_by("date")
                .agg((pl.col(ref_col) ** 2).sum().sqrt().alias("_rnorm")))
    joined = (flow.filter(pl.col(amount_col) > 0)
              .join(reference, on=list(ref_keys), how="inner"))
    return (joined.group_by(list(group_cols))
            .agg((pl.col(amount_col) * pl.col(ref_col)).sum().alias("_dot"),
                 (pl.col(amount_col) ** 2).sum().sqrt().alias("_anorm"))
            .join(ref_norm, on="date", how="left")
            .with_columns(
                pl.when((pl.col("_anorm") > 0) & (pl.col("_rnorm") > 0))
                .then(pl.col("_dot") / (pl.col("_anorm") * pl.col("_rnorm")))
                .otherwise(None).alias(out))
            .select(*group_cols, out))


def basket_self_similarity(
    flow: pl.DataFrame,
    date_map: pl.DataFrame,
    *,
    amount_col: str = "gross",
    out: str = "basket_self_sim",
) -> pl.DataFrame:
    """分點今日購物籃 vs **前一交易日**的 cosine(金額加權)。

    flow:broker + symbol_id + date + amount_col(已套 universe gate;需含
    目標區間前一個交易日的資料,由呼叫端負責多取)。
    date_map:`prev_trading_day_map` 的輸出。

    兩日皆有完整資料但籃子完全不重疊 → 回傳 0(真的不像);
    前一交易日該分點沒交易 → 該列不出現(呼叫端 left join 後為 null,
    語意是「沒得比」)。兩者不可混淆(§12 點名案例)。
    """
    basket = (flow.filter(pl.col(amount_col) > 0)
              .select("broker", "symbol_id", "date",
                      pl.col(amount_col).alias("_v")))
    norms = basket.group_by("broker", "date").agg(
        (pl.col("_v") ** 2).sum().sqrt().alias("_norm"))
    prev = basket.rename({"date": "prev_date", "_v": "_v_prev"})
    dot = (basket.join(date_map, on="date", how="inner")
           .join(prev, on=["broker", "symbol_id", "prev_date"], how="inner")
           .group_by("broker", "date")
           .agg((pl.col("_v") * pl.col("_v_prev")).sum().alias("_dot")))
    # 今日有交易、前一交易日也有交易的分點日全集(含 dot=0 的無重疊情形)
    both = (norms.join(date_map, on="date", how="inner")
            .join(norms.rename({"date": "prev_date", "_norm": "_norm_prev"}),
                  on=["broker", "prev_date"], how="inner"))
    return (both.join(dot, on=["broker", "date"], how="left")
            .with_columns(pl.col("_dot").fill_null(0.0))
            .with_columns((pl.col("_dot")
                           / (pl.col("_norm") * pl.col("_norm_prev"))).alias(out))
            .select("broker", "date", out))
