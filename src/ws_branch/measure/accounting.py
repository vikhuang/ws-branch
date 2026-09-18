"""會計恆等式與硬上下界(純函數,零 IO)。

v3 架構文件 §5.1/§5.2:這一層**不依賴任何行為模型**,只用非負性與總量約束,
語意等級是 `identified`(餘額)與 `bounded`(cohort 內外資量)。它先於校準,
因為「未識別的質量有多大」決定後面的投入值不值得。

核心不等式(單一股票、單日、單側):
  V = 同口徑全市場成交量, F = 官方外資量, S = 目標 cohort 觀測量,
  X = cohort 之中真正屬於外資的量。
  非負性:        X >= 0, X <= S, X <= F
  cohort 外配額:  S - X <= V - F  (cohort 內的非外資量不能超過全市場非外資量)
  =>  L_X = max(0, S + F - V),  U_X = min(S, F)
  cohort 外外資量 Y = F - X  =>  Y ∈ [max(0, F - S), min(F, V - S)]

**S/F 不是外資 coverage**——它是「cohort 觀測量對官方量的比值」,分子分母
量的不是同一件事(cohort 裡可能有非外資、外資也可能在 cohort 外)。真正的
coverage 區間是 [L_X/F, U_X/F],只有 L_X = U_X 時才唯一識別。

閉環但書(A5):broker 日報 ⊆ TEJ vol。若 V 取自 TEJ 而 T1 觀測不到全部,
差額是「未觀測交易」,不能算進任何可見席位。本模組要求呼叫端同時給
`observed_total`(T1 加總)與 `market_total`(V),把未觀測量顯式留一欄。
"""

from __future__ import annotations

import polars as pl


def flow_bounds(
    df: pl.DataFrame,
    *,
    cohort_col: str,
    official_col: str,
    market_col: str,
    prefix: str,
) -> pl.DataFrame:
    """對每列(股票日單側)算 X 與 Y 的硬上下界。

    df 需含 cohort_col(S)、official_col(F)、market_col(V)。回傳原表加上
    `{prefix}_x_lo/_x_hi/_x_width`(cohort 內外資量)、`{prefix}_y_lo/_y_hi`
    (cohort 外外資量)、`{prefix}_cov_lo/_cov_hi`(真 coverage 區間,F=0 時 null)、
    `{prefix}_bounds_ok`(輸入自洽:0 <= S,F <= V)。

    不做 clipping:輸入不自洽時 bounds_ok=False,界限欄位照算但呼叫端必須
    據此擋下發布(§5.1「不能用 clipping 讓結果表面落在合法範圍」)。
    """
    s, f, v = pl.col(cohort_col), pl.col(official_col), pl.col(market_col)
    x_lo = pl.max_horizontal(pl.lit(0.0), s + f - v)
    x_hi = pl.min_horizontal(s, f)
    y_lo = pl.max_horizontal(pl.lit(0.0), f - s)
    y_hi = pl.min_horizontal(f, v - s)
    return df.with_columns(
        x_lo.alias(f"{prefix}_x_lo"),
        x_hi.alias(f"{prefix}_x_hi"),
        (x_hi - x_lo).alias(f"{prefix}_x_width"),
        y_lo.alias(f"{prefix}_y_lo"),
        y_hi.alias(f"{prefix}_y_hi"),
        pl.when(f > 0).then(x_lo / f).otherwise(None).alias(f"{prefix}_cov_lo"),
        pl.when(f > 0).then(x_hi / f).otherwise(None).alias(f"{prefix}_cov_hi"),
        ((s >= 0) & (f >= 0) & (s <= v) & (f <= v)).alias(f"{prefix}_bounds_ok"),
    )


def actor_residual(
    df: pl.DataFrame,
    *,
    market_col: str,
    bucket_cols: list[str],
    out: str,
) -> pl.DataFrame:
    """會計餘額 Other = V − Σ(官方桶),語意 `identified`(總量),**不是散戶**。

    餘額的投資人種類與席位配置皆未識別(§5.2);命名刻意不帶 retail 字樣。
    另回傳 `{out}_ok`:餘額非負且各桶非負才成立(桶不互斥或口徑不同會破)。
    """
    total_buckets = pl.sum_horizontal([pl.col(c) for c in bucket_cols])
    return df.with_columns(
        (pl.col(market_col) - total_buckets).alias(out),
        (
            (pl.col(market_col) - total_buckets >= 0)
            & pl.all_horizontal([pl.col(c) >= 0 for c in bucket_cols])
        ).alias(f"{out}_ok"),
    )


ODD_LOT_TOLERANCE_SH: float = 1_000.0
"""零股容差 1 張(= 1,000 股)。

TEJ vol 只計整張、broker_tx 含零股(A5 實證:119,153 股 vs 119 張),所以
T1 加總幾乎必然小幅超過 V×1000。2026 全年實測:不加容差時 96.8% 的股票日
會被誤判成閉環破裂。超過 1 張才是真的破裂——與 `checks.closure_overshoots`
同一條規則,不另立第二套容差。
"""


def unobserved_flow(
    df: pl.DataFrame, *, market_col: str, observed_col: str, out: str,
    tolerance: float = ODD_LOT_TOLERANCE_SH,
) -> pl.DataFrame:
    """未觀測交易 = V − T1 可觀測總量(鉅額/特殊交易;A5 實證 broker ⊆ TEJ)。

    差額為小幅負值是零股尾差(見 ODD_LOT_TOLERANCE_SH),不是錯誤;超過容差
    才代表閉環破裂,`{out}_ok` 標 False。差額本身照實存(可為負),不 clip。
    """
    diff = pl.col(market_col) - pl.col(observed_col)
    return df.with_columns(diff.alias(out), (diff >= -tolerance).alias(f"{out}_ok"))
