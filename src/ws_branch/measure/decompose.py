"""Trait / 共同日狀態 / 席位特有異常的分解(純函數,零 IO)。

v3 架構文件 §6:

    X_{b,t} = μ + f(log G, B) + α_b + γ_t + ε_{b,t}

三個設計決定,都是 §6 明令:

1. **聯合估計,不是各自估一次再相減**。size/breadth 的條件化與兩組固定效果
   彼此不正交(大席位系統性碰更多股票),分開估再相減會重複扣除。這裡用
   交替投影(alternating projections / 反覆去中心化)聯合收斂。
2. **f 用分箱均值**,不假設函數形式——Phase 1 已見 top5_share 對 breadth
   單調但非線性、cos_market 對 breadth 飽和。
3. **變異占比不強制加總 100%**。非正交設計下增量 R² 依順序而變,呼叫端必須
   跑兩個順序並同時報告(見 `sequential_r2`)。

`ε` 的解讀界線:它是「扣掉席位長期水準與當日全市場共同狀態後,這個席位
今天的偏離」,**不是**「這個席位做了什麼」——ε 大只說明與自身常態不符,
成因(客戶進出、單一大單、資料瑕疵)未識別。
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

_MAX_ITER = 50
_TOL = 1e-10


@dataclass(frozen=True, slots=True)
class Decomposition:
    """分解結果。`frame` 為逐列成分,其餘為摘要統計。"""

    frame: pl.DataFrame           # 原鍵 + y, f_hat, alpha, gamma, resid
    var_total: float
    var_shares: dict[str, float]  # 各成分「自身變異 / 總變異」(非正交,不加總)
    n_obs: int
    n_branch: int
    n_day: int
    iterations: int


def _bin_expr(col: str, edges: list[float], alias: str) -> pl.Expr:
    """把連續變數切成有序箱(超出範圍的併入首尾箱,不產生 null)。"""
    e = pl.col(col)
    out = pl.when(e < edges[0]).then(0)
    for i, hi in enumerate(edges):
        out = out.when(e < hi).then(i)
    return out.otherwise(len(edges)).cast(pl.Int32).alias(alias)


def add_conditioning_bins(
    df: pl.DataFrame, *, size_col: str, breadth_col: str,
    size_edges: list[float], breadth_edges: list[float],
) -> pl.DataFrame:
    """加上 (size, breadth) 二維分箱鍵——f 的定義域。"""
    return df.with_columns(
        _bin_expr(size_col, size_edges, "_sbin"),
        _bin_expr(breadth_col, breadth_edges, "_bbin"),
    ).with_columns(
        (pl.col("_sbin") * 1000 + pl.col("_bbin")).alias("_cell"))


def decompose(
    df: pl.DataFrame, *, y: str, branch_col: str = "broker",
    date_col: str = "date", cell_col: str = "_cell",
    max_iter: int = _MAX_ITER, tol: float = _TOL,
) -> Decomposition:
    """交替投影聯合估計 μ + f(cell) + α_b + γ_t。

    每輪依序把當前殘差的 cell 均值、席位均值、日均值抽出來累加到各成分,
    直到殘差的均值調整量收斂。這等價於三組固定效果的最小平方解(Frisch-
    Waugh-Lovell),但不需要建 (n_branch + n_day + n_cell) 維設計矩陣。

    回傳的 `f_hat`/`alpha`/`gamma` 已各自去掉自身均值(歸入 μ),故三者
    均值皆為 0,`y ≈ μ + f_hat + alpha + gamma + resid` 逐列成立。
    """
    work = df.filter(pl.col(y).is_not_null()).select(
        branch_col, date_col, cell_col, pl.col(y).alias("_y"))
    mu = work["_y"].mean()
    work = work.with_columns((pl.col("_y") - mu).alias("_r"),
                             pl.lit(0.0).alias("_f"),
                             pl.lit(0.0).alias("_a"),
                             pl.lit(0.0).alias("_g"))
    def _max_group_mean(w: pl.DataFrame) -> float:
        """殘差在三組鍵上的最大絕對群均值——全部為 0 即三組投影都已收斂。"""
        return max(
            w.group_by(k).agg(pl.col("_r").mean().alias("m"))["m"].abs().max()
            for k in (cell_col, branch_col, date_col))

    it = 0
    for it in range(1, max_iter + 1):
        for key, comp in ((cell_col, "_f"), (branch_col, "_a"), (date_col, "_g")):
            adj = pl.col("_r").mean().over(key)
            work = work.with_columns((pl.col(comp) + adj).alias(comp),
                                     (pl.col("_r") - adj).alias("_r"))
        if _max_group_mean(work) < tol:
            break
    # 各成分去均值,常數併入 mu
    for comp in ("_f", "_a", "_g"):
        work = work.with_columns((pl.col(comp) - work[comp].mean()).alias(comp))
    var_total = work["_y"].var()
    shares = {c: (work[k].var() / var_total if var_total else float("nan"))
              for c, k in (("f", "_f"), ("alpha", "_a"),
                           ("gamma", "_g"), ("resid", "_r"))}
    frame = work.rename({"_y": y, "_f": "f_hat", "_a": "alpha",
                         "_g": "gamma", "_r": "resid"})
    return Decomposition(
        frame=frame, var_total=var_total, var_shares=shares,
        n_obs=work.height, n_branch=work[branch_col].n_unique(),
        n_day=work[date_col].n_unique(), iterations=it)


def sequential_r2(
    df: pl.DataFrame, *, y: str, order: list[str], branch_col: str = "broker",
    date_col: str = "date", cell_col: str = "_cell",
) -> dict[str, float]:
    """依指定順序逐一加入成分,回傳各步的增量 R²。

    **非正交設計下順序會改變結果**,呼叫端必須跑至少兩個順序並同時報告
    (§6:不強行把多組解釋變異當成可相加到 100% 的份額)。

    order 元素為 "f" / "alpha" / "gamma"。
    """
    key_of = {"f": cell_col, "alpha": branch_col, "gamma": date_col}
    work = df.filter(pl.col(y).is_not_null()).select(
        branch_col, date_col, cell_col, pl.col(y).alias("_y"))
    total = work["_y"].var()
    work = work.with_columns((pl.col("_y") - work["_y"].mean()).alias("_r"))
    out, prev_explained = {}, 0.0
    for name in order:
        work = work.with_columns(
            (pl.col("_r") - pl.col("_r").mean().over(key_of[name])).alias("_r"))
        explained = 1.0 - work["_r"].var() / total
        out[name] = explained - prev_explained
        prev_explained = explained
    out["residual"] = 1.0 - prev_explained
    return out
