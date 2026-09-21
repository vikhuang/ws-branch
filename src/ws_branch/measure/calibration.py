"""Actor calibration 的統計工具(純函數,零 IO)。

架構文件 §8。**驗收對象是配置關聯與既有 cohort 區分力,不是投資人身份的
正確率**——本土席位不是已知「非外資投資人」負樣本(Phase 1 硬界限已證
至少 37.5% 的官方外資買量必須在 11 家外資席位之外),故本模組算出來的
AUC 一律讀成「對**行政 cohort** 的區分力」。負樣本含外資流量**很可能**壓低
AUC,但這不是數學保證(被污染的負樣本若本來就落在低分端,拿掉反而降 AUC),
故只說「可能低估」,**不說「下界」**(2026-09-21 外部審查指正)。

三條方法論紀律(§8 明令)
------------------------
1. **殘差化的係數只在訓練期估**:`fit_residual` 與 `apply_residual` 分開,
   測試期用訓練期的係數,不得重估(在 holdout 上重估 = 用到未見資料)。
2. **同一席位連續日不是獨立樣本**:Phase 2 量到 cos_market 有 59% 是席位
   固定效果。故 `auc_ci` 預設以**席位為單位** block bootstrap;逐列重抽
   會把信賴區間縮到假的窄(本模組提供 `unit="row"` 只為對照展示)。
3. **時間切分不打散**:金融面板隨機打散 = 前視。`time_split` 只做時序切分。
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True, slots=True)
class ResidualModel:
    """在訓練期估好的殘差化係數(截距 + 各控制項斜率)。"""

    target: str
    controls: tuple[str, ...]
    intercept: float
    coefs: tuple[float, ...]
    n_train: int


def fit_residual(
    df: pl.DataFrame, *, target: str, controls: list[str],
) -> ResidualModel:
    """OLS(正規方程)估 target ~ 1 + controls。**只餵訓練期資料**。

    控制項少(3-4 個)、樣本大(十萬級),用正規方程即可,不引入額外依賴。
    """
    sub = df.filter(
        pl.all_horizontal([pl.col(c).is_not_null() for c in [target, *controls]]))
    if sub.height <= len(controls) + 1:
        raise ValueError(f"fit_residual:訓練樣本不足 {sub.height}")
    y = sub[target].to_numpy()
    x_cols = [sub[c].to_numpy() for c in controls]
    import numpy as np

    design = np.column_stack([np.ones(len(y)), *x_cols])
    beta, *_ = np.linalg.lstsq(design, y, rcond=None)
    return ResidualModel(target=target, controls=tuple(controls),
                         intercept=float(beta[0]),
                         coefs=tuple(float(b) for b in beta[1:]),
                         n_train=sub.height)


def apply_residual(
    df: pl.DataFrame, model: ResidualModel, *, out: str | None = None,
) -> pl.DataFrame:
    """用**訓練期估好的**係數算殘差。測試期呼叫此函數,不得重估。"""
    o = out or f"{model.target}_resid"
    pred = pl.lit(model.intercept)
    for c, b in zip(model.controls, model.coefs):
        pred = pred + pl.lit(b) * pl.col(c)
    return df.with_columns((pl.col(model.target) - pred).alias(o))


def time_split(
    df: pl.DataFrame, *, date_col: str = "date", train_frac: float = 2 / 3,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """時序切分(**不打散**)。回傳 (train, test),以日期為界不切斷同一天。"""
    days = sorted(df[date_col].unique().to_list())
    cut = days[int(len(days) * train_frac)]
    return df.filter(pl.col(date_col) < cut), df.filter(pl.col(date_col) >= cut)


def auc(pos: pl.Series, neg: pl.Series) -> float:
    """Mann-Whitney AUC:隨機抽一正一負,正 > 負的機率。"""
    if pos.len() == 0 or neg.len() == 0:
        return float("nan")
    both = pl.concat([
        pl.DataFrame({"v": pos, "p": [True] * pos.len()}),
        pl.DataFrame({"v": neg, "p": [False] * neg.len()}),
    ]).with_columns(pl.col("v").rank(method="average").alias("r"))
    rs = both.filter(pl.col("p"))["r"].sum()
    return (rs - pos.len() * (pos.len() + 1) / 2) / (pos.len() * neg.len())


def auc_ci(
    df: pl.DataFrame, *, score: str, label: str, group: str = "broker",
    unit: str = "group", n_boot: int = 300, seed: int = 20260921,
    alpha: float = 0.05,
) -> tuple[float, float, float]:
    """AUC 的 bootstrap 信賴區間。回傳 (點估計, 下界, 上界)。

    `unit="group"`(預設):**以席位為單位重抽**——同一席位的連續日高度相關
    (Phase 2:cos_market 席位固定效果 59%),逐列重抽會把區間縮到假的窄。
    `unit="row"` 只供對照展示該低估有多嚴重,不得用於結論。
    """
    import random

    sub = df.filter(pl.col(score).is_not_null())
    point = auc(sub.filter(pl.col(label))[score], sub.filter(~pl.col(label))[score])
    rng = random.Random(seed)
    boots: list[float] = []
    if unit == "group":
        # 正負樣本各自以群組為單位重抽,維持原本的正負群組數
        pos_g = sub.filter(pl.col(label))[group].unique().sort().to_list()
        neg_g = sub.filter(~pl.col(label))[group].unique().sort().to_list()
        frames = {g: sub.filter(pl.col(group) == g) for g in pos_g + neg_g}
        for _ in range(n_boot):
            pick = ([rng.choice(pos_g) for _ in pos_g]
                    + [rng.choice(neg_g) for _ in neg_g])
            s = pl.concat([frames[g] for g in pick])
            a = auc(s.filter(pl.col(label))[score], s.filter(~pl.col(label))[score])
            if a == a:
                boots.append(a)
    else:
        for _ in range(n_boot):
            idx = [rng.randrange(sub.height) for _ in range(sub.height)]
            s = sub[idx]
            a = auc(s.filter(pl.col(label))[score], s.filter(~pl.col(label))[score])
            if a == a:
                boots.append(a)
    boots.sort()
    lo = boots[int(len(boots) * alpha / 2)] if boots else float("nan")
    hi = boots[int(len(boots) * (1 - alpha / 2))] if boots else float("nan")
    return point, lo, hi


def leave_one_group_out(
    df: pl.DataFrame, *, score: str, label: str, group: str,
) -> pl.DataFrame:
    """逐一拿掉某個正樣本群組,看區分力是否靠單一群組撐起。

    11 家外資席位不是 11 個獨立樣本:同集團行為相關,且大席位樣本數遠多於
    小額台。回傳每次留一的 AUC 與被留下的樣本數。
    """
    groups = df.filter(pl.col(label))[group].unique().sort().to_list()
    rows = []
    for g in groups:
        sub = df.filter(~((pl.col(group) == g) & pl.col(label))
                        & pl.col(score).is_not_null())
        rows.append({
            "left_out": g,
            "auc": auc(sub.filter(pl.col(label))[score],
                       sub.filter(~pl.col(label))[score]),
            "n_pos": sub.filter(pl.col(label)).height,
        })
    return pl.DataFrame(rows)


def leave_one_group_out_refit(
    train: pl.DataFrame, test: pl.DataFrame, *, target: str, controls: list[str],
    label: str, group: str,
) -> pl.DataFrame:
    """真正的「未見群組」檢查:殘差係數在**排除該群組**的訓練期估,再看該群組
    在測試期的殘差對負樣本分不分得開。

    `leave_one_group_out` 只是把一家從評估樣本拿掉看其餘穩不穩(影響力分析);
    規格 §8 要的 leave-one-family-out 是外推——係數不能見過它。實務上殘差係數
    是無標籤的 3 個 OLS 係數、九萬列,少一家幾乎不動,但「幾乎」要量出來,
    不能用講的(2026-09-21 外部審查指正)。回傳每群組:auc(該群組 vs 全部負樣本)、
    n_pos、係數與全樣本版的最大差。
    """
    full = fit_residual(train, target=target, controls=controls)
    groups = train.filter(pl.col(label))[group].unique().sort().to_list()
    rows = []
    for g in groups:
        m = fit_residual(train.filter(~((pl.col(group) == g) & pl.col(label))),
                         target=target, controls=controls)
        te = apply_residual(test, m, out="_r")
        pos = te.filter((pl.col(group) == g) & pl.col(label))["_r"]
        neg = te.filter(~pl.col(label))["_r"]
        rows.append({"held_out": g, "auc": auc(pos, neg), "n_pos": pos.len(),
                     "max_coef_shift": max(abs(a - b) for a, b in zip(m.coefs, full.coefs))})
    return pl.DataFrame(rows)
