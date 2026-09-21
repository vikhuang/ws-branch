"""入口前提檢查(純函數,零 IO)。

**為什麼有這個模組**:2026-09-16~21 的四次同型事故——`mean_horizontal` 靜默
跳 null、`n_symbols` 未依定義濾 gross>0、basket 對日曆外日期靜默 inner-join
丟掉、Phase 3 抽樣母體漏套 universe gate——共同形狀都是

> 「呼叫端該保證的前提,函數沒檢查,資料剛好沒踩到」。

四次都是事後複查才抓到,不是設計時防住的。故把前提分三類,**能在入口
便宜檢的一律當場 raise**,不能檢的寫成契約與可執行測試。

前提的三分類
------------
**A 類:便宜且函數內可自檢**(O(n) 掃一遍,不需外部知識)——本模組提供。
  鍵唯一性、必要欄位與型別、值域/非負、鍵欄 null、對照表涵蓋(anti-join
  為空)、join 列數守恆、空輸入。**一律 raise,不 warn、不靜默降級。**

**B 類:便宜但函數自己不知道參考**——解法是**改簽名把參考傳進來**,
  讓它降級成 A 類。例如:
  - universe 一致性:分子與分母是否套了同一個 gate → 把 universe 傳進來
    (Phase 3 的抽樣漏 gate 就是這一類,當時分子分母來源不同卻無從比對)。
  - 交易日曆涵蓋:把日曆傳進來而非在函數內猜(allocation 已如此)。
  **設計守則:與其在 docstring 寫「呼叫端須先 gate」,不如把 gate 當參數。**

**C 類:不可能在入口便宜檢**——只能靠契約 + 可執行測試,docstring 要明說
  「本函數無法自行辨識這種污染」。例如:
  - `amount_cosine` 的 reference 是否為同一 universe 的向量(函數不知道
    universe 是什麼;混入指數列會壓低所有 cosine 且不報錯)。
  - 上游資料的外部真實性(TEJ vol 兩日偏低,A6)。
  - 統計假設(殘差是否白噪音)。
  C 類的處置是把危害寫成**會失敗的測試**當文件,見
  `tests/test_allocation_baseline.py::test_cosine_is_deflated_by_...`。

成本原則
--------
所有檢查都是單次 O(n) 掃描或 hash 聚合,對百萬列等級 <100ms;建表主迴圈
逐月呼叫也只增加秒級。**不做**需要 join 大表或多次 pass 的檢查——那屬於
verify 層(registry),不是入口。
"""

from __future__ import annotations

import polars as pl


def require_columns(df: pl.DataFrame, cols: list[str], *, who: str) -> None:
    """必要欄位存在。缺欄在 polars 會在很後面才炸,且訊息指不到呼叫點。"""
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{who}:缺少必要欄位 {missing};實際欄位 {df.columns}")


def require_unique_key(df: pl.DataFrame, keys: list[str], *, who: str) -> None:
    """鍵唯一。重複鍵會讓 row-based 滾動窗把同一天數兩次、join 悄悄放大列數。

    2026-09-21 實付:`rolling_baseline` 遇重複 (broker, date) 時 9 列進 9 列出,
    窗長縮短且無任何警示。
    """
    n_dup = df.height - df.select(keys).unique().height
    if n_dup:
        raise ValueError(f"{who}:{n_dup} 列重複鍵 {keys}(共 {df.height} 列)")


def require_no_nulls(df: pl.DataFrame, cols: list[str], *, who: str) -> None:
    """鍵欄/旗標欄不得為 null。

    null 旗標的害處是下游 `~flag` 篩選會靜默漏列——2026-09-20 t3b 有 112 列
    因 T3 值缺而三個旗標皆 null。
    """
    bad = {c: df[c].null_count() for c in cols if df[c].null_count()}
    if bad:
        raise ValueError(f"{who}:欄位含 null 但不允許 {bad}")


def require_non_negative(df: pl.DataFrame, cols: list[str], *, who: str) -> None:
    """金額/股數不得為負(負值通常代表上游欄位對錯或符號慣例不一致)。"""
    bad = {c: df.filter(pl.col(c) < 0).height for c in cols
           if df.filter(pl.col(c) < 0).height}
    if bad:
        raise ValueError(f"{who}:欄位有負值 {bad}")


def require_covered(
    df: pl.DataFrame, reference: pl.DataFrame, keys: list[str], *, who: str,
    hint: str = "",
) -> None:
    """df 的鍵必須全部被 reference 涵蓋(anti-join 為空)。

    未涵蓋時 inner join 會**靜默丟列**——2026-09-21 實付:basket 對交易日曆
    外的日期直接消失,0 列輸出且無訊息。
    """
    miss = df.select(keys).unique().join(reference.select(keys).unique(),
                                         on=keys, how="anti")
    if miss.height:
        sample = miss.head(3).rows()
        raise ValueError(
            f"{who}:{miss.height} 個鍵不在對照表中(例 {sample}){hint}")


def require_same_universe(
    numerator: pl.DataFrame, denominator_universe: pl.DataFrame, *,
    keys: list[str], who: str,
) -> None:
    """分子與分母必須套同一個 universe gate(B 類降級成 A 類的典型)。

    2026-09-21 實付:Phase 3 抽樣母體從 T1 原始代號抽,分子含 ETF/興櫃而
    分母(席位日總額)已 gate,salience 定義不一致——當時兩邊來源不同,
    函數無從比對。把 universe 當參數傳進來,這個錯就變成可檢的。
    """
    require_covered(numerator, denominator_universe, keys, who=who,
                    hint=";分子未套 universe gate 或 gate 版本不一致")


def require_non_empty(df: pl.DataFrame, *, who: str) -> None:
    """空輸入要當場說清楚,不要讓下游算出一片 NaN 再回頭找原因。"""
    if df.height == 0:
        raise ValueError(f"{who}:輸入為空")


def warn_if_lossy_join(
    before: int, after: int, *, who: str, tolerance: float = 0.0,
) -> None:
    """1:1 join 前後列數守恆。放大代表對照表有重複鍵,縮小代表涵蓋不足。

    tolerance 為可接受的縮減比例(0 = 完全不許)。
    """
    if after > before:
        raise ValueError(f"{who}:join 後列數增加 {before}→{after},對照表有重複鍵")
    if before and (before - after) / before > tolerance:
        raise ValueError(
            f"{who}:join 後列數減少 {before}→{after}"
            f"({(before - after) / before:.2%} > 容許 {tolerance:.2%})")
