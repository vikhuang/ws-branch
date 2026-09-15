"""Broker taxonomy 凍結名單:虎尾幫三期、signature pair、pair archetype。

遷自 ws-quant `src/broker_taxonomy/constants.py`(2026-09-15;source of truth
= experiments/dispo_recurring_broker/ 的 Sprint 2 findings)。名單更新應出
v3 新版本,不直接改此檔。

**遷移時移除**:CLUSTER_NAMES 歸 `measurement.identity`;CLUSTER_SUMMARY
(手填 vol_share)經體檢判定為錯誤記載,由 `identity.CLUSTER_VOLUME_SHARE`
(版控重算、雙分母)取代——見 docs/audit_ledger.md A1。
**體檢狀態**:本檔名單建於舊制/2025 前資料,2026 新制 regime 未重驗
(audit_ledger 未結案項),引用時注意時效。
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import polars as pl

_TAX_DIR = Path(__file__).resolve().parents[3] / "experiments" / "taxonomy"

ClusterLabel = Literal[0, 1, 2, 3]


# 虎尾幫 hardcore(2021-2025 三期皆屬 C1,4 年鐵桿)
HARDCORE_HUWEI: frozenset[str] = frozenset({
    "兆豐-虎尾",
    "凱基-總公司",
    "永豐金-潮州",
})

# 虎尾幫 fadeout(2021-2022 是後來不是 — user intel「被外資量化打爆」的簽名)
FADEOUT_HUWEI: frozenset[str] = frozenset({
    "凱基-斗六",
    "台灣企銀-三民",
    "國票-南京",
    "凱基-虎尾",
})

# 虎尾幫 new(2023 起才變 C1-like)
NEW_HUWEI: frozenset[str] = frozenset({
    "元大-忠孝鼎富",
    "兆豐-內湖",
    "台中銀-高雄",
    "台灣企銀-建成",
    "致和-台北",
})


# 9 個地方 buyer 專營戶(Sprint 2 H2 verify:IS corr +0.537,OOS n=4 corr +0.875 方向一致)
# (broker_name, stock_id)
BUYER_SIGNATURE_PAIRS: frozenset[tuple[str, str]] = frozenset({
    ("元大-歸仁", "7728"),
    ("元大-大雅", "6499"),
    ("統一-士林", "6907"),
    ("元大-台北", "4946"),  # 例外:都會分行,IS 反向 corr,保留追蹤
})

# 5 個 seller signature pair(對照組;Sprint 2 finding = 純 selection bias 非 alpha)
SELLER_SIGNATURE_PAIRS: frozenset[tuple[str, str]] = frozenset({
    ("凱基-台北", "5386"),
    ("凱基-台北", "4722"),
    ("凱基-台北", "8096"),
    ("富邦-建國", "6907"),
    ("國票-北投", "8042"),
})


# 4 種 pair archetype(v2 finding,broker × symbol 專營性分類)
PairArchetype = Literal[
    "institutional_daily",  # 機構日常持倉 e.g. 摩根大通→2330
    "dedicated_making",     # 專營做股 e.g. 京城-嘉義→9919
    "occasional_whale",     # 偶發大戶單
    "day_trade_regular",    # 頻繁進出小額
    "minor",                # 邊界,unrated
]

ARCHETYPE_DESCRIPTION: dict[str, str] = {
    "institutional_daily": "機構長期持倉,persistence≥85% 且 concentration≥5%",
    "dedicated_making": "專營做股,persistence≥40% AND market_share≥5% AND concentration≥1.5%",
    "occasional_whale": "偶發大戶單,persistence<20% 但當時 market_share≥15%",
    "day_trade_regular": "頻繁進出小額,persistence≥25% AND market_share≥2% AND low concentration",
    "minor": "邊界或活躍度太低,unrated",
}


def load_pair_archetypes(path: Path | None = None) -> pl.DataFrame:
    """讀 broker×symbol pair archetype 表(遷移後正典在 experiments/taxonomy/)。"""
    return pl.read_parquet(path or _TAX_DIR / "broker_symbol_archetypes.parquet")


def load_symbol_pairs(path: Path | None = None) -> pl.DataFrame:
    """讀 broker×symbol persistence pair 表。"""
    return pl.read_parquet(path or _TAX_DIR / "broker_symbol_pairs.parquet")


def load_day_features(path: Path | None = None) -> pl.DataFrame:
    """讀 broker×day 行為特徵表(分群重做的原料;窗 2021-01~2025-08 抽樣)。"""
    return pl.read_parquet(path or _TAX_DIR / "broker_day_features.parquet")
