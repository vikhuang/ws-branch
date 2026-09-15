"""身分量測:分點是誰在用(名字規則 + 行為分群 + 官方對帳錨)。

遷自 ws-quant(2026-09-15,docs/REDESIGN_2026-09.md §5),兩處修正:
1. 名字規則的 prop 桶標記棄用——「-自營」席位僅蓋 0.06% 全量(2026-09-15
   對帳實證):自營主力單併入無 dash 總公司席位申報(TWSE 規則:總分公司
   自營買賣併入總公司)。無 dash 席位語意 = 投信+自營+本土法人混合通道
   (與官方投信買賣超 corr≈0.58),非單一主體。
2. CLUSTER_SUMMARY 的手填 vol_share(0.53/0.36)無版控出處且兩種分母皆
   無法重現,以版控重算值取代(見 CLUSTER_VOLUME_SHARE 與 docs/audit_ledger.md)。

官方對帳錨(2026-09-15,ws-quant u3 實證):外資名字席位淨額 vs 官方
qfii_ex Pearson +0.973,gross 覆蓋 76%——外資身分先驗可信;缺的 24% =
外資經本土券商下單。
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import polars as pl

_REPO = Path(__file__).resolve().parents[1]
CLUSTERS_PARQUET = _REPO / "experiments" / "broker_clusters.parquet"

BrokerCohort = Literal["retail", "prop_shell", "institutional", "mixed", "unknown"]

_FOREIGN_PREFIXES = ("港商", "美商", "日商", "新加坡商", "法銀")
_FOREIGN_EXACT = frozenset({
    "摩根大通", "美林", "花旗環球", "台灣摩根士丹利", "香港上海匯豐",
    "犇亞證券", "大和國泰",
})
_FUTURES_AGGREGATES = frozenset({"元大期貨", "群益期貨"})


def classify_broker_cohort(broker_name: str | None) -> BrokerCohort:
    """名字規則五類(遷自 ws-quant aux1 凍結規則,prop 更名 prop_shell)。

    prop_shell:掛名「-自營」席位,僅 0.06% 全量——**不代表自營商流量**,
    自營主力藏在 mixed(無 dash HQ)。下游不得把 prop_shell 當自營 proxy,
    自營流量請用官方 tej_shareholding dlrp/dlrh。
    """
    if broker_name is None or broker_name == "":
        return "unknown"
    if any(broker_name.startswith(p) for p in _FOREIGN_PREFIXES):
        return "institutional"
    if broker_name in _FOREIGN_EXACT:
        return "institutional"
    if "-自營" in broker_name:
        return "prop_shell"
    if "法人" in broker_name:
        return "institutional"
    if broker_name in _FUTURES_AGGREGATES:
        return "mixed"
    if "-" in broker_name:
        return "retail"
    return "mixed"


CLUSTER_NAMES: dict[int, str] = {
    0: "都會大型散戶密集",
    1: "鄉鎮藏單/隔日沖(虎尾幫)",
    2: "中型主力密集",
    3: "小型地方性散戶",
}

# 版控重算(2026-09-15,broker_day_features 2021-01~2025-08 抽樣窗,股數):
# 取代 ws-quant constants.py 手填的 vol_share(無出處,個別值不可重現)。
# 兩種分母並陳——引用時必須說明用哪個。
CLUSTER_VOLUME_SHARE: dict[int, dict[str, float]] = {
    #      佔全體量(含 HQ 席位)   佔分行(dash)量
    0: {"of_total": 0.454, "of_branch": 0.708},
    1: {"of_total": 0.008, "of_branch": 0.012},
    2: {"of_total": 0.062, "of_branch": 0.096},
    3: {"of_total": 0.110, "of_branch": 0.171},
}
# C0+C3(散戶密集)佔全體 56.4% / 佔分行 87.9%。
# 分行整體佔全體 64.1%;HQ(無 dash,75 席位)佔其餘大宗。


def load_clusters(path: Path | None = None) -> pl.DataFrame:
    """讀分群表(broker_name → cluster)。搬遷後正典路徑在本 repo。"""
    p = path or CLUSTERS_PARQUET
    if not p.exists():
        raise FileNotFoundError(
            f"{p} 不存在——執行搬遷:cp ws-quant experiments/broker_taxonomy/"
            f"broker_clusters.parquet → {p}")
    return pl.read_parquet(p).select("broker_name", "cluster")
