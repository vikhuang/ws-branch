"""校準卡(純資料):Phase 4 對四個官方桶 cosine 的校準結論,profile 與 T4 v3 manifest 共用。

數字來源:findings/v3_phase4_calibration.md §3.5、v3_crossyear_2025.md §4。
資料版本 2026-09-22(A9 補拉後:T3 2025–2026 168 檔缺列已補,official_missing_share p90 = 0)。
改數字只改這裡;改公式要升 t4_broker_measure.MEASUREMENT_VERSION。
"""

from __future__ import annotations

from typing import Final

DATA_VERSION: Final = "2026-09-22-a9"

CALIBRATION_CARD: Final[dict[str, dict[str, str]]] = {
    # 所有檢定的標籤都是「外資券商 cohort」——fund/prop 三桶**沒有席位真值**,unanchored 的
    # 理由是「無法校準」,不是「已證無辨識力」(2026-09-21 外部審查指正)。
    "foreign": {"status": "anchored",
                "text": "殘差 AUC 對外資券商 cohort:年內 買 0.824 / 賣 0.794,跨年 買 0.748 / 賣 0.823;"
                        "逐席位 refit 買 0.29-0.98 / 賣 0.18-0.98(大五家 0.88-0.98,小額台 0.65-0.82,"
                        "大和國泰 買 0.29 / 賣 0.18)。線性控規模/廣度/市場後的**行政 cohort 區分力**,"
                        "很可能低估、非嚴格下界;**不是投資人身份的機率**"},
    "fund": {"status": "unanchored",
             "text": "無投信席位真值,**無法校準**;唯一做過的檢定是對外資券商 cohort 無區分力"
                     "(殘差 AUC ≈0.52)——不能據此說它能或不能辨識投信"},
    "prop_self": {"status": "unanchored",
                  "text": "無自營席位真值,無法校準;對外資 cohort 殘差 AUC 0.40-0.47(不是自營辨識力)"},
    "prop_hedge": {"status": "unanchored",
                   "text": "無席位真值;權證發行商間接證據控規模後僅 +0.05-0.08(AUC 0.59-0.62)——弱"},
}
