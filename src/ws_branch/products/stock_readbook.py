"""個股一頁讀本:raw + 會計界限 + salience 三問 + 席位性格/市況/狀態(§11 B→E)。

與 O1 原型(`scripts/observatory/o1_stock_readout.py`)的三處差別,每處都是
Phase 1-2 實證逼出來的:

1. **不顯示 actor 相似度**。O1 的 rank 版指紋量的是廣度不是身分(AUC 0.53);
   v3 的金額 cosine 已進 `t4_broker_measure`,但 Phase 4 證它只是「線性控規模後
   的行政 cohort 區分力」,不是身分——個股讀本裡任何一個「像外資 0.83」的數字
   都會被讀成身分,故**仍不顯示**;席位層的 cosine 留給分點 profile 並附校準卡。
2. **顯示會計界限**:官方外資量有多少**必須**在外資席位之外(model-free
   下界),以及未屬四官方桶的餘額。這是目前唯一 identified/bounded 等級的
   actor 資訊。
3. **席位層級顯示 raw + salience 三問**(Phase 3 起)。§7 明令三個問題分開:
   持續參與(參與率)/ 相對重要性上升(salience 對自身基線)/ 絕對流量增加
   (該股金額對自身基線)。**salience 永不單獨呈現**——Phase 3 實證
   salience_z ∈ [0, 0.5) 的「微幅變重要」有 18.1% 純粹是席位自己縮量
   (分母效應),z ≥ 1 才幾乎總有真實金額撐起。

4. **席位性格 / 規模效應 / 市況 / 狀態分開讀**(Step E,`measure.state`):對集中度
   與方向性兩個 primitive,並列 今日 raw / 平常(自身 60 活躍日落後均值)/ 規模
   (今日規模與廣度變化在橫斷面上「應該」帶來的偏移)/ 市況(扣掉規模後所有席位
   偏離的中位數)/ 特有 z(殘餘偏離對自身 sd)。§12 完成標準 3:讀本要能區分
   raw、trait、day effect、state;**這些是讀時代理,不是 §6 的全樣本分解**。

頁尾固定註腳:描述性觀測,非交易訊號(觀測站家法 §6)。
"""

from __future__ import annotations

import datetime

import polars as pl

from ws_branch.measure import universe

FOOTER = "本頁為描述性觀測,非交易訊號。(docs/OBSERVATORY_2026-09.md §6)"


def _pct(x: float | None) -> str:
    return "—" if x is None else f"{x * 100:.2f}%"


def _num(x: float | None, fmt: str = "+.1f") -> str:
    return "—" if x is None else format(x, fmt)


def _lots(sh: float | None, signed: bool = False) -> str:
    if sh is None:
        return "—"
    return f"{sh / 1000:+,.0f}" if signed else f"{sh / 1000:,.0f}"


def render(
    t1_day: pl.DataFrame,
    bounds_day: pl.DataFrame,
    t3_day: pl.DataFrame,
    *,
    symbol_id: str,
    date: datetime.date,
    cohort_codes: frozenset[str],
    top_n: int = 12,
    salience_day: pl.DataFrame | None = None,
    seat_state_day: pl.DataFrame | None = None,
) -> str:
    """組一頁讀本(純函數:資料由呼叫端載入,回傳字串不印)。

    t1_day:該股該日的 broker/broker_name/buy_sh/sell_sh/buy_dollar/sell_dollar
    bounds_day:t3b 該股該日兩側
    t3_day:T3 該股該日一列
    salience_day:`measure.salience` 的當日輸出(broker + salience/sal_mean/
    salience_z/stock_gross_z/participation_rate/denominator_effect);None 時
    只顯示 raw 欄位。
    seat_state_day:`measure.state.seat_state` 的當日輸出(broker + 每個 primitive
    的 raw/_trait/_day/_state/_n);None 時不顯示性格/狀態區塊。
    """
    lines: list[str] = []
    w = 76
    lines.append("=" * w)
    has_state = seat_state_day is not None and seat_state_day.height > 0
    lines.append(f"{symbol_id} — {date} 一頁讀本(raw + 會計界限"
                 + (" + salience 三問" if salience_day is not None and salience_day.height else "")
                 + (" + 席位性格/市況/狀態" if has_state else "") + ")")
    lines.append("=" * w)

    if t1_day.height == 0:
        lines.append("t1_broker_daily 無此股日資料")
        return "\n".join(lines)

    tot_b, tot_s = t1_day["buy_sh"].sum(), t1_day["sell_sh"].sum()
    lines.append(f"分點成交:買 {_lots(tot_b)} 張 / 賣 {_lots(tot_s)} 張;"
                 f"碰過此股的席位 {t1_day.height}")

    if t3_day.height:
        r = t3_day.row(0, named=True)
        lines.append(
            f"官方四桶(張):外資 買{_lots(r['foreign_buy_sh'])}/賣{_lots(r['foreign_sell_sh'])} | "
            f"投信 買{_lots(r['fund_buy_sh'])}/賣{_lots(r['fund_sell_sh'])} | "
            f"自營自行 買{_lots(r['prop_self_buy_sh'])}/賣{_lots(r['prop_self_sell_sh'])} | "
            f"自營避險 買{_lots(r['prop_hedge_buy_sh'])}/賣{_lots(r['prop_hedge_sell_sh'])}")
        lines.append(f"當沖佔比 {r['day_trade_pct']:.1f}%")

    # ── 會計界限(唯一 identified/bounded 等級的 actor 資訊)──
    lines.append("-" * w)
    pub = bounds_day.filter(pl.col("bounds_publishable"))
    if pub.height == 0:
        lines.append("會計界限:本日不可發布(輸入缺值/不自洽/TEJ vol 異常,見 audit_ledger A6)")
    else:
        for row in pub.sort("side").iter_rows(named=True):  # buy 先於 sell
            side = "買方" if row["side"] == "buy" else "賣方"
            other_pct = (row["other_actor_sh"] / row["market_total_sh"] * 100
                         if row["market_total_sh"] else float("nan"))
            lines.append(
                f"{side}:官方外資 {_lots(row['official_foreign_sh'])} 張,其中"
                f"**至少 {_lots(row['foreign_y_lo'])} 張必須在 {len(cohort_codes)} 家外資席位之外**"
                f"(席位內外資量界限 {_lots(row['foreign_x_lo'])}~"
                f"{_lots(row['foreign_x_hi'])} 張);"
                f"未屬四官方桶的餘額 {other_pct:.0f}%")

    # ── 席位 raw(不含 trait/state,不含 actor 相似度)──
    lines.append("-" * w)
    has_sal = salience_day is not None and salience_day.height > 0
    head = (f"{'席位':<16}{'cohort':<10}{'買(張)':>10}{'賣(張)':>10}"
            f"{'淨(張)':>10}{'佔此股':>8}{'來回率':>8}")
    if has_sal:
        head += f"{'佔席位本子':>11}{'(平常)':>9}{'相對z':>7}{'金額z':>7}{'參與率':>7}"
    lines.append(head)
    lines.append("-" * w)
    d = (t1_day.with_columns(
        (pl.col("buy_sh") + pl.col("sell_sh")).alias("_g"),
        (pl.col("buy_sh") - pl.col("sell_sh")).alias("_net"),
        # 外資客戶*(9A81):明知客戶以外資為主但不在 cohort,其量在「必須在外資
        # 席位之外」的下界裡,不能混進總公司/分行(universe.seat_class 共用)
        universe.seat_class(cohort_codes=cohort_codes).alias("_cohort"))
        .with_columns(
            pl.when(pl.max_horizontal("buy_sh", "sell_sh") > 0)
            .then(pl.min_horizontal("buy_sh", "sell_sh")
                  / pl.max_horizontal("buy_sh", "sell_sh"))
            .otherwise(None).alias("_two"))  # 兩側皆零 → null,不是 nan(§7)
        .sort("_g", descending=True))
    if has_sal:
        d = d.join(salience_day, on="broker", how="left")
    gross_all = d["_g"].sum()
    for row in d.head(top_n).iter_rows(named=True):
        two = "—" if row["_two"] is None else f"{row['_two']:.2f}"
        line = (f"{row['broker_name']:<16}{row['_cohort']:<10}"
                f"{_lots(row['buy_sh']):>10}{_lots(row['sell_sh']):>10}"
                f"{_lots(row['_net'], signed=True):>10}"
                f"{row['_g'] / gross_all * 100:>7.1f}%{two:>8}")
        if has_sal:
            line += (f"{_pct(row.get('salience')):>11}"
                     f"{_pct(row.get('sal_mean')):>9}"
                     f"{_num(row.get('salience_z')):>7}"
                     f"{_num(row.get('stock_gross_z')):>7}"
                     f"{_num(row.get('participation_rate'), '.2f'):>7}")
            if row.get("denominator_effect"):
                line += "  ← 佔比升但金額未增(席位本子縮水)"
        lines.append(line)

    if has_state:
        lines.append("-" * w)
        lines.append("席位性格 / 市況 / 狀態(§6;只用 ≤ 當日資料)"
                     "         集中度 top5_share            方向性 directional_ratio")
        lines.append(f"{'席位':<16}{'今日':>8}{'平常':>8}{'規模':>8}{'市況':>8}{'特有z':>7}"
                     f"   {'今日':>8}{'平常':>8}{'規模':>8}{'市況':>8}{'特有z':>7}{'歷史n':>6}")
        st = d.head(top_n).select("broker", "broker_name").join(seat_state_day, on="broker", how="left")
        for row in st.iter_rows(named=True):
            line = f"{row['broker_name']:<16}"
            for p in ("top5_share", "directional_ratio"):
                line += (f"{_num(row.get(p), '.3f'):>8}{_num(row.get(f'{p}_trait'), '.3f'):>8}"
                         f"{_num(row.get(f'{p}_size'), '+.3f'):>8}{_num(row.get(f'{p}_day'), '+.3f'):>8}"
                         f"{_num(row.get(f'{p}_state')):>7}   ")
            line += f"{_num(row.get('top5_share_n'), '.0f'):>4}"
            lines.append(line)
        lines.append("      『平常』= 該席位自身最近 60 個活躍日的落後均值(不含今日);"
                     "『規模』= 它今天做大/做小、碰多/碰少")
        lines.append("      「應該」帶來的偏移(當日橫斷面對規模與廣度變化的迴歸);"
                     "『市況』= 扣掉規模後所有席位偏離的中位數;")
        lines.append("      『特有z』= (今日−平常−規模−市況)/自身 sd——相對自身、規模與市況的殘餘偏離,"
                     "是讀時代理,不是 §6 的全樣本分解。")
        lines.append("      『—』= 歷史不足 20 個活躍日或自身無變異,無從比較(不是最高異常)。")

    lines.append("-" * w)
    if has_sal:
        lines.append("salience 三問(§7 明令分開讀):『佔席位本子』= 這檔佔該席位"
                     "當日全市場金額的比例;")
        lines.append("      『相對z』= 佔比對自身 60 活躍日基線的偏離;"
                     "『金額z』= 該股絕對金額對自身基線的偏離。")
        lines.append("      **兩者分歧才是重點**:相對高而金額不高 = 這檔在它"
                     "本子裡變重要,但錢沒真的變多;")
        lines.append("      z 為『—』= 過去 60 個活躍日從沒碰過,無從比較"
                     "(不是資料缺失,看參與率)。")
    lines.append("讀法:『必須在外資席位之外』為非負性推得的**硬下界**,"
                 "不依賴任何行為模型;")
    lines.append("      『來回率』= min(買,賣)/max(買,賣),描述雙邊流量,"
                 "**不等於當沖**——同一席位的")
    lines.append("      買方與賣方可能是不同客戶;cohort 為行政分類,不是投資人身分。")
    if d.filter(pl.col("_cohort") == "外資客戶*").height:
        lines.append("      *外資客戶:原獨立外資券商併入本土券商後的席位(如永豐金-匯立),"
                     "依「券商執照」規則不入 cohort,其量計入上述下界。")
    lines.append("=" * w)
    lines.append(FOOTER)
    return "\n".join(lines)
