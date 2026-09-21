"""個股一頁讀本 v1:raw + 會計界限(架構文件 §11 步驟 B 的交付物)。

與 O1 原型(`scripts/observatory/o1_stock_readout.py`)的三處差別,每處都是
Phase 1-2 實證逼出來的:

1. **不顯示排名版指紋**。O1 的 foreign_sim 扣掉「像市場」後對外資 cohort
   的區分力剩 AUC 0.53(findings/o1_fingerprint_read.md),它量的是廣度不是
   身分。v3 的金額 cosine 尚未進表(Phase 4 定稿),故本版**不顯示任何
   actor 相似度**——寧可少一欄,不放一個會被讀成身分的數字。
2. **顯示會計界限**:官方外資量有多少**必須**在外資席位之外(model-free
   下界),以及未屬四官方桶的餘額。這是目前唯一 identified/bounded 等級的
   actor 資訊。
3. **席位層級只顯示 raw 與佔比**,不顯示 trait/state——Phase 2 證明 state
   需要滾動 baseline(ε 自相關 0.16-0.27),滾動版尚未進表。

頁尾固定註腳:描述性觀測,非交易訊號(觀測站家法 §6)。
"""

from __future__ import annotations

import datetime

import polars as pl

FOOTER = "本頁為描述性觀測,非交易訊號。(docs/OBSERVATORY_2026-09.md §6)"


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
) -> str:
    """組一頁讀本(純函數:資料由呼叫端載入,回傳字串不印)。

    t1_day:該股該日的 broker/broker_name/buy_sh/sell_sh/buy_dollar/sell_dollar
    bounds_day:t3b 該股該日兩側
    t3_day:T3 該股該日一列
    """
    lines: list[str] = []
    w = 76
    lines.append("=" * w)
    lines.append(f"{symbol_id} — {date} 一頁讀本 v1(raw + 會計界限)")
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
    lines.append(f"{'席位':<16}{'cohort':<10}{'買(張)':>10}{'賣(張)':>10}"
                 f"{'淨(張)':>10}{'佔此股':>8}{'來回率':>8}")
    lines.append("-" * w)
    d = (t1_day.with_columns(
        (pl.col("buy_sh") + pl.col("sell_sh")).alias("_g"),
        (pl.col("buy_sh") - pl.col("sell_sh")).alias("_net"),
        pl.when(pl.col("broker").is_in(list(cohort_codes)))
        .then(pl.lit("外資席位")).otherwise(
            pl.when(pl.col("broker_name").str.contains("-"))
            .then(pl.lit("分行")).otherwise(pl.lit("總公司")))
        .alias("_cohort"))
        .with_columns(
            pl.when(pl.max_horizontal("buy_sh", "sell_sh") > 0)
            .then(pl.min_horizontal("buy_sh", "sell_sh")
                  / pl.max_horizontal("buy_sh", "sell_sh"))
            .otherwise(None).alias("_two"))  # 兩側皆零 → null,不是 nan(§7)
        .sort("_g", descending=True))
    gross_all = d["_g"].sum()
    for row in d.head(top_n).iter_rows(named=True):
        two = "—" if row["_two"] is None else f"{row['_two']:.2f}"
        lines.append(
            f"{row['broker_name']:<16}{row['_cohort']:<10}"
            f"{_lots(row['buy_sh']):>10}{_lots(row['sell_sh']):>10}"
            f"{_lots(row['_net'], signed=True):>10}"
            f"{row['_g'] / gross_all * 100:>7.1f}%{two:>8}")

    lines.append("-" * w)
    lines.append("讀法:『必須在外資席位之外』為非負性推得的**硬下界**,"
                 "不依賴任何行為模型;")
    lines.append("      『來回率』= min(買,賣)/max(買,賣),描述雙邊流量,"
                 "**不等於當沖**——同一席位的")
    lines.append("      買方與賣方可能是不同客戶;cohort 為行政分類,不是投資人身分。")
    lines.append("=" * w)
    lines.append(FOOTER)
    return "\n".join(lines)
