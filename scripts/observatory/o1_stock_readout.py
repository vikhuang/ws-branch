"""O1「一頁讀本」原型(docs/OBSERVATORY_2026-09.md §2 products/stock_reader.py
的前身;O1 只驗證「user 讀得出東西」,尚未做成正式 parquet 產品)。

給一檔股票、一個交易日,把 T1(誰在買賣)接 T4 v2(這個分點今天像什麼)
組成人類可讀的 terminal 輸出:誰在買、像不像外資的錢、決策者集中還是
分散。純描述,頁尾固定註腳(觀測站家法 §6)。

一次性分析腳本,非套件模組;不寫回任何表。
"""

from __future__ import annotations

import argparse
import datetime

import polars as pl

from ws_branch.measure.identity import classify_broker_cohort
from ws_branch.tables import io

_COHORT_LABEL = {
    "institutional": "外資/法人",
    "prop_shell": "自營殼",
    "mixed": "法人混合(HQ)",
    "retail": "分行",
    "unknown": "未知",
}
# cohort 標籤的完整語意(讀本頁尾附註用):
_COHORT_NOTE = (
    "cohort:外資/法人=名字含外資/法人特徵;自營殼=名字含「-自營」,僅 0.06% 全量,"
    "不代表自營流量;法人混合(HQ)=無 dash 總公司席位,投信/自營/本土法人混合通道;"
    "分行=一般分行(散戶通道)。"
)


def _multiplicity_label(m: float | None) -> str:
    if m is None:
        return "資料不足"
    if m < 0.35:
        return "單一意志(集中/一致/像昨天)"
    if m < 0.65:
        return "中等——介於決策者集中與分散之間"
    return "人群(分散/混合方向/籃子多變)"


def readout(symbol_id: str, date: datetime.date, top_n: int = 15) -> None:
    t1_day = (io.scan("t1_broker_daily", start=str(date), end=str(date))
              .filter(pl.col("symbol_id") == symbol_id)
              .collect())
    if t1_day.height == 0:
        print(f"{symbol_id} @ {date}:t1_broker_daily 無資料")
        return
    t4_day = (io.scan("t4_broker_features", start=str(date), end=str(date))
              .collect())

    merged = (t1_day
              .with_columns((pl.col("buy_dollar") + pl.col("sell_dollar")).alias("gross_this_stock"),
                            (pl.col("buy_dollar") - pl.col("sell_dollar")).alias("net_this_stock"))
              .join(t4_day, on=["broker", "broker_name", "date"], how="left", suffix="_all")
              .with_columns(
                  pl.col("broker_name")
                  .map_elements(classify_broker_cohort, return_dtype=pl.String)
                  .alias("cohort"))
              .sort("gross_this_stock", descending=True))

    total_gross = merged["gross_this_stock"].sum()
    total_net = merged["net_this_stock"].sum()
    t3_day = (io.scan("t3_official_daily", start=str(date), end=str(date))
              .filter(pl.col("symbol_id") == symbol_id).collect())

    print("=" * 78)
    print(f"{symbol_id} — {date} 一頁讀本(O1 原型,觀測站家法:純描述,非交易訊號)")
    print("=" * 78)
    print(f"當日碰過此股的分點數:{merged.height}  gross 總額:{total_gross:,.0f} 元"
          f"  淨額:{total_net:+,.0f} 元({'偏買' if total_net > 0 else '偏賣'})")
    if t3_day.height:
        r = t3_day.row(0, named=True)
        print(f"官方三大法人:外資淨 {r['foreign_net_sh']:+,.0f} 股 | "
              f"投信淨 {r['fund_net_sh']:+,.0f} 股 | "
              f"當沖佔比 {r['day_trade_pct']:.1f}%")
    print("-" * 78)
    print(f"{'分點':<14}{'cohort':<12}{'gross(佔比)':>14}{'方向':>6}"
          f"{'多重性':>8}{'像外資買':>9}{'像外資賣':>9}{'像投信買':>9}{'今日vs昨日籃':>10}")
    print("-" * 78)

    def _num(v: float | None, fmt: str) -> str:
        return "" if v is None else format(v, fmt)

    top_rows = merged.head(top_n)
    for row in top_rows.iter_rows(named=True):
        share = row["gross_this_stock"] / total_gross if total_gross else 0.0
        direction = "買" if row["net_this_stock"] > 0 else "賣"
        cohort = _COHORT_LABEL.get(row["cohort"], row["cohort"])
        print(f"{row['broker_name']:<14}{cohort:<12}"
              f"{row['gross_this_stock']:>9,.0f}({share:>4.0%})"
              f"{direction:>5}"
              f"{_num(row.get('multiplicity'), '.2f'):>8}"
              f"{_num(row.get('foreign_sim_buy'), '+.2f'):>9}"
              f"{_num(row.get('foreign_sim_sell'), '+.2f'):>9}"
              f"{_num(row.get('fund_sim_buy'), '+.2f'):>9}"
              f"{_num(row.get('basket_self_sim'), '.2f'):>10}")
    print("-" * 78)
    print("重倉前 3 名解讀:")
    for row in top_rows.head(3).iter_rows(named=True):
        print(f"  {row['broker_name']}({_COHORT_LABEL.get(row['cohort'], row['cohort'])})"
              f":{_multiplicity_label(row.get('multiplicity'))}")
    print("多重性解讀:低=單一意志(集中/一致方向/籃子跟昨天像),高=人群"
          "(分散/混合方向/籃子每天換)。像外資買/賣、像投信買 = 該分點今天")
    print("加碼/減碼的股票組合跟官方外資/投信當日買超/賣超組合的 rank "
          "correlation(<5 檔購物籃不計算,顯示空白)。")
    print(_COHORT_NOTE)
    print("=" * 78)
    print("本頁為描述性觀測,非交易訊號。(docs/OBSERVATORY_2026-09.md §6)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("symbol_id")
    ap.add_argument("date", help="YYYY-MM-DD")
    ap.add_argument("--top", type=int, default=15)
    args = ap.parse_args()
    readout(args.symbol_id, datetime.date.fromisoformat(args.date), args.top)
