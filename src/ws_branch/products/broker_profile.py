"""分點 profile 一頁(席位頁;架構文件 §10.2「O4 改為可追溯的行為產品」、§12 完成標準 3)。

個股讀本回答「這檔股票今天誰在動、動得反不反常」;席位頁回答「**這個席位**
今天做了什麼、它平常長什麼樣、今天的偏離有多少是規模/市況/它自己的」。
四個區塊,每塊都標認識論等級(§4.5 按欄位標,不給整個分點單一身份等級):

1. **raw**(observed):今日規模/廣度/集中度/方向性/籃子延續/市場配置。
2. **性格 / 規模 / 市況 / 特有**(observed 的讀時代理,`measure.state`):
   每個 primitive 並列 今日 / 平常 / 規模效應 / 市況 / 特有 z。
3. **官方配置關聯 + 校準卡**:cos_foreign 等今日值、自身平常、以及**控規模/廣度/
   市場後的殘差在全市場席位中的百分位**(殘差係數只用 ≤ 當日資料估)。旁邊
   永遠附校準卡——Phase 4 證這是「對行政 cohort 的區分力」,**不是**投資人
   身份的 posterior;fund / prop 三桶標 unanchored——**沒有席位真值,無法校準**
   (對外資 cohort 無區分力不等於「已證無辨識力」)。**這是席位頁與個股讀本唯一的差別**:個股頁不顯示 cosine(會被
   讀成「這檔有外資」),席位頁顯示但綁著校準卡。
4. **本子裡最重的股票**(salience 三問,§7):佔本子 / 平常 / 相對 z / 金額 z / 參與率。

頁尾:身份界線 + 描述性觀測非交易訊號。
"""

from __future__ import annotations

import datetime

import polars as pl

from ws_branch.measure import calibration, state, universe
from ws_branch.products.stock_readbook import FOOTER, _num, _pct

PRIMITIVES = ("top5_share", "directional_ratio", "basket_self_sim",
              "cos_market_buy", "cos_market_sell")
PRIM_LABEL = {"top5_share": "集中度 top5", "directional_ratio": "方向性",
              "basket_self_sim": "籃子延續", "cos_market_buy": "配置像市場(買)",
              "cos_market_sell": "配置像市場(賣)"}
CONTROLS = {"buy": ["cos_market_buy", "log_gross", "log_n"],
            "sell": ["cos_market_sell", "log_gross", "log_n"]}

CALIBRATION_CARD = {
    # 來源:findings/v3_phase4_calibration.md §3.5、v3_crossyear_2025.md §4(m2 物化表重跑)。
    # 所有檢定的標籤都是「外資券商 cohort」——fund/prop 三桶**沒有席位真值**,unanchored 的
    # 理由是「無法校準」,不是「已證無辨識力」(2026-09-21 外部審查指正)。
    "measurement_version": "m2-2026-09-21",
    "foreign": {"status": "anchored",
                "text": "殘差 AUC 對外資券商 cohort:年內 買 0.823 / 賣 0.789,跨年 買 0.737 / 賣 0.814;"
                        "逐席位 refit 買 0.27-0.98 / 賣 0.16-0.98(大五家 買 0.88-0.98 / 賣 0.89-0.98,"
                        "小額台 0.63-0.81,大和國泰 買 0.27 / 賣 0.16)。線性控規模/廣度/市場後的"
                        "**行政 cohort 區分力**,很可能低估、非嚴格下界;**不是投資人身份的機率**"},
    "fund": {"status": "unanchored",
             "text": "無投信席位真值,**無法校準**;唯一做過的檢定是對外資券商 cohort 無區分力"
                     "(殘差 AUC ≈0.52)——不能據此說它能或不能辨識投信"},
    "prop_self": {"status": "unanchored",
                  "text": "無自營席位真值,無法校準;對外資 cohort 殘差 AUC 0.40-0.48(不是自營辨識力)"},
    "prop_hedge": {"status": "unanchored",
                   "text": "無席位真值;權證發行商間接證據控規模後僅 +0.05-0.08(AUC 0.59-0.63)——弱"},
}


def residual_percentile(history: pl.DataFrame, *, date: datetime.date, bucket: str,
                        side: str, min_fit: int = 2000) -> pl.DataFrame:
    """cos_<bucket>_<side> 對 [同側 cos_market, log_gross, log_n] 殘差化,係數只用
    date 之前的歷史估(as-of),回傳當日每席位的殘差與百分位(0-1)。樣本不足 → 空表。"""
    target = f"cos_{bucket}_{side}"
    df = history.with_columns(pl.col("gross_amt").log10().alias("log_gross"),
                              pl.col("n_symbols").cast(pl.Float64).log10().alias("log_n"))
    train = df.filter(pl.col("date") < date)
    ok = pl.all_horizontal([pl.col(c).is_not_null() for c in [target, *CONTROLS[side]]])
    train = train.filter(ok)
    if train.height < min_fit:
        return pl.DataFrame({"broker": [], "_resid": [], "_pct": []},
                            schema={"broker": pl.String, "_resid": pl.Float64, "_pct": pl.Float64})
    model = calibration.fit_residual(train, target=target, controls=CONTROLS[side])
    today = calibration.apply_residual(df.filter((pl.col("date") == date) & ok), model, out="_resid")
    return today.select("broker", "_resid",
                        (pl.col("_resid").rank() / pl.len()).alias("_pct"))


def render(
    history: pl.DataFrame, *, broker: str, date: datetime.date,
    cohort_codes: frozenset[str], stock_rows: pl.DataFrame | None = None,
    window: int = 60, min_periods: int = 20, top_n: int = 10,
) -> str:
    """組一頁席位 profile(純函數:history = T4 v3 到 date 為止的全席位切片)。

    stock_rows:該席位當日本子裡的股票(`measure.salience` 輸出:symbol_id +
    stock_gross/salience/sal_mean/salience_z/stock_gross_z/participation_rate/
    denominator_effect);None 時不顯示第 4 區塊。
    """
    w = 76
    lines: list[str] = []
    mine = history.filter(pl.col("broker") == broker).sort("date")
    today = mine.filter(pl.col("date") == date)
    if today.height == 0:
        return "\n".join([f"{broker} — {date}:t4_broker_measure 無此席位日(當日未在普通股 universe 交易)"])
    r = today.row(0, named=True)
    klass = (today.with_columns(universe.seat_class(cohort_codes=cohort_codes).alias("_k"))
             ["_k"][0])
    lines += ["=" * w,
              f"{r['broker_name']}({broker}) — {date} 席位 profile  [{klass}]"
              f"  歷史活躍日 {mine.height}(窗 {window} / 最少 {min_periods})",
              "=" * w]

    # ── 1. raw ──
    lines.append(f"raw(observed):買 {r['gross_buy_amt']/1e8:,.2f} 億 / 賣 {r['gross_sell_amt']/1e8:,.2f} 億 / "
                 f"淨 {r['net_amt']/1e8:+,.2f} 億;碰 {r['n_symbols']} 檔;"
                 f"top1 {_pct(r['top1_share'])} top5 {_pct(r['top5_share'])};"
                 f"方向性 {_num(r['directional_ratio'], '.3f')};籃子延續 {_num(r['basket_self_sim'], '.3f')}")
    lines.append(f"      配置像市場 買 {_num(r['cos_market_buy'], '.3f')} / 賣 {_num(r['cos_market_sell'], '.3f')};"
                 f"官方缺列佔本子 {_pct(r.get('official_missing_share'))};"
                 f"available_at {r['available_at']:%Y-%m-%d %H:%M %Z}(推定:依 A7 落地規則,非逐檔實測)")

    # ── 2. 性格 / 規模 / 市況 / 特有 ──
    lines.append("-" * w)
    lines.append("性格 / 規模 / 市況 / 特有(讀時代理,只用 ≤ 當日資料;非 §6 全樣本分解)")
    lines.append(f"{'primitive':<18}{'今日':>8}{'平常':>8}{'規模':>8}{'市況':>8}{'特有z':>7}{'歷史n':>6}")
    st = state.seat_state(history, date=date, primitives=PRIMITIVES,
                          window=window, min_periods=min_periods)
    srow = st.filter(pl.col("broker") == broker).row(0, named=True)
    for p in PRIMITIVES:
        lines.append(f"{PRIM_LABEL[p]:<18}{_num(srow[p], '.3f'):>8}{_num(srow[f'{p}_trait'], '.3f'):>8}"
                     f"{_num(srow[f'{p}_size'], '+.3f'):>8}{_num(srow[f'{p}_day'], '+.3f'):>8}"
                     f"{_num(srow[f'{p}_state']):>7}{_num(srow[f'{p}_n'], '.0f'):>6}")
    lines.append("      平常 = 自身落後均值;規模 = 今日規模/廣度變化「應該」帶來的偏移;市況 = 扣規模後全市場中位數;")
    lines.append("      特有z = 殘餘偏離 / 自身 sd。『—』= 歷史不足或無變異,無從比較(不是最高異常)。")

    # ── 3. 官方配置關聯 + 校準卡 ──
    lines.append("-" * w)
    lines.append("官方配置關聯(observed 描述量)+ 校準卡(Phase 4;讀 cosine 前先讀卡)")
    lines.append(f"{'桶':<12}{'側':<4}{'今日':>8}{'平常':>8}{'殘差百分位':>10}   狀態")
    hist_mine = mine.filter(pl.col("date") < date).tail(window)
    for bucket, card in CALIBRATION_CARD.items():
        if bucket == "measurement_version":
            continue
        for side in ("buy", "sell"):
            col = f"cos_{bucket}_{side}"
            trait = hist_mine[col].drop_nulls()
            trait_v = trait.mean() if trait.len() >= min_periods else None
            rp = residual_percentile(history, date=date, bucket=bucket, side=side)
            me = rp.filter(pl.col("broker") == broker)
            pct = me["_pct"][0] if me.height else None
            lines.append(f"{bucket:<12}{side:<4}{_num(r[col], '.3f'):>8}{_num(trait_v, '.3f'):>8}"
                         f"{_num(pct, '.2f'):>10}   {card['status']}")
    for bucket, card in CALIBRATION_CARD.items():
        if bucket != "measurement_version":
            lines.append(f"  [{bucket}] {card['text']}")
    lines.append("  殘差百分位 = 控同側 cos_market / log_gross / log_n 後,在當日全部席位中的位置"
                 "(係數只用當日之前的歷史估);『—』= 歷史不足以估係數。")

    # ── 4. 本子裡最重的股票(salience 三問)──
    if stock_rows is not None and stock_rows.height:
        lines.append("-" * w)
        lines.append("本子裡最重的股票(§7 salience 三問;分母 = 席位當日全市場金額)")
        lines.append(f"{'股票':<8}{'金額(億)':>9}{'佔本子':>8}{'平常':>8}{'相對z':>7}{'金額z':>7}{'參與率':>7}")
        for row in stock_rows.sort("stock_gross", descending=True).head(top_n).iter_rows(named=True):
            line = (f"{row['symbol_id']:<8}{row['stock_gross']/1e8:>9.2f}{_pct(row.get('salience')):>8}"
                    f"{_pct(row.get('sal_mean')):>8}{_num(row.get('salience_z')):>7}"
                    f"{_num(row.get('stock_gross_z')):>7}{_num(row.get('participation_rate'), '.2f'):>7}")
            if row.get("denominator_effect"):
                line += "  ← 佔比升但金額未增"
            lines.append(line)

    lines.append("-" * w)
    lines.append("身份界線:本頁沒有任何欄位是投資人身份。cohort 為行政分類(券商執照);"
                 "cosine 與殘差百分位是")
    lines.append("      配置形狀的描述量,已校準的只有「對外資券商 cohort 的區分力」,且很可能低估、非下界。")
    if klass == "外資客戶*":
        lines.append("      *外資客戶:原獨立外資券商併入本土券商後的席位,依券商執照規則不入 cohort。")
    lines.append("=" * w)
    lines.append(FOOTER)
    return "\n".join(lines)
