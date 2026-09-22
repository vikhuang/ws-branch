"""行動者狀態純函數(L-A,docs/OBSERVATORY_2026-09.md §1):決策者多重性、
籃子指紋(自相似 + 官方對帳)、產業集中度、當沖幫關聯度。

零 IO——所有輸入(T1/T3 切片、ws-core tickers)由呼叫端(transforms 層)
載入後傳入。所有函數皆為描述性量測,不做任何預測性宣稱(觀測站家法
§6:這一期的欄位只回答「看得懂什麼」,不回答「能不能賺」)。
"""

from __future__ import annotations

import polars as pl

_MIN_SUPPORT = 5  # 分點日購物籃 < 5 檔時,rank correlation 統計上不穩定,標 null


def basket_self_similarity(t1_slice: pl.DataFrame) -> pl.DataFrame:
    """分點購物籃 today vs 前一個「有資料的交易日」cosine similarity
    (gross 金額加權向量:分點今天在每檔股票上的 buy+sell 金額)。

    「前一交易日」由資料本身的日期集合推導(shift(1)),不查行事曆——
    台股周末/國定假日會讓「昨天」落在好幾個月曆天前,尤其連假後第一天。
    分點若當天沒交易或前一交易日沒交易,basket_self_sim 為 null:不得
    補 0——0 代表「今天跟昨天完全不像」,「沒得比」是另一種缺失狀態,
    兩者混淆會把「新分點/久未交易分點」誤讀成「高度人群化」。

    呼叫端須在 t1_slice 多帶一段緩衝日期,月初的「昨天」才不會被切掉。

    Returns: broker, date, basket_self_sim(∈[0,1],金額皆非負)
    """
    basket = (
        t1_slice.with_columns(
            (pl.col("buy_dollar") + pl.col("sell_dollar")).alias("gross")
        )
        .filter(pl.col("gross") > 0)
        .select("broker", "symbol_id", "date", "gross")
    )
    if basket.height == 0:
        return pl.DataFrame(
            schema={"broker": pl.String, "date": pl.Date, "basket_self_sim": pl.Float64}
        )
    date_map = (
        basket.select("date").unique().sort("date")
        .with_columns(pl.col("date").shift(1).alias("prev_date"))
    )
    norms = basket.group_by("broker", "date").agg(
        (pl.col("gross") ** 2).sum().sqrt().alias("norm")
    )
    with_prev_date = basket.join(date_map, on="date")
    prev_basket = basket.rename({"date": "prev_date", "gross": "gross_prev"})
    dot = (
        with_prev_date.join(
            prev_basket, on=["broker", "symbol_id", "prev_date"], how="inner"
        )
        .group_by("broker", "date")
        .agg((pl.col("gross") * pl.col("gross_prev")).sum().alias("dot"))
    )
    result = (
        dot.join(norms, on=["broker", "date"])
        .join(date_map, on="date")
        .join(
            norms.rename({"date": "prev_date", "norm": "norm_prev"}),
            on=["broker", "prev_date"],
        )
    )
    return result.with_columns(
        (pl.col("dot") / (pl.col("norm") * pl.col("norm_prev"))).alias(
            "basket_self_sim"
        )
    ).select("broker", "date", "basket_self_sim")


def _spearman_by_broker_day(df: pl.DataFrame, x: str, y: str, out: str) -> pl.DataFrame:
    """群組內(broker,date)rank correlation(= Spearman,經 pl.corr(rank,rank))。

    < _MIN_SUPPORT 檔的購物籃標 null,不硬湊數字。
    """
    ranked = df.with_columns(
        pl.col(x).rank(method="average").over("broker", "date").alias("_rx"),
        pl.col(y).rank(method="average").over("broker", "date").alias("_ry"),
    )
    return (
        ranked.group_by("broker", "date")
        .agg(pl.len().alias("_n"), pl.corr("_rx", "_ry").alias("_rho"))
        .with_columns(
            pl.when(pl.col("_n") >= _MIN_SUPPORT)
            .then(pl.col("_rho").fill_nan(None))
            .otherwise(None)
            .alias(out)
        )
        .select("broker", "date", out)
    )


def identity_similarity(t1_slice: pl.DataFrame, t3_slice: pl.DataFrame) -> pl.DataFrame:
    """分點日購物籃 vs 官方外資/投信日向量的 rank correlation(買/賣分開算,
    不合併成淨額——買超股票組合跟賣超股票組合是兩個獨立的「像不像」問題)。

    比對範圍 = 分點當日**實際碰過**的股票支撐集合(買向量比外資/投信買
    向量、賣向量比賣向量),不是零填滿全市場(~2000 檔)的向量。理由:
    多數分點日僅碰幾十檔,若零填滿,rank correlation 會被上千個「雙方
    皆零」的並列排名(tie)主導,稀釋掉真正有資訊的少數非零項——這裡要
    回答的問題是「這個分點今天加碼的股票,跟外資/投信加碼的股票是不是
    同一批」,限定在分點實際觸碰的支撐集合上更能回答這個問題,計算量
    也從 O(分點數×2000) 降到 O(分點數×平均籃子大小)。

    信賴帶(U3 實證,見 measure/identity.py docstring):foreign_sim 錨
    corr 0.973(高信賴);fund_sim 錨僅 0.58(投信分點指紋先天糊,信賴帶
    標 low,下游呈現須降權)。自營無分點錨(官方數字無法對到分點層級),
    本表不產出 prop_sim。

    Returns: broker, date, foreign_sim_buy, foreign_sim_sell, fund_sim_buy,
    fund_sim_sell, foreign_sim_confidence, fund_sim_confidence
    """
    buy = t1_slice.filter(pl.col("buy_dollar") > 0).join(
        t3_slice.select("symbol_id", "date", "foreign_buy_sh", "fund_buy_sh"),
        on=["symbol_id", "date"],
        how="inner",
    )
    sell = t1_slice.filter(pl.col("sell_dollar") > 0).join(
        t3_slice.select("symbol_id", "date", "foreign_sell_sh", "fund_sell_sh"),
        on=["symbol_id", "date"],
        how="inner",
    )
    parts = [
        _spearman_by_broker_day(buy, "buy_dollar", "foreign_buy_sh", "foreign_sim_buy"),
        _spearman_by_broker_day(sell, "sell_dollar", "foreign_sell_sh", "foreign_sim_sell"),
        _spearman_by_broker_day(buy, "buy_dollar", "fund_buy_sh", "fund_sim_buy"),
        _spearman_by_broker_day(sell, "sell_dollar", "fund_sell_sh", "fund_sim_sell"),
    ]
    out = parts[0]
    for p in parts[1:]:
        out = out.join(p, on=["broker", "date"], how="full", coalesce=True)
    return out.with_columns(
        pl.lit("high").alias("foreign_sim_confidence"),
        pl.lit("low").alias("fund_sim_confidence"),
    )


def sector_concentration(t1_slice: pl.DataFrame, tickers: pl.DataFrame) -> pl.DataFrame:
    """分點日產業集中度(HHI)+ 主力產業(產業碼來源:ws-core tickers
    industry_local,TEJ 產業碼粗——接受此限制,不做更細的自建分類)。

    tickers: symbol_id, industry_local。查無產業碼的股票(新股/尚未收錄/
    ETF)歸「未知」桶,不得靜默丟棄(家法#3)。

    Returns: broker, date, sector_hhi(∈(0,1]), top_sector
    """
    g = (
        t1_slice.with_columns(
            (pl.col("buy_dollar") + pl.col("sell_dollar")).alias("gross")
        )
        .filter(pl.col("gross") > 0)
        .join(tickers, on="symbol_id", how="left")
        .with_columns(pl.col("industry_local").fill_null("未知"))
    )
    by_sector = g.group_by("broker", "date", "industry_local").agg(
        pl.col("gross").sum().alias("sector_gross")
    )
    totals = by_sector.group_by("broker", "date").agg(
        pl.col("sector_gross").sum().alias("total_gross")
    )
    shares = by_sector.join(totals, on=["broker", "date"]).with_columns(
        (pl.col("sector_gross") / pl.col("total_gross")).alias("share")
    )
    hhi = shares.group_by("broker", "date").agg(
        (pl.col("share") ** 2).sum().alias("sector_hhi")
    )
    top = (
        shares.sort("share", descending=True)
        .group_by("broker", "date", maintain_order=True)
        .first()
        .select("broker", "date", pl.col("industry_local").alias("top_sector"))
    )
    return hhi.join(top, on=["broker", "date"])


def daytrade_association(t1_slice: pl.DataFrame, t3_slice: pl.DataFrame) -> pl.DataFrame:
    """分點日沖幫關聯度:所碰股票的 T3 day_trade_pct 以 gross 金額加權平均。

    高值 = 今天碰的股票普遍是市場當沖熱門股(區分當沖/隔日沖幫 vs 現貨
    散戶櫃檯的行為特徵);T3 查無 day_trade_pct 的股票(新股)直接不計入
    加權平均(分母也扣掉),不得用 0 填——0 會系統性低估關聯度。

    Returns: broker, date, daytrade_assoc(∈[0,100],沿用 T3 day_trade_pct 的
    百分比尺度)
    """
    g = (
        t1_slice.with_columns(
            (pl.col("buy_dollar") + pl.col("sell_dollar")).alias("gross")
        )
        .filter(pl.col("gross") > 0)
        .join(
            t3_slice.select("symbol_id", "date", "day_trade_pct"),
            on=["symbol_id", "date"],
            how="inner",
        )
    )
    return g.group_by("broker", "date").agg(
        (
            (pl.col("gross") * pl.col("day_trade_pct")).sum() / pl.col("gross").sum()
        ).alias("daytrade_assoc")
    )


def compute_multiplicity(daily: pl.DataFrame) -> pl.DataFrame:
    """決策者多重性 = 1 − mean(方向一致度, 集中度, 購物籃穩定度)。

    三分量各自已 ∈[0,1],且各自獨立指向同一件事「背後是不是同一個腦袋」:
    - directional_ratio(v1 既有):今天買賣是不是同一個方向,一致=單一意志
    - top5_share(v1 既有):火力是不是集中在少數股票,集中=決策收斂
    - basket_self_sim(本期新增):今天的籃子像不像昨天,穩定=同一批人
      每天做同樣的事(機構台/專營戶),善變=每天換一批人上門(散戶櫃檯)

    三者取平均而非相乘,理由:任一分量單獨偏低不該把 multiplicity 直接
    推到頂(例如集中度低但方向與昨天籃子都高度一致,仍應判定偏低多重性,
    而非因為單一弱項就整體判人群化)——相乘對單一低分量過度敏感。

    basket_self_sim 在分點首個活躍日或前一交易日缺交易時為 null,此時
    退回只用另外兩分量的平均,不得靜默補 0(0 會誤讀成「跟昨天完全不像」
    而把新分點/久未活動分點誤判成人群化櫃檯)。

    直接用 `pl.mean_horizontal` 對三欄取平均會有陷阱:它會**靜默跳過
    null**(等同 nanmean),不是我們要的「basket_self_sim 缺值時明確退回
    兩分量」。directional_ratio/top5_share 兩者目前總是同時為 null(都
    由 compute_broker_day 的 gross_amt==0 觸發),此時 mean_horizontal
    對三個 null 取平均會正確回傳 null——但這是**兩個獨立判斷剛好同步的
    巧合**,不是設計保證;之後任何一邊的 null 觸發條件被改動,
    mean_horizontal 會在不知不覺間換成用剩下的 1-2 個分量硬算,不會報錯
    也不會留痕跡。因此這裡改用明確的三段式 when/then,把「三分量都在→
    三分量平均」「directional_ratio/top5_share 在但 basket_self_sim 缺→
    兩分量平均」「directional_ratio/top5_share 缺→null」三種狀態各自
    寫死,不依賴 mean_horizontal 的隱式跳過行為。

    Returns: 原欄位 + multiplicity(∈[0,1])
    """
    core_present = (
        pl.col("directional_ratio").is_not_null() & pl.col("top5_share").is_not_null()
    )
    unity = (
        pl.when(~core_present)
        .then(None)
        .when(pl.col("basket_self_sim").is_not_null())
        .then(
            (pl.col("directional_ratio") + pl.col("top5_share")
             + pl.col("basket_self_sim")) / 3
        )
        .otherwise((pl.col("directional_ratio") + pl.col("top5_share")) / 2)
    )
    return daily.with_columns((1 - unity).alias("multiplicity"))


def rolling_identity_similarity(
    daily: pl.DataFrame, windows: tuple[int, ...] = (20, 60)
) -> pl.DataFrame:
    """身分相似度的滾動窗版本:分點自己最近 N 個**活躍交易日**的平均
    (不是月曆天——散戶櫃檯出勤不規律,月曆窗會被缺勤日稀釋)。

    暖機期(前 < window 個活躍日)用不足窗口筆數的平均值(min_samples=1),
    越前段可信度越低,下游呈現時須連同活躍日數一併揭露。250D 版留待
    之後(規格 §2);本函數服務 T4 v2(frozen),每年開頭幾筆因看不到前一年的
    活躍日而暖機不足是 v2 的已知限制;v3 `t4_broker_measure._inputs` 已取前一年
    12 月起的資料,不在此函數內偽造前情。
    """
    daily = daily.sort("broker", "date")
    cols = ["foreign_sim_buy", "foreign_sim_sell", "fund_sim_buy", "fund_sim_sell"]
    exprs = [
        pl.col(c)
        .rolling_mean(window_size=w, min_samples=1)
        .over("broker")
        .alias(f"{c}_{w}d")
        for w in windows
        for c in cols
    ]
    return daily.with_columns(exprs)
