"""Stock salience 與局部流量(純函數,零 IO)。

架構文件 §7。回答三個**必須分開**的問題:

1. 持續參與?        `participation_rate` —— 窗內有交易的日子佔比
2. 相對重要性上升?  `salience_z` —— salience 對自身歷史的偏離
3. 絕對流量增加?    `stock_gross_z` —— 該股金額對自身歷史的偏離

第 2 與第 3 不可互相取代:**salience 的分母會動**。席位在其他股票縮量,
salience 一樣會上升——規格明令 salience 上升不得直接稱為新增資金。

零不是缺值(本模組的核心紀律)
------------------------------
對一個 (broker, symbol) pair,某日 salience 有三種狀態,混淆會讓 baseline
系統性偏高(只用「有交易的日子」= pair 內的倖存者偏誤):

- **有交易**:gross_{b,s,t} / gross_{b,t} > 0 → 入 baseline
- **未交易**:席位當天有活動但沒碰這檔 → **0,真實的零,入 baseline**
- **無定義**:席位當天完全沒活動,或該股不在 universe → null,不入 baseline

故 `build_pair_panel` 會**顯式把未交易日補成 0**(在席位活躍日 × 目標股票
的交叉上),不是靠 T1 的既有列。

two_sidedness 的語意界線(§7)
------------------------------
`min(B,S)/max(B,S)` 描述**雙邊流量**,**不識別當沖**——同一席位的買方與
賣方可能是不同客戶。兩側皆零時為 null(不是 0)。
"""

from __future__ import annotations

import polars as pl

from ws_branch.measure import guards


def daily_salience(
    t1_slice: pl.DataFrame, branch_day: pl.DataFrame, *,
    universe: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """T1 切片 × 席位日總額 → 當日 salience / signed contribution / two_sidedness。

    t1_slice:broker, symbol_id, date, buy_dollar, sell_dollar
    branch_day:broker, date, gross_amt(席位當日全市場總額,來自 T4/primitives)
    universe:(symbol_id, date) 逐日普通股清單。**給了就會檢查分子是否套了
    同一個 gate**——分母 branch_day 必然已 gate,分子沒 gate 的話 salience
    的分子分母口徑不一致(2026-09-21 Phase 3 實付,見 guards B 類)。

    回傳逐列:stock_gross(該 pair 當日金額)、branch_gross(分母)、salience、
    signed_contrib(淨額佔席位總額,帶正負)、two_sidedness。
    """
    guards.require_columns(t1_slice, ["broker", "symbol_id", "date",
                                      "buy_dollar", "sell_dollar"],
                           who="daily_salience(t1_slice)")
    guards.require_columns(branch_day, ["broker", "date", "gross_amt"],
                           who="daily_salience(branch_day)")
    guards.require_unique_key(t1_slice, ["broker", "symbol_id", "date"],
                              who="daily_salience(t1_slice)")
    guards.require_unique_key(branch_day, ["broker", "date"],
                              who="daily_salience(branch_day)")
    if universe is not None:
        guards.require_same_universe(
            t1_slice, universe, keys=["symbol_id", "date"],
            who="daily_salience(t1_slice)")
    return (
        t1_slice.with_columns(
            (pl.col("buy_dollar") + pl.col("sell_dollar")).alias("stock_gross"),
            (pl.col("buy_dollar") - pl.col("sell_dollar")).alias("stock_net"),
        )
        .join(branch_day.select("broker", "date",
                                pl.col("gross_amt").alias("branch_gross")),
              on=["broker", "date"], how="inner")
        .with_columns(
            pl.when(pl.col("branch_gross") > 0)
            .then(pl.col("stock_gross") / pl.col("branch_gross"))
            .otherwise(None).alias("salience"),
            pl.when(pl.col("branch_gross") > 0)
            .then(pl.col("stock_net") / pl.col("branch_gross"))
            .otherwise(None).alias("signed_contrib"),
            # 兩側皆零 → null,不是 0(§7 明令)
            pl.when(pl.max_horizontal("buy_dollar", "sell_dollar") > 0)
            .then(pl.min_horizontal("buy_dollar", "sell_dollar")
                  / pl.max_horizontal("buy_dollar", "sell_dollar"))
            .otherwise(None).alias("two_sidedness"),
        )
    )


def build_pair_panel(
    daily: pl.DataFrame, branch_day: pl.DataFrame, *, symbol_id: str,
) -> pl.DataFrame:
    """把單一股票的 pair panel 補成「席位活躍日 × 該股」的完整交叉。

    未交易日補 0(真實的零);席位當天完全沒活動的日子不在 branch_day 裡,
    自然不出現(無定義)。這是 baseline 正確的前提——見模組 docstring。

    只處理單一股票:讀本與 pair 層研究都是單股查詢,全市場交叉會是
    879 席位 × 1,975 檔 × 170 日 ≈ 2.95 億列(規格 §7:首版不物化)。
    """
    guards.require_unique_key(branch_day, ["broker", "date"],
                              who="build_pair_panel(branch_day)")
    traded = daily.filter(pl.col("symbol_id") == symbol_id)
    guards.require_non_empty(traded, who=f"build_pair_panel({symbol_id})")
    brokers = traded.select("broker").unique()
    # 只對「曾經碰過這檔」的席位補零:從未碰過的席位補零無資訊且會灌爆列數
    grid = branch_day.join(brokers, on="broker", how="semi").select(
        "broker", "date", pl.col("gross_amt").alias("branch_gross"))
    joined = grid.join(
        traded.select("broker", "date", "stock_gross", "stock_net",
                      "salience", "signed_contrib", "two_sidedness"),
        on=["broker", "date"], how="left")
    # traded 必須在 fill_null 之前判定:補零後就分不出「未交易」與「交易 0 元」
    return (joined
            .with_columns(pl.col("stock_gross").is_not_null().alias("traded"))
            .with_columns(
                pl.col("stock_gross").fill_null(0.0),
                pl.col("stock_net").fill_null(0.0),
                pl.col("salience").fill_null(0.0),
                pl.col("signed_contrib").fill_null(0.0),
                pl.lit(symbol_id).alias("symbol_id")))


def pair_history(
    panel: pl.DataFrame, *, window: int = 60, min_periods: int = 20,
) -> pl.DataFrame:
    """對每個席位,算 salience / stock_gross 的**嚴格落後**滾動基準與參與率。

    窗以席位自身的活躍日計(panel 每列 = 一個活躍日)。歷史不足 min_periods
    時回傳 null——**新 pair / 新席位 / 新上市股票不得因沒有歷史就被標成
    最高異常**(§7 明令)。

    另給 `salience_cond_mean`(只在有交易的日子上的均值):
    salience 的無條件均值 = 參與率 × 條件規模,兩者分開讀才不會把
    「稀疏 pair 偶爾碰一次」誤讀成「異常建倉」。
    """
    guards.require_unique_key(panel, ["broker", "date"],
                              who="pair_history(panel)")
    p = panel.sort("broker", "date")
    lag_sal = pl.col("salience").shift(1).over("broker")
    lag_gross = pl.col("stock_gross").shift(1).over("broker")
    lag_traded = pl.col("traded").shift(1).over("broker").cast(pl.Float64)
    # 條件均值 = 落後 salience 總和 / 落後有交易日數
    roll = dict(window_size=window, min_samples=min_periods)
    return p.with_columns(
        lag_sal.rolling_mean(**roll).over("broker").alias("sal_mean"),
        lag_sal.rolling_std(**roll).over("broker").alias("sal_sd"),
        lag_gross.rolling_mean(**roll).over("broker").alias("gross_mean"),
        lag_gross.rolling_std(**roll).over("broker").alias("gross_sd"),
        lag_traded.rolling_mean(**roll).over("broker").alias("participation_rate"),
        lag_sal.rolling_sum(**roll).over("broker").alias("_sal_sum"),
        lag_traded.rolling_sum(**roll).over("broker").alias("_traded_days"),
        lag_sal.is_not_null().cast(pl.Int32)
        .rolling_sum(window_size=window, min_samples=1)
        .over("broker").alias("history_n"),
    ).with_columns(
        pl.when(pl.col("_traded_days") > 0)
        .then(pl.col("_sal_sum") / pl.col("_traded_days"))
        .otherwise(None).alias("salience_cond_mean"),
    ).drop("_sal_sum", "_traded_days")


def pair_anomaly(df: pl.DataFrame, *, min_sd: float = 1e-12) -> pl.DataFrame:
    """三個並列的異常量:相對重要性、絕對流量、以及兩者是否分歧。

    `denominator_effect` = salience 上升但該股絕對金額下降 —— 規格 §7 點名的
    陷阱(席位在其他股票縮量也會推高 salience)。標出來,讓讀本不能只講
    「這檔變重要了」。
    """
    sal_up = pl.col("salience") - pl.col("sal_mean")
    gross_up = pl.col("stock_gross") - pl.col("gross_mean")
    return df.with_columns(
        pl.when(pl.col("sal_sd") > min_sd)
        .then(sal_up / pl.col("sal_sd")).otherwise(None).alias("salience_z"),
        pl.when(pl.col("gross_sd") > min_sd)
        .then(gross_up / pl.col("gross_sd")).otherwise(None).alias("stock_gross_z"),
        pl.when((pl.col("sal_mean") > 0) & pl.col("sal_mean").is_not_null())
        .then(pl.col("salience") / pl.col("sal_mean"))
        .otherwise(None).alias("salience_ratio"),
        ((sal_up > 0) & (gross_up < 0)).alias("denominator_effect"),
    )
