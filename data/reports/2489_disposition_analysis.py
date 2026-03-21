"""2489 瑞軒 處置期間分點行為深度分析 — 資料準備與分析腳本"""

import polars as pl
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from collections import defaultdict

# === 路徑 ===
DATA_DIR = Path("/Users/vikhuang/r20/data")
WS_BRANCH = Path("/Users/vikhuang/r20/wp/ws-branch")
SSD = Path("/Volumes/DataSSD/twse-tick")
OUT = WS_BRANCH / "tmp" / "2489_disposition_data"
OUT.mkdir(exist_ok=True)

SYMBOL = "2489"

# === 時間範圍 ===
PRE_START = date(2026, 2, 23)   # 四天漲停開始
DISP_START = date(2026, 3, 2)   # 處置開始
DISP_END = date(2026, 3, 18)    # 處置最後一天
POST_END = date(2026, 3, 20)    # 分析結束
FULL_START = date(2026, 2, 23)
FULL_END = date(2026, 3, 20)

# ============================================================================
# Chapter 1: 市場背景
# ============================================================================

def ch1_market_context():
    """Build complete market context for 2489."""
    print("=" * 60)
    print("Chapter 1: 市場背景與處置環境")
    print("=" * 60)

    # --- TEJ Prices ---
    prices = pl.read_parquet(DATA_DIR / "tej" / "prices.parquet")
    p = (
        prices.filter(
            (pl.col("coid") == SYMBOL)
            & (pl.col("mdate") >= FULL_START)
            & (pl.col("mdate") <= FULL_END)
        )
        .sort("mdate")
        .select("coid", "mdate", "open_d", "high_d", "low_d", "close_d",
                "vol", "amt", "roi", "turnover")
    )
    print(f"\n價量時間線 ({len(p)} 天):")
    print(p)

    # --- TEJ Stock Attr ---
    sa = pl.read_parquet(DATA_DIR / "tej" / "stock_attr.parquet")
    attr = (
        sa.filter(
            (pl.col("coid") == SYMBOL)
            & (pl.col("mdate") >= FULL_START)
            & (pl.col("mdate") <= FULL_END)
        )
        .sort("mdate")
        .select("coid", "mdate", "atten_fg", "disp_fg", "mch_prd",
                "susp_fg", "full_fg", "ssadt_fg", "sbadt_fg")
    )
    print(f"\n處置旗標:")
    print(attr)

    # --- TEJ Shareholding ---
    sh = pl.read_parquet(DATA_DIR / "tej" / "shareholding.parquet")
    margin = (
        sh.filter(
            (pl.col("coid") == SYMBOL)
            & (pl.col("mdate") >= FULL_START)
            & (pl.col("mdate") <= FULL_END)
        )
        .sort("mdate")
        .select(
            "coid", "mdate",
            # 融資
            "buy_l", "sell_l", "long_t", "long_ta", "cash_l", "limit_l",
            # 融券
            "buy_s", "sell_s", "short_t", "short_ta",
            # 借券
            "sale_b1", "borr_t1",
            # 當沖
            "vol_dt", "vol_dtp",
            # 三大法人
            "qfii_buy", "qfii_sell", "fund_buy", "fund_sell",
            "dlrp_buy", "dlrp_sell",
        )
    )
    print(f"\n融資融券/借券/當沖:")
    print(margin)

    # --- Merge all ---
    combined = (
        p.join(attr.drop("coid"), on="mdate", how="left")
        .join(margin.drop("coid"), on="mdate", how="left")
    )
    combined.write_parquet(OUT / "ch1_market_context.parquet")
    print(f"\n✅ 已儲存 ch1_market_context.parquet ({len(combined)} rows)")
    return combined


# ============================================================================
# Chapter 2: Tick 深度微觀分析
# ============================================================================

def ch2_tick_microstructure():
    """Analyze tick-level microstructure for each trading day."""
    print("\n" + "=" * 60)
    print("Chapter 2: Tick 深度微觀分析")
    print("=" * 60)

    # Get trading days in our range
    trading_days = []
    for d in _daterange(FULL_START, FULL_END):
        fpath = SSD / "trades" / "Equity" / f"{d.strftime('%Y%m%d')}.parquet"
        if fpath.exists():
            trading_days.append(d)

    print(f"交易日數: {len(trading_days)}")

    all_daily = []

    for td in trading_days:
        ds = td.strftime('%Y%m%d')
        is_disp = DISP_START <= td <= DISP_END

        # --- Trades ---
        trades_path = SSD / "trades" / "Equity" / f"{ds}.parquet"
        trades = pl.read_parquet(trades_path).filter(pl.col("symbol") == SYMBOL)

        if len(trades) == 0:
            continue

        trades = trades.sort("time")
        actual = trades.filter(pl.col("isTrial") == False)
        trial = trades.filter(pl.col("isTrial") == True)

        # --- Orderbooks ---
        ob_path = SSD / "orderbooks" / "Equity" / f"{ds}.parquet"
        ob = None
        if ob_path.exists():
            ob_raw = pl.read_parquet(ob_path).filter(pl.col("symbol") == SYMBOL)
            if len(ob_raw) > 0:
                ob = ob_raw.sort("time")

        # Per-match analysis
        matches = []
        if is_disp and len(actual) > 0:
            # Disposition days: detailed per-match with trial evolution
            for row in actual.iter_rows(named=True):
                t = row["time"]
                dt = datetime.fromtimestamp(t / 1_000_000)

                # Trial trades in the 20 min before this match
                window_start = t - 20 * 60 * 1_000_000  # 20 min in μs
                pre_trial = trial.filter(
                    (pl.col("time") > window_start) & (pl.col("time") < t)
                )

                # Trial price evolution
                trial_prices = pre_trial["price"].to_list() if len(pre_trial) > 0 else []
                trial_first = trial_prices[0] if trial_prices else None
                trial_last = trial_prices[-1] if trial_prices else None
                trial_high = max(trial_prices) if trial_prices else None
                trial_low = min(trial_prices) if trial_prices else None
                trial_std = float(pre_trial["price"].std()) if len(pre_trial) > 1 else 0.0

                # Orderbook at match time
                ob_bid1 = ob_ask1 = ob_bid1_size = ob_ask1_size = None
                if ob is not None and len(ob) > 0:
                    pre_ob = ob.filter(pl.col("time") <= t).tail(1)
                    if len(pre_ob) > 0:
                        ob_row = pre_ob.row(0, named=True)
                        ob_bid1 = ob_row.get("bid1_price")
                        ob_ask1 = ob_row.get("ask1_price")
                        ob_bid1_size = ob_row.get("bid1_size")
                        ob_ask1_size = ob_row.get("ask1_size")

                spread = None
                if ob_bid1 and ob_ask1 and ob_bid1 > 0:
                    spread = (ob_ask1 - ob_bid1) / ob_bid1 * 10000  # bps

                matches.append({
                    "date": td,
                    "match_time": dt.strftime("%H:%M:%S"),
                    "price": float(row["price"]),
                    "size": int(row["size"]),
                    "volume": int(row["volume"]),
                    "bid": float(row["bid"]) if row["bid"] else 0.0,
                    "ask": float(row["ask"]) if row["ask"] else 0.0,
                    "n_trial_before": len(pre_trial),
                    "trial_first": float(trial_first) if trial_first else 0.0,
                    "trial_last": float(trial_last) if trial_last else 0.0,
                    "trial_high": float(trial_high) if trial_high else 0.0,
                    "trial_low": float(trial_low) if trial_low else 0.0,
                    "trial_std": trial_std if trial_std else 0.0,
                    "ob_bid1": float(ob_bid1) if ob_bid1 else 0.0,
                    "ob_ask1": float(ob_ask1) if ob_ask1 else 0.0,
                    "ob_bid1_size": int(ob_bid1_size) if ob_bid1_size else 0,
                    "ob_ask1_size": int(ob_ask1_size) if ob_ask1_size else 0,
                    "spread_bps": float(spread) if spread else 0.0,
                })
        elif not is_disp and len(actual) > 0:
            # Non-disposition days: save 1-minute OHLCV summary
            minute_df = actual.with_columns(
                (pl.col("time") // 60_000_000 * 60_000_000).alias("min_bucket")
            )
            min_ohlc = (
                minute_df.group_by("min_bucket").agg(
                    pl.col("price").first().alias("open"),
                    pl.col("price").max().alias("high"),
                    pl.col("price").min().alias("low"),
                    pl.col("price").last().alias("close"),
                    pl.col("size").sum().alias("volume"),
                ).sort("min_bucket")
                .with_columns(
                    pl.lit(td).alias("date"),
                    pl.col("min_bucket").map_elements(
                        lambda x: datetime.fromtimestamp(x / 1_000_000).strftime("%H:%M"),
                        return_dtype=pl.Utf8,
                    ).alias("time_str"),
                )
            )
            min_ohlc.write_parquet(OUT / f"ch2_minute_{ds}.parquet")

        # Daily summary
        daily = {
            "date": td,
            "is_disp": is_disp,
            "total_ticks": len(trades),
            "n_trial": len(trial),
            "n_actual": len(actual),
            "n_matches": len(matches),
        }
        if len(actual) > 0:
            daily["open"] = actual["price"][0]
            daily["close"] = actual["price"][-1]
            daily["high"] = actual["price"].max()
            daily["low"] = actual["price"].min()
            daily["total_volume"] = int(actual["size"].sum())
        all_daily.append(daily)

        # Save per-match data
        if matches:
            match_df = pl.DataFrame(matches)
            match_df.write_parquet(OUT / f"ch2_matches_{ds}.parquet")

        phase = "處置" if is_disp else ("前置" if td < DISP_START else "出處置")
        print(f"  {td} [{phase}] ticks={len(trades)} trial={len(trial)} "
              f"actual={len(actual)} matches={len(matches)}")

    # Save daily summary
    daily_df = pl.DataFrame(all_daily)
    daily_df.write_parquet(OUT / "ch2_tick_daily_summary.parquet")
    print(f"\n✅ 已儲存 ch2 資料 ({len(daily_df)} days, per-match parquets)")
    return daily_df


# ============================================================================
# Chapter 3: 分點全景分析
# ============================================================================

def ch3_broker_panorama():
    """Classify all brokers by their disposition-period behavior."""
    print("\n" + "=" * 60)
    print("Chapter 3: 分點全景分析")
    print("=" * 60)

    ds = pl.read_parquet(WS_BRANCH / "data" / "daily_summary" / "2489.parquet")
    ds = ds.with_columns(pl.col("broker").cast(pl.Utf8))

    # --- 處置期間 ---
    disp = ds.filter(
        (pl.col("date") >= DISP_START) & (pl.col("date") <= DISP_END)
    )

    # 逐日淨買超矩陣
    daily_net = (
        disp.with_columns(
            (pl.col("buy_shares") - pl.col("sell_shares")).alias("net_shares"),
            (pl.col("buy_amount") - pl.col("sell_amount")).alias("net_amount"),
        )
        .select("broker", "date", "net_shares", "net_amount",
                "buy_shares", "sell_shares", "buy_amount", "sell_amount")
    )

    # 處置期間累計
    cumul = (
        daily_net.group_by("broker")
        .agg(
            pl.col("net_shares").sum().alias("total_net_shares"),
            pl.col("net_amount").sum().alias("total_net_amount"),
            pl.col("buy_shares").sum().alias("total_buy_shares"),
            pl.col("sell_shares").sum().alias("total_sell_shares"),
            pl.col("buy_amount").sum().alias("total_buy_amount"),
            pl.col("sell_amount").sum().alias("total_sell_amount"),
            pl.len().alias("active_days"),
            (pl.col("net_shares") > 0).sum().alias("buy_days"),
            (pl.col("net_shares") < 0).sum().alias("sell_days"),
            (pl.col("net_shares") == 0).sum().alias("flat_days"),
        )
        .sort("total_net_shares", descending=True)
    )

    print(f"\n處置期間 active brokers: {len(cumul)}")
    print(f"\nTop 20 淨買超（吸籌方）:")
    top_buy = cumul.head(20)
    print(top_buy.select("broker", "total_net_shares", "total_net_amount",
                          "active_days", "buy_days", "sell_days"))

    print(f"\nTop 20 淨賣超（出貨方）:")
    top_sell = cumul.tail(20).reverse()
    print(top_sell.select("broker", "total_net_shares", "total_net_amount",
                           "active_days", "buy_days", "sell_days"))

    # --- 行為分類 ---
    def classify(row):
        bd, sd, ad = row["buy_days"], row["sell_days"], row["active_days"]
        if ad <= 2:
            return "單次型"
        buy_ratio = bd / ad if ad > 0 else 0
        sell_ratio = sd / ad if ad > 0 else 0
        if buy_ratio >= 0.6:
            return "累積型"
        elif sell_ratio >= 0.6:
            return "出貨型"
        elif abs(buy_ratio - sell_ratio) < 0.2:
            return "投機型"
        else:
            return "混合型"

    classifications = [classify(row) for row in cumul.iter_rows(named=True)]
    cumul = cumul.with_columns(pl.Series("behavior_type", classifications))

    type_counts = cumul.group_by("behavior_type").agg(
        pl.len().alias("count"),
        pl.col("total_net_shares").sum().alias("group_net_shares"),
    )
    print(f"\n行為分類:")
    print(type_counts.sort("count", descending=True))

    # --- 前半/後半反轉分析 ---
    disp_dates = sorted(disp["date"].unique().to_list())
    mid = len(disp_dates) // 2
    first_half_dates = set(disp_dates[:mid])
    second_half_dates = set(disp_dates[mid:])

    first_half = (
        daily_net.filter(pl.col("date").is_in(first_half_dates))
        .group_by("broker").agg(pl.col("net_shares").sum().alias("first_half_net"))
    )
    second_half = (
        daily_net.filter(pl.col("date").is_in(second_half_dates))
        .group_by("broker").agg(pl.col("net_shares").sum().alias("second_half_net"))
    )
    reversal = (
        first_half.join(second_half, on="broker", how="outer_coalesce")
        .with_columns(
            pl.col("first_half_net").fill_null(0),
            pl.col("second_half_net").fill_null(0),
        )
        .with_columns(
            pl.when(
                (pl.col("first_half_net") > 0) & (pl.col("second_half_net") < 0)
            ).then(pl.lit("先買後賣"))
            .when(
                (pl.col("first_half_net") < 0) & (pl.col("second_half_net") > 0)
            ).then(pl.lit("先賣後買"))
            .otherwise(pl.lit("一致"))
            .alias("reversal_type")
        )
    )
    rev_counts = reversal.group_by("reversal_type").agg(pl.len().alias("count"))
    print(f"\n前半/後半反轉:")
    print(rev_counts)

    # --- 新進 vs 老手 ---
    pre_6m = ds.filter(
        (pl.col("date") >= date(2025, 9, 1)) & (pl.col("date") < DISP_START)
    )
    old_brokers = set(pre_6m["broker"].unique().to_list())
    cumul = cumul.with_columns(
        pl.col("broker").is_in(old_brokers).alias("is_veteran")
    )
    vet_stats = cumul.group_by("is_veteran").agg(
        pl.len().alias("count"),
        pl.col("total_net_shares").sum().alias("group_net_shares"),
        pl.col("total_net_amount").sum().alias("group_net_amount"),
    )
    print(f"\n新進 vs 老手:")
    print(vet_stats)

    # Save
    cumul.write_parquet(OUT / "ch3_broker_cumulative.parquet")
    daily_net.write_parquet(OUT / "ch3_broker_daily_net.parquet")
    reversal.write_parquet(OUT / "ch3_reversal.parquet")
    print(f"\n✅ 已儲存 ch3 資料")
    return cumul, daily_net


# ============================================================================
# Chapter 4: 聰明錢 vs 散戶
# ============================================================================

def ch4_smart_vs_retail():
    """PNL-based broker stratification."""
    print("\n" + "=" * 60)
    print("Chapter 4: 聰明錢 vs 散戶")
    print("=" * 60)

    # PNL daily for rolling ranking up to 3/1
    pnl_daily = pl.read_parquet(WS_BRANCH / "data" / "pnl_daily" / "2489.parquet")
    pnl_daily = pnl_daily.with_columns(pl.col("broker").cast(pl.Utf8))

    # 3-year rolling window ending at 3/1
    train_end = date(2026, 3, 1)
    window_start = date(2023, 3, 1)

    # Baseline: last unrealized before window start
    baseline = (
        pnl_daily.filter(pl.col("date") < window_start)
        .sort("date")
        .group_by("broker")
        .agg(pl.col("unrealized_pnl").last().alias("baseline_unrealized"))
    )

    window = pnl_daily.filter(
        (pl.col("date") >= window_start) & (pl.col("date") <= train_end)
    )

    ranking = (
        window.sort("date")
        .group_by("broker")
        .agg([
            pl.col("realized_pnl").sum(),
            pl.col("unrealized_pnl").last(),
        ])
        .join(baseline, on="broker", how="left")
        .with_columns(pl.col("baseline_unrealized").fill_null(0.0))
        .with_columns(
            (pl.col("realized_pnl") + pl.col("unrealized_pnl")
             - pl.col("baseline_unrealized")).alias("total_pnl")
        )
        .sort("total_pnl", descending=True)
    )

    ranking = ranking.with_row_index("rank", offset=1)
    n_brokers = len(ranking)

    # Stratify: Top-20, Middle, Bottom-20
    top_20 = set(ranking.head(20)["broker"].to_list())
    bottom_20 = set(ranking.tail(20)["broker"].to_list())

    print(f"PNL ranking brokers: {n_brokers}")
    print(f"\nTop-20 聰明錢 (3yr rolling PNL to 3/1):")
    print(ranking.head(20).select("rank", "broker", "total_pnl"))
    print(f"\nBottom-20 績差券商:")
    print(ranking.tail(20).select("rank", "broker", "total_pnl"))

    # Load disposition-period daily net
    daily_net = pl.read_parquet(OUT / "ch3_broker_daily_net.parquet")

    # Stratify daily net
    daily_net = daily_net.with_columns(
        pl.when(pl.col("broker").is_in(top_20)).then(pl.lit("聰明錢(Top-20)"))
        .when(pl.col("broker").is_in(bottom_20)).then(pl.lit("績差(Bottom-20)"))
        .otherwise(pl.lit("其他"))
        .alias("pnl_tier")
    )

    tier_daily = (
        daily_net.group_by("pnl_tier", "date")
        .agg(
            pl.col("net_shares").sum().alias("tier_net_shares"),
            pl.col("net_amount").sum().alias("tier_net_amount"),
            pl.col("broker").n_unique().alias("n_brokers"),
        )
        .sort("date")
    )

    tier_cumul = (
        daily_net.group_by("pnl_tier")
        .agg(
            pl.col("net_shares").sum().alias("total_net_shares"),
            pl.col("net_amount").sum().alias("total_net_amount"),
            pl.col("broker").n_unique().alias("n_brokers"),
        )
    )

    print(f"\n處置期間各層累計:")
    print(tier_cumul)
    print(f"\n處置期間各層逐日:")
    for tier in ["聰明錢(Top-20)", "績差(Bottom-20)", "其他"]:
        t = tier_daily.filter(pl.col("pnl_tier") == tier)
        print(f"\n  {tier}:")
        print(t.select("date", "tier_net_shares", "n_brokers"))

    # Save
    ranking.write_parquet(OUT / "ch4_pnl_ranking.parquet")
    tier_daily.write_parquet(OUT / "ch4_tier_daily.parquet")
    tier_cumul.write_parquet(OUT / "ch4_tier_cumulative.parquet")
    daily_net.write_parquet(OUT / "ch4_daily_net_with_tier.parquet")
    print(f"\n✅ 已儲存 ch4 資料")
    return ranking, tier_daily


# ============================================================================
# Chapter 5: 價位策略分析
# ============================================================================

def ch5_price_strategy():
    """Price-level analysis from Fugle broker_tx."""
    print("\n" + "=" * 60)
    print("Chapter 5: 價位策略分析")
    print("=" * 60)

    # Load broker_tx for disposition period + post-disposition
    all_tx = []
    for d in _daterange(DISP_START, POST_END):
        ds = d.strftime('%Y%m%d')
        path = DATA_DIR / "fugle" / "broker_tx" / f"broker_tx_{ds}.parquet"
        if path.exists():
            tx = pl.read_parquet(path)
            tx = tx.filter(pl.col("symbol_id") == SYMBOL)
            if len(tx) > 0:
                tx = tx.with_columns(pl.lit(d).alias("trade_date"))
                all_tx.append(tx)

    if not all_tx:
        print("  無 broker_tx 資料")
        return None

    tx_df = pl.concat(all_tx)
    # Clean up: price is string, convert
    tx_df = tx_df.with_columns(
        pl.col("price").cast(pl.Float64).alias("price_f"),
        pl.col("broker").cast(pl.Utf8),
    )
    print(f"broker_tx rows: {len(tx_df)}, dates: {tx_df['trade_date'].n_unique()}")

    # Load top buyers/sellers from ch3
    cumul = pl.read_parquet(OUT / "ch3_broker_cumulative.parquet")
    top_buyers = cumul.head(10)["broker"].to_list()
    top_sellers = cumul.tail(10).reverse()["broker"].to_list()

    # Disposition-period only
    disp_tx = tx_df.filter(
        (pl.col("trade_date") >= DISP_START) & (pl.col("trade_date") <= DISP_END)
    )

    # VWAP per broker (disposition period)
    broker_vwap = (
        disp_tx.filter(pl.col("buy") > 0)
        .group_by("broker")
        .agg(
            (pl.col("price_f") * pl.col("buy")).sum().alias("buy_value"),
            pl.col("buy").sum().alias("buy_shares"),
        )
        .filter(pl.col("buy_shares") > 0)
        .with_columns(
            (pl.col("buy_value") / pl.col("buy_shares")).alias("buy_vwap")
        )
    )

    sell_vwap = (
        disp_tx.filter(pl.col("sell") > 0)
        .group_by("broker")
        .agg(
            (pl.col("price_f") * pl.col("sell")).sum().alias("sell_value"),
            pl.col("sell").sum().alias("sell_shares"),
        )
        .filter(pl.col("sell_shares") > 0)
        .with_columns(
            (pl.col("sell_value") / pl.col("sell_shares")).alias("sell_vwap")
        )
    )

    vwap = (
        broker_vwap.select("broker", "buy_vwap", "buy_shares")
        .join(sell_vwap.select("broker", "sell_vwap", "sell_shares"),
              on="broker", how="outer_coalesce")
    )

    print(f"\nTop 10 吸籌方 VWAP:")
    for b in top_buyers:
        v = vwap.filter(pl.col("broker") == b)
        if len(v) > 0:
            r = v.row(0, named=True)
            bv = r.get("buy_vwap")
            sv = r.get("sell_vwap")
            bs = r.get("buy_shares") or 0
            ss = r.get("sell_shares") or 0
            print(f"  {b}: buy_vwap={bv:.2f} ({bs}張) sell_vwap={sv if sv else 'N/A'} ({ss}張)")

    print(f"\nTop 10 出貨方 VWAP:")
    for b in top_sellers:
        v = vwap.filter(pl.col("broker") == b)
        if len(v) > 0:
            r = v.row(0, named=True)
            bv = r.get("buy_vwap")
            sv = r.get("sell_vwap")
            bs = r.get("buy_shares") or 0
            ss = r.get("sell_shares") or 0
            bv_s = f"{bv:.2f}" if bv else "N/A"
            sv_s = f"{sv:.2f}" if sv else "N/A"
            print(f"  {b}: buy_vwap={bv_s} ({bs}張) sell_vwap={sv_s} ({ss}張)")

    # Price-level distribution for key brokers
    key_brokers = set(top_buyers + top_sellers)
    key_tx = disp_tx.filter(pl.col("broker").is_in(key_brokers))
    key_tx.write_parquet(OUT / "ch5_key_broker_tx.parquet")
    vwap.write_parquet(OUT / "ch5_broker_vwap.parquet")

    # Post-disposition tx (3/19~3/20) for key brokers
    post_tx = tx_df.filter(
        (pl.col("trade_date") > DISP_END)
        & (pl.col("broker").is_in(key_brokers))
    )
    post_tx.write_parquet(OUT / "ch5_post_disp_key_tx.parquet")

    # Full broker_tx for report
    tx_df.write_parquet(OUT / "ch5_all_broker_tx.parquet")

    print(f"\n✅ 已儲存 ch5 資料")
    return vwap


# ============================================================================
# Chapter 6: 出處置後清算
# ============================================================================

def ch6_post_disposition():
    """Analyze post-disposition behavior (3/19~3/20)."""
    print("\n" + "=" * 60)
    print("Chapter 6: 出處置後清算")
    print("=" * 60)

    ds = pl.read_parquet(WS_BRANCH / "data" / "daily_summary" / "2489.parquet")
    ds = ds.with_columns(pl.col("broker").cast(pl.Utf8))

    # Post-disposition daily net
    post = ds.filter(
        (pl.col("date") >= date(2026, 3, 19)) & (pl.col("date") <= POST_END)
    ).with_columns(
        (pl.col("buy_shares") - pl.col("sell_shares")).alias("net_shares"),
        (pl.col("buy_amount") - pl.col("sell_amount")).alias("net_amount"),
    )

    # Load disposition accumulation
    cumul = pl.read_parquet(OUT / "ch3_broker_cumulative.parquet")
    top_buyers = cumul.filter(pl.col("total_net_shares") > 0).head(20)["broker"].to_list()

    # What did accumulators do after disposition?
    print("\n處置期間吸籌方（Top 20）出處置後行為:")
    for b in top_buyers[:20]:
        disp_net = cumul.filter(pl.col("broker") == b)["total_net_shares"][0]
        post_b = post.filter(pl.col("broker") == b)
        if len(post_b) > 0:
            for row in post_b.iter_rows(named=True):
                d = row["date"]
                ns = row["net_shares"]
                action = "買" if ns > 0 else "賣" if ns < 0 else "平"
                print(f"  {b} (處置期淨買{disp_net}): {d} → {action} {abs(ns)}張")
        else:
            print(f"  {b} (處置期淨買{disp_net}): 出處置後無交易")

    # 3/19 and 3/20 top movers
    for d in [date(2026, 3, 19), date(2026, 3, 20)]:
        day_data = post.filter(pl.col("date") == d).sort("net_shares", descending=True)
        print(f"\n{d} Top 10 淨買超:")
        print(day_data.head(10).select("broker", "net_shares", "net_amount"))
        print(f"{d} Top 10 淨賣超:")
        print(day_data.tail(10).reverse().select("broker", "net_shares", "net_amount"))

    # Tick analysis for 3/20 (the crash day)
    print(f"\n3/20 Tick 崩跌過程:")
    trades_path = SSD / "trades" / "Equity" / "20260320.parquet"
    if trades_path.exists():
        t320 = pl.read_parquet(trades_path).filter(pl.col("symbol") == SYMBOL)
        actual = t320.filter(pl.col("isTrial") == False).sort("time")
        print(f"  Total ticks: {len(t320)}, Actual: {len(actual)}")

        # Price trajectory in 1-minute buckets
        actual = actual.with_columns(
            (pl.col("time") // 60_000_000 * 60_000_000).alias("minute_bucket")
        )
        minute_ohlc = (
            actual.group_by("minute_bucket")
            .agg(
                pl.col("price").first().alias("open"),
                pl.col("price").max().alias("high"),
                pl.col("price").min().alias("low"),
                pl.col("price").last().alias("close"),
                pl.col("size").sum().alias("volume"),
            )
            .sort("minute_bucket")
        )

        # Show key moments
        for row in minute_ohlc.head(30).iter_rows(named=True):
            dt = datetime.fromtimestamp(row["minute_bucket"] / 1_000_000)
            print(f"  {dt.strftime('%H:%M')} O={row['open']:.2f} H={row['high']:.2f} "
                  f"L={row['low']:.2f} C={row['close']:.2f} V={row['volume']}")

        minute_ohlc_out = minute_ohlc.with_columns(
            pl.col("minute_bucket").map_elements(
                lambda x: datetime.fromtimestamp(x / 1_000_000).strftime("%H:%M"),
                return_dtype=pl.Utf8,
            ).alias("time_str")
        )
        minute_ohlc_out.write_parquet(OUT / "ch6_320_minute_ohlc.parquet")

    # Save post-disposition data
    post.write_parquet(OUT / "ch6_post_disposition_daily.parquet")

    # Estimated P&L for accumulators
    print(f"\n吸籌方推估損益（以 3/20 收盤 38.65 計算）:")
    vwap = pl.read_parquet(OUT / "ch5_broker_vwap.parquet")
    close_320 = 38.65
    for b in top_buyers[:10]:
        v = vwap.filter(pl.col("broker") == b)
        c = cumul.filter(pl.col("broker") == b)
        if len(v) > 0 and len(c) > 0:
            bv = v["buy_vwap"][0]
            net = c["total_net_shares"][0]
            if bv and net > 0:
                pnl = (close_320 - bv) * net * 1000  # 張 → 股
                pnl_pct = (close_320 / bv - 1) * 100
                print(f"  {b}: VWAP={bv:.2f} 淨買{net}張 → 帳面損益 {pnl:,.0f} ({pnl_pct:+.1f}%)")

    print(f"\n✅ 已儲存 ch6 資料")


# ============================================================================
# Helpers
# ============================================================================

def _daterange(start: date, end: date):
    """Yield dates from start to end (inclusive)."""
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    print("2489 瑞軒 處置期間分點行為深度分析")
    print("=" * 60)

    ch1_market_context()
    ch2_tick_microstructure()
    ch3_result = ch3_broker_panorama()
    ch4_smart_vs_retail()
    ch5_price_strategy()
    ch6_post_disposition()

    print("\n" + "=" * 60)
    print("全部分析完成！資料已儲存至 tmp/2489_disposition_data/")
    print("=" * 60)
