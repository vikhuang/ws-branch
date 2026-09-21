"""表的宣告(資料即設定):加新表 = 在此加一筆,runner 一行不改。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import polars as pl

from ws_branch.tables.transforms import (
    t4_broker_measure,
    t1_broker_daily,
    t3_official_daily,
    t3b_accounting_bounds,
    t4_broker_features,
)

FIRST_YEAR = 2021


@dataclass(frozen=True)
class Table:
    name: str
    build_year: Callable[[int], pl.LazyFrame]  # 純轉換:年 → LazyFrame
    verify: Callable[[int, int], None]         # (n_samples, seed) → raise on FAIL
    first_year: int = FIRST_YEAR
    date_col: str = "date"
    manifest: Callable[[int], dict] | None = None   # §4.5:年 → manifest 內容(runner 寫檔)
    frozen: bool = False   # Step F:退役的現行產出——仍可 build 供重現,但不隨年份更新


def _verify_t1(n_samples: int, seed: int) -> None:
    """T1 對帳:逐年抽日期→抽股票,總量 vs TEJ vol 閉環(>0.1% 即 FAIL)。

    記憶體紀律:單日 filter 走謂詞下推,峰值 <1GB——前版對全史 unique()
    收集曾膨脹 30GB 壓縮頁(2026-09-15 事故三)。
    """
    import random

    from ws_core import prices

    from ws_branch.tables import io
    from ws_branch.tables.checks import closure_overshoots

    rng = random.Random(seed)
    years = io.existing_years("t1_broker_daily")
    dates_per_year = max(n_samples // (len(years) * 5), 2)
    frames = []
    for y in years:
        ylf = pl.scan_parquet(io.year_path("t1_broker_daily", y))
        all_dates = ylf.select("date").unique().collect()["date"].to_list()
        for d in rng.sample(all_dates, min(dates_per_year, len(all_dates))):
            day = (ylf.filter(pl.col("date") == d)
                   .group_by("symbol_id", "date")
                   .agg(pl.col("buy_sh").sum().alias("buy"),
                        pl.col("sell_sh").sum().alias("sell"))
                   .collect())
            frames.append(day.sample(min(5, day.height),
                                     seed=rng.randint(0, 9999)))
    totals = pl.concat(frames)
    px = (prices(coids=totals["symbol_id"].unique().to_list(),
                 start=str(totals["date"].min()), end=str(totals["date"].max()),
                 columns=["coid", "mdate", "vol"])
          .rename({"coid": "symbol_id", "mdate": "date"})
          .with_columns(pl.col("date").cast(pl.Date)))
    joined = totals.join(px, on=["symbol_id", "date"], how="inner")
    # 逐年覆蓋揭露:本地 TEJ prices 為 2024+ 滾動視窗,舊年份 join 為零
    # 必須明示,不得靜默(家法);2021-2023 閉環需 BigQuery 全量另驗
    cov = (totals.with_columns(pl.col("date").dt.year().alias("y"))
           .group_by("y").len().rename({"len": "sampled"})
           .join(joined.with_columns(pl.col("date").dt.year().alias("y"))
                 .group_by("y").len().rename({"len": "joined"}),
                 on="y", how="left")
           .with_columns(pl.col("joined").fill_null(0)).sort("y"))
    print("verify[t1_broker_daily] 逐年覆蓋(sampled→joined):")
    for r in cov.iter_rows(named=True):
        note = "" if r["joined"] else " ← 本地 TEJ 無此年,未驗(需 BQ)"
        print(f"  {r['y']}: {r['sampled']} → {r['joined']}{note}")
    bad = closure_overshoots(totals, px)
    if bad.height:
        print(bad.head(10))
        raise SystemExit(f"FAIL: {bad.height} 股日 broker 超過 TEJ vol(硬性不變量破裂)")
    under = joined.with_columns(
        (pl.col("vol") - pl.col("buy") / 1000).alias("short_lots"))
    n_under = under.filter(pl.col("short_lots") > 1).height
    print(f"PASS: 零超出(broker ⊆ TEJ 成立);短少>1張 {n_under}/{joined.height} 股日"
          f"(鉅額/特殊交易,監控用)")


def _verify_t3(n_samples: int, seed: int) -> None:
    """T3 對帳:抽樣列 vs ws-core shareholding 原始值 ×1000 逐格重導。"""
    import random

    from ws_core import shareholding

    from ws_branch.tables import io
    from ws_branch.tables.transforms.t3_official_daily import convert

    rng = random.Random(seed)
    years = io.existing_years("t3_official_daily")
    frames = []
    for y in rng.sample(years, min(4, len(years))):
        ylf = pl.scan_parquet(io.year_path("t3_official_daily", y))
        dates = ylf.select("date").unique().collect()["date"].to_list()
        for d in rng.sample(dates, min(2, len(dates))):
            day = ylf.filter(pl.col("date") == d).collect()
            frames.append(day.sample(min(n_samples // 8 + 1, day.height),
                                     seed=rng.randint(0, 9999)))
    mine = pl.concat(frames)
    raw = shareholding(coids=mine["symbol_id"].unique().to_list(),
                       start=str(mine["date"].min()), end=str(mine["date"].max()))
    ref = convert(raw).collect()
    j = mine.join(ref, on=["symbol_id", "date"], how="inner", suffix="_ref")
    if j.height < mine.height * 0.9:
        raise SystemExit(f"FAIL: 重導覆蓋不足 {j.height}/{mine.height}")
    for c in ["foreign_buy_sh", "fund_net_sh", "prop_hedge_sell_sh",
              "foreign_buy_amt", "day_trade_pct"]:
        bad = j.filter((pl.col(c) - pl.col(f"{c}_ref")).abs() > 1e-6)
        if bad.height:
            print(bad.select("symbol_id", "date", c, f"{c}_ref").head(5))
            raise SystemExit(f"FAIL: {c} 與原始重導不符 {bad.height} 列")
    print(f"PASS: T3 抽樣 {j.height} 列 × 5 欄逐格重導一致")


def _verify_t4(n_samples: int, seed: int) -> None:
    """T4 對帳:抽樣分點日自 T1 重算比對 + 不變量(ratio/share ∈ [0,1];
    v2:multiplicity/basket_self_sim ∈[0,1]、sector_hhi ∈(0,1]、
    foreign_sim/fund_sim(含 20/60D 滾動版)∈[-1,1])。"""
    import random

    from ws_branch.tables import io
    from ws_branch.tables.transforms.t4_broker_features import compute_broker_day

    rng = random.Random(seed)
    years = io.existing_years("t4_broker_features")
    frames = []
    for y in rng.sample(years, min(3, len(years))):
        ylf = pl.scan_parquet(io.year_path("t4_broker_features", y))
        dates = ylf.select("date").unique().collect()["date"].to_list()
        d = rng.choice(dates)
        day = ylf.filter(pl.col("date") == d).collect()
        sim_cols = ["foreign_sim_buy", "foreign_sim_sell",
                    "fund_sim_buy", "fund_sim_sell"]
        sim_window_cols = [f"{c}_{w}d" for c in sim_cols for w in (20, 60)]
        sim_bad = pl.any_horizontal([
            (pl.col(c) < -1 - 1e-9) | (pl.col(c) > 1 + 1e-9)
            for c in sim_cols + sim_window_cols])
        inv = day.filter(
            (pl.col("directional_ratio") < -1e-9)
            | (pl.col("directional_ratio") > 1 + 1e-9)
            | (pl.col("top1_share") > pl.col("top5_share") + 1e-9)
            | (pl.col("top5_share") > 1 + 1e-9)
            # v2(2026-09-16 O1):multiplicity ∈[0,1]、basket_self_sim ∈[0,1]
            # (金額皆非負→cosine 非負)、sector_hhi ∈(0,1]、相似度欄位 ∈[-1,1]
            | (pl.col("multiplicity") < -1e-9) | (pl.col("multiplicity") > 1 + 1e-9)
            | (pl.col("basket_self_sim") < -1e-9)
            | (pl.col("basket_self_sim") > 1 + 1e-9)
            | (pl.col("sector_hhi") <= 0) | (pl.col("sector_hhi") > 1 + 1e-9)
            | sim_bad)
        if inv.height:
            print(inv.head(5))
            raise SystemExit(f"FAIL: {d} 有 {inv.height} 列違反不變量")
        t1_day = (pl.scan_parquet(io.year_path("t1_broker_daily", y))
                  .filter(pl.col("date") == d)
                  .select("broker", "broker_name", "date",
                          "buy_dollar", "sell_dollar").collect())
        recomputed = compute_broker_day(t1_day)
        j = day.join(recomputed, on=["broker", "date"], suffix="_rc")
        bad = j.filter(
            ((pl.col("gross_amt") - pl.col("gross_amt_rc")).abs() > 1)
            | ((pl.col("n_symbols") - pl.col("n_symbols_rc")).abs() > 0))
        if bad.height:
            raise SystemExit(f"FAIL: {d} 有 {bad.height} 分點與 T1 重算不符")
        frames.append(day)
    n = sum(f.height for f in frames)
    print(f"PASS: T4 不變量 + T1 重算一致(抽 {len(frames)} 日 {n:,} 分點日)")


def _verify_t3b(n_samples: int, seed: int) -> None:
    """T3b 對帳:界限的數學不變量 + 與 T1/T3 重算一致 + universe gate 生效。

    界限不做 clipping(架構文件 §5.1),所以這裡不是「看起來合法就過」——
    先查輸入自洽旗標,再查不自洽列的比例是否在可接受範圍。
    """
    import random

    from ws_branch.tables import io

    rng = random.Random(seed)
    years = io.existing_years("t3b_accounting_bounds")
    checked = 0
    for y in rng.sample(years, min(2, len(years))):
        ylf = pl.scan_parquet(io.year_path("t3b_accounting_bounds", y))
        dates = ylf.select("date").unique().collect()["date"].to_list()
        for d in rng.sample(dates, min(max(n_samples // 20, 2), len(dates))):
            day = ylf.filter(pl.col("date") == d).collect()
            # §5.3:鍵唯一性是第一道檢查——長表擴充 cohort/actor 時最容易破
            keys = ["symbol_id", "date", "side", "cohort_id", "actor_bucket"]
            n_dup = day.height - day.select(keys).unique().height
            if n_dup:
                raise SystemExit(f"FAIL: {d} 有 {n_dup} 列重複鍵 {keys}")
            n_null_flag = day.filter(pl.col("bounds_publishable").is_null()).height
            if n_null_flag:
                raise SystemExit(
                    f"FAIL: {d} 有 {n_null_flag} 列 bounds_publishable 為 null"
                    f"——旗標不得為 null,下游 `~flag` 會靜默漏列")
            # 界限的數學不變量只在輸入自洽的列上成立;輸入不自洽的列(官方桶
            # 超過 V)照 §5.1 以旗標擋下發布,不 clip 也不在此當硬錯誤
            ok = day.filter(pl.col("bounds_publishable"))
            bad = ok.filter(
                (pl.col("foreign_x_lo") > pl.col("foreign_x_hi") + 1e-6)
                | (pl.col("foreign_x_lo") < -1e-6)
                | (pl.col("foreign_x_hi") > pl.col("cohort_sh") + 1e-6)
                | (pl.col("foreign_x_hi") > pl.col("official_foreign_sh") + 1e-6)
                | (pl.col("foreign_y_lo") > pl.col("foreign_y_hi") + 1e-6)
                | (pl.col("foreign_cov_lo") > pl.col("foreign_cov_hi") + 1e-9))
            if bad.height:
                print(bad.head(5))
                raise SystemExit(f"FAIL: {d} 有 {bad.height} 列界限自相矛盾")
            # 覆蓋:母體(universe ∩ 當日有分點成交)一檔都不能少——缺 T3/行情的
            # 股票日要在表裡以 input_complete=False 出現,不是消失(A9 複查)
            from ws_core import stock_attr

            from ws_branch.measure import universe as _uni
            uni_d = _uni.stock_universe(stock_attr(start=str(d), end=str(d),
                                                   columns=["coid", "mdate", "stktp_c"]))
            pop = _uni.apply_universe(
                io.scan("t1_broker_daily", start=str(d), end=str(d))
                .select("symbol_id", "date").unique().collect(), uni_d)
            got = day.filter(pl.col("side") == "buy").select("symbol_id")
            lost = pop.join(got, on="symbol_id", how="anti").height
            if lost:
                raise SystemExit(f"FAIL: {d} 母體 {pop.height} 檔,表內 {got.height} 檔,"
                                 f"{lost} 檔無聲消失(缺 T3/行情須以 input_complete=False 進表)")
            n_missing = day.filter(~pl.col("input_complete")).height
            n_bad_input = day.filter(pl.col("input_complete")
                                     & ~pl.col("foreign_bounds_ok")).height
            n_bad_resid = day.filter(~pl.col("other_actor_sh_ok").fill_null(False)).height
            n_bad_unobs = day.filter(~pl.col("unobserved_sh_ok").fill_null(False)).height
            n_no_t3 = day.filter(~pl.col("t3_present")).height
            print(f"  {d}: {day.height:,} 列(可發布 {ok.height:,});母體 {pop.height} 檔全在;"
                  f"輸入缺值 {n_missing}(其中缺 T3 列 {n_no_t3})、輸入不自洽 {n_bad_input}、"
                  f"餘額為負 {n_bad_resid}、閉環破裂 {n_bad_unobs}")
            if n_bad_input > day.height * 0.01:
                raise SystemExit(
                    f"FAIL: {d} 輸入不自洽 {n_bad_input}/{day.height} 超過 1%——"
                    f"口徑問題,非零星資料瑕疵")
            # 閉環破裂在 2026 集中於 05-05 / 07-17 兩天(TEJ vol 偏低,
            # audit_ledger A6);單日大規模破裂是資料事件不是計算錯誤,
            # 這些列已由 bounds_publishable 擋下,此處只在「零星破裂」
            # (<10%)時視為異常,全日性破裂則印出供人判讀
            if 0 < n_bad_unobs < day.height * 0.1:
                print(day.filter(~pl.col("unobserved_sh_ok")).head(3))
                raise SystemExit(
                    f"FAIL: {d} 有 {n_bad_unobs} 列 T1 超過 TEJ vol 逾 1 張容差"
                    f"(零星閉環破裂,A5 硬性不變量)")
            if n_bad_unobs:
                print(f"    ⚠ {d} 全日性閉環破裂 {n_bad_unobs}/{day.height}"
                      f"——TEJ vol 當日偏低,該日界限全部不發布(A6)")
            checked += day.height
    print(f"PASS: T3b 界限不變量 + 閉環(抽查 {checked:,} 列)")


def _verify_t4v3(n_samples: int, seed: int) -> None:
    """T4 v3 對帳:鍵唯一、不變量、available_at 規則、抽一月自 T1/T3 以同一純函數重算恆等。"""
    import random

    from ws_branch.tables import io
    from ws_branch.tables.transforms import t4_broker_measure as m

    rng = random.Random(seed)
    years = io.existing_years(m.TABLE)
    checked = 0
    cos_cols = [f"cos_{b}_{s}" for b in ["market", *m.OFFICIAL_BUCKETS] for s in ("buy", "sell")]
    for y in rng.sample(years, min(2, len(years))):
        ylf = pl.scan_parquet(io.year_path(m.TABLE, y))
        dates = ylf.select("date").unique().collect()["date"].to_list()
        for d in rng.sample(dates, min(max(n_samples // 20, 2), len(dates))):
            day = ylf.filter(pl.col("date") == d).collect()
            if day.height != day.select("broker", "date").unique().height:
                raise SystemExit(f"FAIL: {d} 鍵 (broker, date) 重複")
            bad = day.filter(
                (pl.col("gross_amt") <= 0) | (pl.col("n_symbols") < 1)
                | (pl.col("directional_ratio") < -1e-9) | (pl.col("directional_ratio") > 1 + 1e-9)
                | (pl.col("top1_share") > pl.col("top5_share") + 1e-9) | (pl.col("top5_share") > 1 + 1e-9)
                | (pl.col("basket_self_sim") < -1e-9) | (pl.col("basket_self_sim") > 1 + 1e-9)
                | pl.any_horizontal([(pl.col(c) < -1 - 1e-9) | (pl.col(c) > 1 + 1e-9) for c in cos_cols])
                | (pl.col("available_at").dt.convert_time_zone(m.TAIPEI).dt.time() != m.AVAILABLE_AT_TIME)
                | (pl.col("available_at").dt.convert_time_zone(m.TAIPEI).dt.date() != pl.col("date")))
            if bad.height:
                print(bad.head(5))
                raise SystemExit(f"FAIL: {d} 有 {bad.height} 列違反不變量/available_at 規則")
            checked += day.height
        # 抽一個月用同一純函數重算,必須逐列恆等(公式只有一份)
        d = rng.choice(dates)
        uni, t3, cal = m._inputs(y)
        month_start, month_end = m._month_bounds(y, d.month)
        prior = cal.filter(pl.col("date") >= month_start)["prev_date"].min()
        read_from = min(prior, month_start) if prior is not None else month_start
        t1 = (io.scan("t1_broker_daily", start=str(read_from), end=str(month_end))
              .select("broker", "broker_name", "symbol_id", "date", "buy_dollar", "sell_dollar").collect())
        recomputed = m.compute_month(t1, t3, uni, cal, month_start=month_start)
        stored = (ylf.filter((pl.col("date") >= month_start) & (pl.col("date") <= month_end))
                  .collect().select(recomputed.columns).sort("date", "broker"))
        recomputed = recomputed.sort("date", "broker")
        # 浮點欄位容忍加總順序噪音(實測 ≤ 3e-16);其餘欄位與 null 位置必須完全相等
        if stored.height != recomputed.height:
            raise SystemExit(f"FAIL: {y}-{d.month:02d} 重算列數 {recomputed.height} ≠ 表 {stored.height}")
        for c in stored.columns:
            a, b = stored[c], recomputed[c]
            same = ((a - b).abs().fill_null(0) <= 1e-9).all() and (a.is_null() == b.is_null()).all() \
                if a.dtype == pl.Float64 else a.equals(b)
            if not same:
                raise SystemExit(f"FAIL: {y}-{d.month:02d} 欄 {c} 重算與表不恆等")
    print(f"PASS: T4 v3 不變量 + available_at + 一月重算恆等(抽查 {checked:,} 列)")


TABLES: dict[str, Table] = {
    "t1_broker_daily": Table(
        name="t1_broker_daily",
        build_year=t1_broker_daily.build_year,
        verify=_verify_t1,
    ),
    "t3_official_daily": Table(
        name="t3_official_daily",
        build_year=t3_official_daily.build_year,
        verify=_verify_t3,
        first_year=2016,
    ),
    "t3b_accounting_bounds": Table(
        name="t3b_accounting_bounds",
        build_year=t3b_accounting_bounds.build_year,
        verify=_verify_t3b,
        first_year=2021,
    ),
    # T4 v2(rank 版指紋、multiplicity):Step F 退役(docs/STEP_F_INVENTORY_2026-09-21.md)
    # ——舊檔保留、可重建供重現,不原地覆蓋語意;新消費端一律接 t4_broker_measure
    "t4_broker_features": Table(
        name="t4_broker_features",
        build_year=t4_broker_features.build_year,
        verify=_verify_t4,
        frozen=True,
    ),
    # T4 v3(Step E):與 v2 並存,不原地覆蓋(§11);manifest 由 runner 寫
    "t4_broker_measure": Table(
        name="t4_broker_measure",
        build_year=t4_broker_measure.build_year,
        verify=_verify_t4v3,
        manifest=t4_broker_measure.manifest,
    ),
    # T2 價位表不物化:ws-core broker_tx_pricelevel_scan 即視圖(REDESIGN §3)
    # TDCC/主動 ETF 量小,ws-core 直讀不物化
}
