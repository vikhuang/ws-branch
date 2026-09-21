"""T4 v3 `t4_broker_measure`:分點×日的最小量測表(架構文件 §4.2,Step E)。

與 `t4_broker_features`(v2,rank 版指紋)**並存、不原地覆蓋**(§11:未經版本化
不得在原 T4 路徑把 rank 改為 cosine)。欄位全部是 Phase 1-4 驗過的量,計算
一律呼叫 measure/ 的純函數——讀本、研究腳本與本表用同一份公式。

時間契約(§4.5)
--------------
- 每列 `available_at`:該日 T1(broker_tx 21:35 落地)與 T3(shareholding 21:43)
  皆到齊之後 = **date 當天 21:45 台北**(audit A7 實測);交易日 ≠ 可用時間。
- 本表每列只用 date 當日與**前一交易日**(basket)的輸入,故未來資料追加
  不改變既有列(e2e `test_e2e_asof_append_does_not_change_frozen_rows` 執法)。
- 滾動 baseline / state 不在本表(讀時由 measure.baseline 以嚴格落後窗計算)。

manifest(`year=YYYY.manifest.json`,由 runner 寫):schema/measurement/universe/
cohort 版本、source_snapshot、fit_start/fit_end(資料範圍)、as_of(建表時刻)、
available_at 規則、缺值規則。
"""

from __future__ import annotations

import datetime
import json
from pathlib import Path

import polars as pl
from ws_core import stock_attr, tradedays
from ws_core.paths import fugle_dir, tej_dir

from ws_branch.measure import allocation, universe
from ws_branch.tables import io

TABLE = "t4_broker_measure"
SCHEMA_VERSION = "t4_broker_measure.v3.0"
MEASUREMENT_VERSION = "m1-2026-09-21"   # 公式版本;改任何一欄的定義就要升
AVAILABLE_AT_TIME = datetime.time(21, 45)
TAIPEI = "Asia/Taipei"
OFFICIAL_BUCKETS = {  # 官方桶 → (T3 買金額欄, 賣金額欄, anchor 狀態)
    "foreign": ("foreign_buy_amt", "foreign_sell_amt", "anchored"),
    "fund": ("fund_buy_amt", "fund_sell_amt", "unanchored"),
    "prop_self": ("prop_self_buy_amt", "prop_self_sell_amt", "unanchored"),
    "prop_hedge": ("prop_hedge_buy_amt", "prop_hedge_sell_amt", "unanchored"),
}
NULL_RULES = {
    "directional_ratio/top1_share/top5_share": "gross_amt=0 時無定義 → null(實務上不出現:列只在 gross>0 席位日)",
    "cos_market_*": "席位該側 norm=0(當日只買或只賣)→ null",
    "cos_<bucket>_*": "席位該側 norm=0 或當日官方桶向量 norm=0 → null;不填 0",
    "basket_self_sim": "前一交易日無資料 → 該列不出現(缺值),完整兩日但籃子不重疊 → 0",
}


def _month_bounds(year: int, month: int) -> tuple[datetime.date, datetime.date]:
    start = datetime.date(year, month, 1)
    end = (datetime.date(year + 1, 1, 1) if month == 12
           else datetime.date(year, month + 1, 1)) - datetime.timedelta(days=1)
    return start, end


def compute_month(
    t1: pl.DataFrame, t3: pl.DataFrame, uni: pl.DataFrame, cal: pl.DataFrame,
    *, month_start: datetime.date,
) -> pl.DataFrame:
    """純函數:一個月(含前一交易日)的 T1 切片 → 該月分點日量測列。

    t1:broker, broker_name, symbol_id, date, buy_dollar, sell_dollar(未 gate)
    t3:symbol_id, date + OFFICIAL_BUCKETS 的金額欄(未 gate)
    uni:逐日 (symbol_id, date) 普通股;cal:`prev_trading_day_map` 輸出
    """
    sl = universe.apply_universe(t1, uni).with_columns(
        (pl.col("buy_dollar") + pl.col("sell_dollar")).alias("gross"))
    in_month = sl.filter(pl.col("date") >= month_start)
    if in_month.height == 0:
        return pl.DataFrame()
    traded = in_month.filter(pl.col("gross") > 0)
    base = (traded.group_by("broker", "date").agg(
        pl.col("broker_name").max().alias("broker_name"),   # 群內順序無關(確定性)
        pl.col("buy_dollar").sum().alias("gross_buy_amt"),
        pl.col("sell_dollar").sum().alias("gross_sell_amt"),
        pl.col("gross").sum().alias("gross_amt"),
        pl.col("symbol_id").n_unique().alias("n_symbols"),     # §4.2:gross>0 的股票數
        pl.col("gross").max().alias("_t1"),
        pl.col("gross").sort(descending=True).head(5).sum().alias("_t5"))
        .with_columns(
            (pl.col("gross_buy_amt") - pl.col("gross_sell_amt")).alias("net_amt"))
        .with_columns(
            (pl.col("net_amt").abs() / pl.col("gross_amt")).fill_nan(None)
            .alias("directional_ratio"),
            (pl.col("_t1") / pl.col("gross_amt")).fill_nan(None).alias("top1_share"),
            (pl.col("_t5") / pl.col("gross_amt")).fill_nan(None).alias("top5_share"))
        .drop("_t1", "_t5"))
    mkt = traded.group_by("symbol_id", "date").agg(pl.col("gross").sum().alias("m"))
    t3g = universe.apply_universe(t3, uni)
    out = base
    for side, col in (("buy", "buy_dollar"), ("sell", "sell_dollar")):
        out = out.join(allocation.amount_cosine(
            in_month, mkt, amount_col=col, ref_col="m", out=f"cos_market_{side}"),
            on=["broker", "date"], how="left")
        for name, (bcol, scol, _) in OFFICIAL_BUCKETS.items():
            ref = t3g.select("symbol_id", "date",
                             pl.col(bcol if side == "buy" else scol).alias("r"))
            out = out.join(allocation.amount_cosine(
                in_month, ref, amount_col=col, ref_col="r", out=f"cos_{name}_{side}"),
                on=["broker", "date"], how="left")
    out = out.join(
        allocation.basket_self_similarity(sl, cal).filter(pl.col("date") >= month_start),
        on=["broker", "date"], how="left")
    return out.with_columns(
        pl.col("date").cast(pl.Datetime("us")).dt.offset_by(
            f"{AVAILABLE_AT_TIME.hour}h{AVAILABLE_AT_TIME.minute}m")
        .dt.replace_time_zone(TAIPEI).alias("available_at"),
    ).sort("date", "broker")


def _calendar(year: int) -> pl.DataFrame:
    cal_raw = tradedays(start=f"{year - 1}-12-01", end=f"{year}-12-31")
    return allocation.prev_trading_day_map(
        cal_raw.filter(pl.col("is_trading_day"))["zdate"].cast(pl.Date).to_list())


def _inputs(year: int) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    # universe 要涵蓋前一年最後一個交易日:1 月首個交易日的 basket 要看它,
    # 只取當年會把前一日整段 gate 掉、首日 basket 全 null(Phase 1 快取即有此缺口)
    uni = universe.stock_universe(stock_attr(
        start=f"{year - 1}-12-01", end=f"{year}-12-31", columns=["coid", "mdate", "stktp_c"]))
    t3 = (io.scan("t3_official_daily", start=f"{year}-01-01", end=f"{year}-12-31")
          .select("symbol_id", "date",
                  *[c for pair in OFFICIAL_BUCKETS.values() for c in pair[:2]])
          .collect())
    return uni, t3, _calendar(year)


def build_year(year: int) -> pl.LazyFrame:
    uni, t3, cal = _inputs(year)
    parts = []
    for m in range(1, 13):
        month_start, month_end = _month_bounds(year, m)
        prior = cal.filter(pl.col("date") >= month_start)["prev_date"].min()
        read_from = min(prior, month_start) if prior is not None else month_start
        t1 = (io.scan("t1_broker_daily", start=str(read_from), end=str(month_end))
              .select("broker", "broker_name", "symbol_id", "date",
                      "buy_dollar", "sell_dollar").collect())
        if t1.height == 0:
            continue
        part = compute_month(t1, t3, uni, cal, month_start=month_start)
        if part.height:
            parts.append(part)
            print(f"  {TABLE} {year}-{m:02d}: {part.height:,} 分點日", flush=True)
    if not parts:
        raise ValueError(f"{TABLE} {year}:無任何分點日")
    return pl.concat(parts).lazy()


def _snapshot(path: Path) -> dict | None:
    if not path.exists():
        return None
    st = path.stat()
    return {"path": str(path), "bytes": st.st_size,
            "mtime": datetime.datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds")}


def _meta_updated_at(path: Path) -> str | None:
    try:
        return json.loads(path.read_text()).get("updated_at")
    except (OSError, ValueError):
        return None


def manifest(year: int) -> dict:
    """§4.5 要求的 manifest 內容(純資料;寫檔由 runner 做)。"""
    out = io.year_path(TABLE, year)
    dates = (pl.scan_parquet(out).select(pl.col("date").min().alias("lo"),
                                        pl.col("date").max().alias("hi")).collect())
    return {
        "table": TABLE, "year": year,
        "schema_version": SCHEMA_VERSION,
        "measurement_version": MEASUREMENT_VERSION,
        "universe_version": universe.UNIVERSE_VERSION,
        "cohort_version": universe.FOREIGN_BROKER_COHORT_VERSION,
        "source_snapshot": {
            "t1_broker_daily": _snapshot(io.year_path("t1_broker_daily", year)),
            "t3_official_daily": _snapshot(io.year_path("t3_official_daily", year)),
            "fugle_broker_tx_updated_at": _meta_updated_at(Path(fugle_dir()) / "broker_tx_meta.json"),
            "tej_stock_attr_updated_at": _meta_updated_at(Path(tej_dir()) / "stock_attr_meta.json"),
        },
        "fit_start": str(dates[0, "lo"]), "fit_end": str(dates[0, "hi"]),
        "as_of": datetime.datetime.now().astimezone().isoformat(timespec="seconds"),
        "available_at_rule": f"每列 date 當天 {AVAILABLE_AT_TIME:%H:%M} {TAIPEI}"
                             "(broker_tx 21:35 + shareholding 21:43 落地後;audit A7)",
        "window": None, "min_samples": None,
        "fallback": "無:本表不含滾動量;baseline/state 讀時以 measure.baseline 嚴格落後窗計算",
        "null_rules": NULL_RULES,
        "anchor_status": {k: v[2] for k, v in OFFICIAL_BUCKETS.items()},
        "not_included": ["market_rank_alignment(Phase 4 §3.4 增量 <0.02,退役)",
                         "multiplicity / foreign_sim(v2 rank 版,見 t4_broker_features)"],
    }
