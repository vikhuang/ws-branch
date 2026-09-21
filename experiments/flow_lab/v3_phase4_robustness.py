"""Phase 4 複查補做:§8.3 八條中首版漏掉的四條 + 卡四的規模混淆修正。

首版 Phase 4 只做了 buy/sell、leave-one-out、seat bootstrap、殘差控制;
漏了:剔 2330、剔當日成交前五、小額席位、逐月。且 prop_hedge 的發行商比較
用 raw 均值——發行商總公司是最大席位,可能只是規模效應。本腳本全部補上,
且**所有比較都在同一母體(測試期)上做**。
"""

from __future__ import annotations

import datetime
import os

import polars as pl

from ws_branch.measure import calibration as cal, universe
from ws_branch.tables import io
from v3_phase4_calibration import BUCKETS, WARRANT_ISSUER_HQ, _labelled

YEAR = 2026
CONTROLS = lambda side: [f"cos_market_{side}", "log_gross", "log_n"]  # noqa: E731


def build_variant(year: int, name: str, exclude) -> pl.DataFrame:
    """重建 cosine,對 T1 與 T3 套同一個排除規則(剔 2330 / 剔當日前五)。

    m2 起呼叫 `t4_broker_measure.compute_month`——與物化表**同一份公式**(官方 cosine
    在 T3 觀測支撐上算);快取檔名帶 MEASUREMENT_VERSION,版本一變就重算
    (外部審查 09-21:首版變體自己算 cosine、缺 T3 當 0,與主線不同源)。
    """
    from ws_branch.tables.transforms import t4_broker_measure as m
    cache = f"/tmp/v3_phase4_cosines_{name}_{m.MEASUREMENT_VERSION}.parquet"
    if os.path.exists(cache):
        return pl.read_parquet(cache)
    uni, t3, cal_map = m._inputs(year)
    parts = []
    for mo in range(1, 13):
        month_start, month_end = m._month_bounds(year, mo)
        prior = cal_map.filter(pl.col("date") >= month_start)["prev_date"].min()
        read_from = min(prior, month_start) if prior is not None else month_start
        t1 = (io.scan("t1_broker_daily", start=str(read_from), end=str(month_end))
              .select("broker", "broker_name", "symbol_id", "date", "buy_dollar", "sell_dollar").collect())
        if t1.height == 0:
            continue
        sl = universe.apply_universe(t1, uni)
        mkt = (sl.with_columns((pl.col("buy_dollar") + pl.col("sell_dollar")).alias("m"))
               .group_by("symbol_id", "date").agg(pl.col("m").sum()))
        keep = exclude(mkt)                       # (symbol_id, date) 要保留的集合
        t1k = t1.join(keep, on=["symbol_id", "date"], how="semi")
        t3k = t3.join(keep, on=["symbol_id", "date"], how="semi")
        out = m.compute_month(t1k, t3k, uni, cal_map, month_start=month_start)
        if out.height:
            parts.append(out)
        print(f"  [{name}] {year}-{mo:02d}", flush=True)
    df = pl.concat(parts)
    df.write_parquet(cache)
    return df


def test_auc(df: pl.DataFrame, target: str, side: str, *, subset=None) -> dict:
    """訓練期估殘差係數 → 測試期(可再取子集)算 raw 與殘差 AUC,同一母體。"""
    ctrl = CONTROLS(side)
    sub = df.filter(pl.all_horizontal([pl.col(c).is_not_null() for c in [target, *ctrl]]))
    tr, te = cal.time_split(sub, train_frac=2 / 3)
    m = cal.fit_residual(tr, target=target, controls=ctrl)
    te = cal.apply_residual(te, m, out="resid")
    if subset is not None:
        te = subset(te)
    pos, neg = te.filter(pl.col("is_foreign")), te.filter(~pl.col("is_foreign"))
    return {"raw_test": cal.auc(pos[target], neg[target]),
            "resid_test": cal.auc(pos["resid"], neg["resid"]),
            "n_test": te.height, "n_pos_seats": pos["broker"].n_unique()}


def main() -> None:
    from v3_common import cosines
    base = _labelled(cosines([YEAR]))     # m2 物化表,不讀舊快取
    print("=" * 78 + "\n[修正] raw 與殘差在同一母體(測試期)上比較\n" + "=" * 78)
    for side in ("buy", "sell"):
        r = test_auc(base, f"cos_foreign_{side}", side)
        print(f"  foreign {side}: raw(test)={r['raw_test']:.3f} → resid(test)={r['resid_test']:.3f}")

    print("\n" + "=" * 78 + "\n[§8.3-1/2] 剔除 2330 / 剔除當日成交前五(全年重建 cosine)\n" + "=" * 78)
    variants = {
        "no2330": lambda mkt: mkt.filter(pl.col("symbol_id") != "2330").select("symbol_id", "date"),
        "notop5": lambda mkt: (mkt.with_columns(pl.col("m").rank(descending=True).over("date").alias("_r"))
                               .filter(pl.col("_r") > 5).select("symbol_id", "date")),
    }
    for name, rule in variants.items():
        v = _labelled(build_variant(YEAR, name, rule))
        for side in ("buy", "sell"):
            r = test_auc(v, f"cos_foreign_{side}", side)
            print(f"  [{name}] foreign {side}: resid(test)={r['resid_test']:.3f} (raw {r['raw_test']:.3f})")

    print("\n" + "=" * 78 + "\n[§8.3-4] 小額席位是否仍成立(測試期,按席位中位 gross 切下半)\n" + "=" * 78)
    med = base.group_by("broker").agg(pl.col("gross_amt").median().alias("g"))
    cut = med["g"].median()
    small = set(med.filter(pl.col("g") <= cut)["broker"].to_list())
    for side in ("buy", "sell"):
        r = test_auc(base, f"cos_foreign_{side}", side,
                     subset=lambda te: te.filter(pl.col("broker").is_in(list(small))))
        print(f"  小額席位 foreign {side}: resid(test)={r['resid_test']:.3f} "
              f"(n={r['n_test']:,},外資席位 {r['n_pos_seats']} 家)")
    # 更嚴:只留小額外資台當正樣本(匯豐/法巴/大和/麥格理/犇亞已排除)
    small_foreign = {"8960", "8900", "8890", "1360"}
    for side in ("buy", "sell"):
        r = test_auc(base, f"cos_foreign_{side}", side,
                     subset=lambda te: te.filter(~pl.col("is_foreign")
                                                 | pl.col("broker").is_in(list(small_foreign))))
        print(f"  只留 4 家小額外資台為正樣本 {side}: resid(test)={r['resid_test']:.3f}")

    print("\n" + "=" * 78 + "\n[§8.3-5] 不同月份(測試期逐月 AUC,殘差係數固定用訓練期)\n" + "=" * 78)
    for side in ("buy", "sell"):
        ctrl = CONTROLS(side)
        sub = base.filter(pl.all_horizontal([pl.col(c).is_not_null() for c in [f"cos_foreign_{side}", *ctrl]]))
        tr, te = cal.time_split(sub, train_frac=2 / 3)
        te = cal.apply_residual(te, cal.fit_residual(tr, target=f"cos_foreign_{side}", controls=ctrl), out="resid")
        te = te.with_columns(pl.col("date").dt.month().alias("mo"))
        for mo in sorted(te["mo"].unique().to_list()):
            g = te.filter(pl.col("mo") == mo)
            a = cal.auc(g.filter(pl.col("is_foreign"))["resid"], g.filter(~pl.col("is_foreign"))["resid"])
            print(f"  {side} 2026-{mo:02d}: resid AUC={a:.3f} (n={g.height:,})")

    print("\n" + "=" * 78 + "\n[卡四修正] prop_hedge 發行商比較:raw → 殘差(控 size/breadth/market)\n" + "=" * 78)
    for side in ("buy", "sell"):
        target, ctrl = f"cos_prop_hedge_{side}", CONTROLS(side)
        sub = base.filter(pl.all_horizontal([pl.col(c).is_not_null() for c in [target, *ctrl]]))
        tr, te = cal.time_split(sub, train_frac=2 / 3)
        te = cal.apply_residual(te, cal.fit_residual(tr, target=target, controls=ctrl), out="resid")
        te = te.with_columns(pl.col("broker").is_in(list(WARRANT_ISSUER_HQ)).alias("issuer"))
        iss = te.filter(pl.col("issuer")); oth = te.filter(~pl.col("issuer") & ~pl.col("is_foreign"))
        print(f"  {side}: raw 差 {iss[target].mean() - oth[target].mean():+.3f} (AUC {cal.auc(iss[target], oth[target]):.3f})"
              f" → 殘差差 {iss['resid'].mean() - oth['resid'].mean():+.3f} (AUC {cal.auc(iss['resid'], oth['resid']):.3f})"
              f";發行商 log_gross 中位 {iss['log_gross'].median():.2f} vs 其他 {oth['log_gross'].median():.2f}")
        # 同規模對照:只拿 log_gross 落在發行商區間內的其他席位比
        lo, hi = iss["log_gross"].quantile(0.1), iss["log_gross"].quantile(0.9)
        peer = oth.filter(pl.col("log_gross").is_between(lo, hi))
        print(f"      同規模對照(log_gross ∈ [{lo:.2f},{hi:.2f}],n={peer.height:,}):"
              f"殘差差 {iss['resid'].mean() - peer['resid'].mean():+.3f} (AUC {cal.auc(iss['resid'], peer['resid']):.3f})")


if __name__ == "__main__":
    main()
