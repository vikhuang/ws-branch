"""v3 Phase 1 §1:硬會計界限讀數(findings/v3_phase1_geometry.md §1 表的來源)。

2026-09-21 複查抓到:§1 那張表是建完 t3b 後在 REPL 手算的,repo 內沒有
程式能重現;其中「唯一識別(L=U)8.0%」的口徑也沒寫清楚。本腳本把每一列
的定義寫死,之後改 cohort / universe 版本時重跑即可。

口徑(全部只用 `bounds_publishable` 列;量加權 = 對該側所有股票日加總後相除):
- Other 佔比      = Σ other_actor_sh / Σ market_total_sh
- X 量加權區間    = [Σ foreign_x_lo, Σ foreign_x_hi] / Σ cohort_sh
- HiddenForeign   = Σ foreign_y_lo / Σ official_foreign_sh(下界)
- S/F             = Σ cohort_sh / Σ official_foreign_sh(**不是 coverage**)
- 未觀測          = Σ unobserved_sh / Σ market_total_sh
- L=U             = foreign_x_lo == foreign_x_hi 的股票日佔比;分「退化」(S=0 或
                    F=0,區間必為單點)與「非退化」兩種**分開報**,合計亦報
"""

from __future__ import annotations

import datetime

import polars as pl

from ws_branch.tables import io

YEAR = 2026
DEMO = ("3450", datetime.date(2026, 9, 14))


def readout(t3b: pl.DataFrame, side: str) -> dict[str, float]:
    p = t3b.filter((pl.col("side") == side) & pl.col("bounds_publishable"))
    s = {c: p[c].sum() for c in ("market_total_sh", "other_actor_sh", "cohort_sh",
                                 "official_foreign_sh", "foreign_x_lo", "foreign_x_hi",
                                 "foreign_y_lo", "unobserved_sh")}
    degenerate = (pl.col("cohort_sh") == 0) | (pl.col("official_foreign_sh") == 0)
    tight = pl.col("foreign_x_lo") == pl.col("foreign_x_hi")
    q = lambda col, k: p.filter(pl.col("market_total_sh") > 0).select(  # noqa: E731
        (pl.col(col) / pl.col("market_total_sh")).quantile(k)).item()
    return {
        "publishable_rate": p.height / t3b.filter(pl.col("side") == side).height,
        "other_share_w": s["other_actor_sh"] / s["market_total_sh"],
        "other_p25": q("other_actor_sh", .25), "other_p50": q("other_actor_sh", .5),
        "other_p75": q("other_actor_sh", .75),
        "x_lo_w": s["foreign_x_lo"] / s["cohort_sh"],
        "x_hi_w": s["foreign_x_hi"] / s["cohort_sh"],
        "cov_hi_med": p["foreign_cov_hi"].median(), "cov_lo_med": p["foreign_cov_lo"].median(),
        "hidden_foreign_w": s["foreign_y_lo"] / s["official_foreign_sh"],
        "hidden_foreign_med": p.filter(pl.col("official_foreign_sh") > 0)
                               .select((pl.col("foreign_y_lo") / pl.col("official_foreign_sh"))
                                       .median()).item(),
        "s_over_f": s["cohort_sh"] / s["official_foreign_sh"],
        "unobserved_w": s["unobserved_sh"] / s["market_total_sh"],
        "tight_all": p.filter(tight).height / p.height,
        "tight_degenerate": p.filter(tight & degenerate).height / p.height,
        "tight_nondegenerate": p.filter(tight & ~degenerate).height / p.height,
        "n_rows": p.height,
    }


def main() -> None:
    t3b = io.scan("t3b_accounting_bounds", start=f"{YEAR}-01-01",
                  end=f"{YEAR}-12-31").collect()
    print(f"t3b {YEAR}:{t3b.height:,} 列;cohort {t3b['cohort_version'][0]};"
          f"universe {t3b['universe_version'][0]}")
    for side in ("buy", "sell"):
        r = readout(t3b, side)
        print(f"\n[{side}] 可發布 {r['n_rows']:,} 列({r['publishable_rate']:.2%})")
        print(f"  Other 佔比   量加權 {r['other_share_w']:.1%};逐股票日 p25/p50/p75 = "
              f"{r['other_p25']:.2f}/{r['other_p50']:.2f}/{r['other_p75']:.2f}")
        print(f"  X 區間       量加權 [{r['x_lo_w']:.1%}, {r['x_hi_w']:.1%}];"
              f"cov_hi 中位 {r['cov_hi_med']:.3f}、cov_lo 中位 {r['cov_lo_med']:.3f}")
        print(f"  HiddenForeign 下界 量加權 {r['hidden_foreign_w']:.1%};中位 {r['hidden_foreign_med']:.1%}")
        print(f"  S/F          量加權 {r['s_over_f']:.3f}(不是 coverage)")
        print(f"  未觀測       量加權 {r['unobserved_w']:.2%}")
        print(f"  L=U 唯一識別 合計 {r['tight_all']:.1%} = 退化(S=0 或 F=0){r['tight_degenerate']:.1%}"
              f" + 非退化 {r['tight_nondegenerate']:.1%}")
    sym, d = DEMO
    demo = t3b.filter((pl.col("symbol_id") == sym) & (pl.col("date") == d)).sort("side")
    print(f"\n{sym} @ {d}:")
    print(demo.select("side", "cohort_sh", "official_foreign_sh", "foreign_y_lo",
                      "foreign_cov_hi", "bounds_publishable"))


if __name__ == "__main__":
    main()
