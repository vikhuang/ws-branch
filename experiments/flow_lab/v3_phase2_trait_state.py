"""v3 Phase 2:trait / 共同日狀態 / 席位特有異常(架構文件 §6)。

預期見 findings/v3_phase2_trait_state.md(已凍結)。輸入 = Phase 1 產出的
gated primitives(/tmp/v3_phase1_primitives.parquet),沒有就自動重算。
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from ws_branch.measure import decompose

PRIM = Path("/tmp/v3_phase1_primitives.parquet")
SIZE_EDGES = [8.0, 8.5, 9.0, 9.3, 9.7]          # log10(gross_amt),6 箱
BREADTH_EDGES = [100, 300, 500, 700, 900, 1200]  # n_symbols,7 箱
TARGETS = ["top5_share", "directional_ratio", "cos_market_buy", "cos_market_sell"]


def _corr(a: pl.Series, b: pl.Series) -> float:
    return pl.DataFrame({"a": a, "b": b}).select(pl.corr("a", "b")).item()


def _load() -> pl.DataFrame:
    if not PRIM.exists():
        from v3_phase1_geometry import _gated_primitives
        df = _gated_primitives(2026)
        df = df.with_columns(pl.col("gross_amt").log10().alias("log_gross"),
                             pl.col("n_symbols").log10().alias("log_n"))
        df.write_parquet(PRIM)
    return pl.read_parquet(PRIM)


def main() -> None:
    df = _load()
    df = decompose.add_conditioning_bins(
        df, size_col="log_gross", breadth_col="n_symbols",
        size_edges=SIZE_EDGES, breadth_edges=BREADTH_EDGES)
    print(f"分點日 {df.height:,};分點 {df['broker'].n_unique()};"
          f"交易日 {df['date'].n_unique()};條件化格數 {df['_cell'].n_unique()}")
    cell_n = df.group_by("_cell").len()["len"]
    print(f"  每格樣本數 min/median/max = {cell_n.min()}/{int(cell_n.median())}/{cell_n.max()}")

    results = {}
    for y in TARGETS:
        print("\n" + "=" * 72 + f"\n{y}\n" + "=" * 72)
        out = decompose.decompose(df, y=y)
        results[y] = out
        s = out.var_shares
        print(f"  n={out.n_obs:,} 席位={out.n_branch} 日={out.n_day} "
              f"迭代={out.iterations} 總變異={out.var_total:.5f}")
        print(f"  成分自身變異/總變異(非正交,不加總):f={s['f']:.1%} "
              f"α={s['alpha']:.1%} γ={s['gamma']:.1%} ε={s['resid']:.1%}")
        a = decompose.sequential_r2(df, y=y, order=["f", "alpha", "gamma"])
        b = decompose.sequential_r2(df, y=y, order=["gamma", "alpha", "f"])
        print(f"  順序 A(f→α→γ)增量 R²:f={a['f']:.1%} α={a['alpha']:.1%} "
              f"γ={a['gamma']:.1%} 殘差={a['residual']:.1%}")
        print(f"  順序 B(γ→α→f)增量 R²:γ={b['gamma']:.1%} α={b['alpha']:.1%} "
              f"f={b['f']:.1%} 殘差={b['residual']:.1%}")
        print(f"  順序敏感度:f {abs(a['f']-b['f']):.1%}、α {abs(a['alpha']-b['alpha']):.1%}、"
              f"γ {abs(a['gamma']-b['gamma']):.1%}")

        # E5:殘差的分點內自相關(α 有沒有吃乾淨)
        r = (out.frame.sort("broker", "date")
             .with_columns(pl.col("resid").shift(1).over("broker").alias("lag"),
                           pl.len().over("broker").alias("nd"))
             .filter((pl.col("nd") >= 60) & pl.col("lag").is_not_null()))
        print(f"  ε 分點內 lag-1 自相關 = {_corr(r['resid'], r['lag']):.3f}"
              f"  (原始 y 的 lag-1 = ", end="")
        r0 = (df.filter(pl.col(y).is_not_null()).sort("broker", "date")
              .with_columns(pl.col(y).shift(1).over("broker").alias("lag"),
                            pl.len().over("broker").alias("nd"))
              .filter((pl.col("nd") >= 60) & pl.col("lag").is_not_null()))
        print(f"{_corr(r0[y], r0['lag']):.3f})")

        # 敏感度:≥60 活躍日子集 + 流量加權
        sub = df.with_columns(pl.len().over("broker").alias("_nd")).filter(pl.col("_nd") >= 60)
        o2 = decompose.decompose(sub, y=y)
        print(f"  敏感度(≥60 活躍日,n={o2.n_obs:,}):f={o2.var_shares['f']:.1%} "
              f"α={o2.var_shares['alpha']:.1%} γ={o2.var_shares['gamma']:.1%} "
              f"ε={o2.var_shares['resid']:.1%}")

    print("\n" + "=" * 72 + "\nE3:γ_t 是不是有意義的市場狀態\n" + "=" * 72)
    mkt = df.group_by("date").agg(pl.col("gross_amt").sum().alias("mkt_gross"),
                                  pl.len().alias("n_active"))
    for y in TARGETS:
        g = (results[y].frame.group_by("date").agg(pl.col("gamma").mean().alias("g"))
             .join(mkt, on="date"))
        print(f"  {y:<18} γ 的 sd={g['g'].std():.4f};"
              f"corr(γ, 當日市場量)={_corr(g['g'].rank(), g['mkt_gross'].rank()):+.3f};"
              f"corr(γ, 活躍席位數)={_corr(g['g'].rank(), g['n_active'].rank()):+.3f}")
    g = (results["directional_ratio"].frame.group_by("date")
         .agg(pl.col("gamma").mean().alias("g")).join(mkt, on="date").sort("g", descending=True))
    print("\n  directional_ratio 的 γ 最高 5 日(分點淨部位最分歧):")
    print(g.head(5).select("date", pl.col("g").round(4),
                           (pl.col("mkt_gross") / 1e12).round(2).alias("市場量兆")))
    print("  最低 5 日:")
    print(g.tail(5).select("date", pl.col("g").round(4),
                           (pl.col("mkt_gross") / 1e12).round(2).alias("市場量兆")))

    out = pl.concat([results[y].frame.select(
        "broker", "date", pl.lit(y).alias("primitive"),
        pl.col(y).alias("raw"), "f_hat", "alpha", "gamma", "resid")
        for y in TARGETS])
    out.write_parquet("/tmp/v3_phase2_components.parquet")
    print(f"\n成分表 → /tmp/v3_phase2_components.parquet ({out.height:,} 列)")


if __name__ == "__main__":
    main()
