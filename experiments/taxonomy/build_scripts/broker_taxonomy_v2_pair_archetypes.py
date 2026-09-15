"""Broker taxonomy v2 Q4b:對 broker × symbol pair 分類 archetype。

觀察發現 pair 有多種 signature 混雜:
- 機構日常持倉 (institutional_daily):摩根大通-2330,persistence=1.0,concentration~12%
- 專營做股 (dedicated_making):京城-嘉義-9919,persistence 60%,market_share 29%
- 偶發大戶單 (occasional_whale):元大-忠孝鼎富-6813,5 天但 market_share 59%
- 當沖/隔日沖 (day_trade_regular):永豐金-潮州-00665L,反向 ETF 頻繁

用三維(persistence / concentration / market_share)分類 pair。
"""
from __future__ import annotations

from pathlib import Path

import polars as pl

REPO = Path(__file__).resolve().parents[2]
OUT_DIR = REPO / "experiments/broker_taxonomy"
PAIRS_PATH = OUT_DIR / "broker_symbol_pairs.parquet"
ARCHETYPE_PATH = OUT_DIR / "broker_symbol_archetypes.parquet"
ARCHETYPE_MD = OUT_DIR / "pair_archetypes_analysis.md"


def classify_pair(row: dict) -> str:
    """Pair archetype classifier(pre-declared,可用於後續 v2 rerun)。

    優先順序:institutional_daily > dedicated_making > occasional_whale
             > day_trade_regular > minor
    """
    p = row["persistence"]
    c = row["median_day_concentration"]
    m = row["median_day_market_share"]
    days = row["pair_days"]

    # 至少要有 3 天,否則太雜訊
    if days < 3:
        return "minor"

    # 機構日常持倉:天天在做 + 高集中度(自己主要業務)+ 中等以上 market share
    if p >= 0.85 and c >= 0.05:
        return "institutional_daily"

    # 專營做股:高 persistence + 高 market share + 至少中等 concentration
    if p >= 0.40 and m >= 0.05 and c >= 0.015:
        return "dedicated_making"

    # 偶發大戶單:低 persistence 但當時 market share 極高(超過 15%)
    if p < 0.20 and m >= 0.15:
        return "occasional_whale"

    # 當沖/隔日沖 pattern:中等 persistence + 中等 market share + LOW concentration
    # (交易金額對 broker 來說不高但頻繁進出)
    if p >= 0.25 and m >= 0.02 and c < 0.02:
        return "day_trade_regular"

    return "minor"


def main() -> None:
    pairs = pl.read_parquet(PAIRS_PATH)
    print(f"Total pairs: {len(pairs)}")

    # 加 archetype
    pairs = pairs.with_columns(
        pl.struct(["persistence", "median_day_concentration",
                   "median_day_market_share", "pair_days"])
          .map_elements(classify_pair, return_dtype=pl.String)
          .alias("archetype")
    )
    pairs.write_parquet(ARCHETYPE_PATH)

    # 整體分布
    archetype_counts = (pairs
        .group_by("archetype")
        .agg(pl.len().alias("n_pairs"),
             pl.col("broker_name").n_unique().alias("n_brokers"))
        .sort("n_pairs", descending=True))
    print(f"\n=== Overall archetype distribution ===")
    print(archetype_counts)

    # 各 cluster 的 archetype 分布(row: cluster, col: archetype)
    print(f"\n=== 各 cluster 的 pair archetype 分布 ===")
    ct = (pairs
        .group_by(["cluster", "archetype"])
        .agg(pl.len().alias("n_pairs"))
        .sort(["cluster", "archetype"]))
    print(ct)

    # 每 cluster 有 dedicated_making 的 broker 名單
    lines = ["# Broker × Symbol Archetypes v2 Analysis\n"]
    lines.append(f"Total pairs: {len(pairs)}\n")

    for archetype in ["institutional_daily", "dedicated_making",
                       "occasional_whale", "day_trade_regular"]:
        subset = pairs.filter(pl.col("archetype") == archetype)
        n = len(subset)
        n_brokers = subset["broker_name"].n_unique()
        n_symbols = subset["symbol_id"].n_unique()
        lines.append(f"## {archetype}(n_pairs={n}, n_brokers={n_brokers}, n_symbols={n_symbols})\n")

        # 各 cluster 分布
        by_cluster = (subset.group_by("cluster").agg(pl.len().alias("n"))
                      .sort("cluster"))
        lines.append("### Cluster 分布")
        for row in by_cluster.iter_rows(named=True):
            c = row["cluster"] if row["cluster"] is not None else "null(機構等)"
            lines.append(f"- Cluster {c}: {row['n']} pairs")
        lines.append("")

        # top 15 pairs
        top = subset.sort("median_day_market_share", descending=True).head(15)
        lines.append("### Top 15 pairs by market share")
        lines.append("| broker | symbol | cluster | mkt_share | concentration | persistence | days |")
        lines.append("|--------|--------|---------|-----------|---------------|-------------|------|")
        for row in top.iter_rows(named=True):
            c_str = str(row["cluster"]) if row["cluster"] is not None else "-"
            lines.append(
                f"| {row['broker_name']} | {row['symbol_id']} | {c_str} | "
                f"{row['median_day_market_share']:.3f} | {row['median_day_concentration']:.3f} | "
                f"{row['persistence']:.2f} | {row['pair_days']} |"
            )
        lines.append("")

    # C1 專章
    lines.append("## C1(虎尾幫)pair 完整分析\n")
    c1_pairs = pairs.filter(pl.col("cluster") == 1)
    c1_arch = (c1_pairs.group_by("archetype").agg(pl.len().alias("n")).sort("archetype"))
    lines.append("### Archetype 分布")
    for row in c1_arch.iter_rows(named=True):
        lines.append(f"- {row['archetype']}: {row['n']}")
    lines.append("")

    # C1 top pairs by market share (any archetype)
    c1_top = (c1_pairs.filter(pl.col("archetype") != "minor")
              .sort("median_day_market_share", descending=True)
              .head(30))
    lines.append("### C1 top 30 impactful pairs")
    lines.append("| broker | symbol | archetype | mkt_share | concentration | persistence | days |")
    lines.append("|--------|--------|-----------|-----------|---------------|-------------|------|")
    for row in c1_top.iter_rows(named=True):
        lines.append(
            f"| {row['broker_name']} | {row['symbol_id']} | {row['archetype']} | "
            f"{row['median_day_market_share']:.3f} | {row['median_day_concentration']:.3f} | "
            f"{row['persistence']:.2f} | {row['pair_days']} |"
        )
    lines.append("")

    ARCHETYPE_MD.write_text("\n".join(lines))
    print(f"\nWritten to {ARCHETYPE_MD}")
    print("\n" + "\n".join(lines[:80]))  # 印前面


if __name__ == "__main__":
    main()
