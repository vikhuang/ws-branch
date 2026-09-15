"""Broker taxonomy Q1.5 + Q5:profile 聚合 + 錨點 cross-validation。

Q1.5:broker × day features → broker profile(每 broker 一行,多維穩定 fingerprint)
Q5:已知 institutional / prop 錨點在各維度是否呈現顯著差異(t-test / KS test)

輸出:
- experiments/broker_taxonomy/broker_profiles.parquet
- experiments/broker_taxonomy/anchor_validation.md(表格 + 判讀)
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl

REPO = Path(__file__).resolve().parents[2]
OUT_DIR = REPO / "experiments/broker_taxonomy"
FEATURES_PATH = OUT_DIR / "broker_day_features.parquet"
PROFILES_PATH = OUT_DIR / "broker_profiles.parquet"
ANCHOR_MD = OUT_DIR / "anchor_validation.md"


def _classify_anchor(name: str) -> str:
    """已知 anchor 分類(僅供 cross-validation,非分類 spec)。"""
    if name is None:
        return "unknown"
    if any(name.startswith(p) for p in ("港商", "美商", "日商", "新加坡商", "法銀")):
        return "foreign_institutional"
    if name in {"摩根大通", "美林", "花旗環球", "台灣摩根士丹利",
                "香港上海匯豐", "犇亞證券", "大和國泰"}:
        return "foreign_institutional"
    if "-自營" in name:
        return "prop_desk"
    if "-經紀" in name or "-法人" in name:
        return "aggregator_institutional"
    if name in {"元大期貨", "群益期貨"}:
        return "futures"
    if "-" in name:
        return "branch_dash"  # 帶「-地名」的分行(未細分)
    return "hq_or_other"


def build_profiles(df: pl.DataFrame) -> pl.DataFrame:
    """聚合 broker×day → broker profile。用 median 而非 mean 更 robust。"""
    metrics = ["n_rows", "total_shares", "mean_row_shares", "median_row_shares",
               "n_symbols", "shares_per_symbol", "max_row_concentration",
               "top1_symbol_concentration", "top5_symbol_concentration",
               "aggregate_share"]
    aggs = [pl.col("date").count().alias("active_days")]
    for m in metrics:
        aggs.extend([
            pl.col(m).median().alias(f"{m}_median"),
            pl.col(m).std().alias(f"{m}_std"),
        ])
    profiles = (df
        .group_by("broker_name")
        .agg(*aggs)
    )
    # 加 anchor label
    profiles = profiles.with_columns(
        pl.col("broker_name").map_elements(_classify_anchor, return_dtype=pl.String)
        .alias("anchor_label")
    )
    return profiles.sort("active_days", descending=True)


def ks_stat(a: np.ndarray, b: np.ndarray) -> tuple[float, float]:
    """簡化的兩樣本 KS 統計量。回傳 (D, approx_p)。"""
    a = np.sort(a[~np.isnan(a)])
    b = np.sort(b[~np.isnan(b)])
    na, nb = len(a), len(b)
    if na == 0 or nb == 0:
        return float("nan"), float("nan")
    combined = np.concatenate([a, b])
    cdf_a = np.searchsorted(a, combined, side="right") / na
    cdf_b = np.searchsorted(b, combined, side="right") / nb
    D = float(np.max(np.abs(cdf_a - cdf_b)))
    # Kolmogorov approximate p
    en = np.sqrt(na * nb / (na + nb))
    lam = (en + 0.12 + 0.11 / en) * D
    p = 2 * np.exp(-2 * lam ** 2)
    return D, min(p, 1.0)


def anchor_validate(profiles: pl.DataFrame) -> str:
    """對每個 anchor group vs branch_dash group 做 KS test,回報統計量。"""
    lines = ["# Anchor Cross-Validation Report\n"]
    lines.append("目的:驗證行為維度是否能區隔已知 institutional / prop vs 一般分行(branch_dash)。")
    lines.append("若已知 anchor 在特徵上與 branch_dash 差異微弱,說明維度定義有問題。\n")

    groups = {
        "foreign_institutional": profiles.filter(pl.col("anchor_label") == "foreign_institutional"),
        "prop_desk": profiles.filter(pl.col("anchor_label") == "prop_desk"),
        "aggregator_institutional": profiles.filter(pl.col("anchor_label") == "aggregator_institutional"),
        "futures": profiles.filter(pl.col("anchor_label") == "futures"),
        "hq_or_other": profiles.filter(pl.col("anchor_label") == "hq_or_other"),
    }
    baseline = profiles.filter(pl.col("anchor_label") == "branch_dash")
    lines.append(f"**Baseline(branch_dash「公司-地名」):n = {baseline.height}**\n")

    feature_cols = [
        "n_symbols_median", "shares_per_symbol_median",
        "median_row_shares_median", "mean_row_shares_median",
        "max_row_concentration_median", "top1_symbol_concentration_median",
        "top5_symbol_concentration_median", "n_rows_median", "total_shares_median",
        "active_days", "aggregate_share_median",
    ]

    for gname, g in groups.items():
        if g.height == 0:
            lines.append(f"### {gname}: n=0(空 group)\n")
            continue
        lines.append(f"### {gname}(n = {g.height})\n")
        lines.append("| Feature | Anchor median | Branch median | Anchor/Branch ratio | KS D | KS approx p |")
        lines.append("|---------|---------------|---------------|---------------------|------|-------------|")
        for col in feature_cols:
            a = g[col].to_numpy()
            b = baseline[col].to_numpy()
            am = float(np.nanmedian(a))
            bm = float(np.nanmedian(b))
            ratio = am / bm if abs(bm) > 1e-9 else float("nan")
            D, p = ks_stat(a, b)
            lines.append(f"| {col} | {am:,.4g} | {bm:,.4g} | {ratio:.3f} | {D:.3f} | {p:.4g} |")
        lines.append("")

    # 判讀
    lines.append("## 判讀\n")
    lines.append("**理想結果**:foreign_institutional / prop_desk 至少在 shares_per_symbol、")
    lines.append("max_row_concentration、n_symbols 上有明顯差異(KS p < 0.01, ratio 遠離 1)。")
    lines.append("aggregator_institutional 應該在 n_rows / total_shares 上偏低或偏高(-經紀通常聚合單少)。")
    lines.append("若差異微弱 → 目前維度不足以識別 anchor → 需要加更多維度(時序、跨股集中度、日間變異)。")
    return "\n".join(lines)


def main() -> None:
    df = pl.read_parquet(FEATURES_PATH)
    print(f"[Q1.5] Loaded {len(df)} broker×day rows, {df['broker_name'].n_unique()} brokers")

    profiles = build_profiles(df)
    profiles.write_parquet(PROFILES_PATH)
    print(f"[Q1.5] Wrote {profiles.height} broker profiles → {PROFILES_PATH}")

    # 印錨點 count 分布
    print("\nAnchor label counts:")
    print(profiles.group_by("anchor_label").agg(pl.len().alias("count")).sort("count", descending=True))

    md = anchor_validate(profiles)
    ANCHOR_MD.write_text(md)
    print(f"\n[Q5] Anchor validation → {ANCHOR_MD}")
    # 也印到 stdout
    print("\n" + md)


if __name__ == "__main__":
    main()
