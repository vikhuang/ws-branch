"""Broker taxonomy v2 Q3:時間穩定性 — cluster 標籤跨年是否穩定?

方法:
1. 把 broker × day features 切成三段:2021-2022 / 2023 / 2024-2025H1
2. 對每段 rebuild broker profile + cluster
3. Cross-tab:同一 broker 在三段是否被歸到同一 cluster?
4. 特別:C1(虎尾幫)是不是 2021 就存在,還是後來變的?

輸出:experiments/broker_taxonomy/time_stability_analysis.md
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import datetime

REPO = Path(__file__).resolve().parents[2]
OUT_DIR = REPO / "experiments/broker_taxonomy"
FEATURES_PATH = OUT_DIR / "broker_day_features.parquet"
STABILITY_MD = OUT_DIR / "time_stability_analysis.md"

FEATURE_COLS = [
    "n_symbols_median", "shares_per_symbol_median",
    "median_row_shares_median", "max_row_concentration_median",
    "top1_symbol_concentration_median", "top5_symbol_concentration_median",
    "n_rows_median", "total_shares_median",
]
LOG_TRANSFORM = {"shares_per_symbol_median", "median_row_shares_median",
                 "n_rows_median", "total_shares_median"}

PERIODS = [
    ("2021-2022", datetime.date(2021, 1, 1), datetime.date(2022, 12, 31)),
    ("2023-2024H1", datetime.date(2023, 1, 1), datetime.date(2024, 6, 30)),
    ("2024H2-2025H1", datetime.date(2024, 7, 1), datetime.date(2025, 8, 31)),
]


def _build_profile(features_period: pl.DataFrame) -> pl.DataFrame:
    metrics = ["n_rows", "total_shares", "median_row_shares",
               "n_symbols", "shares_per_symbol", "max_row_concentration",
               "top1_symbol_concentration", "top5_symbol_concentration"]
    aggs = [pl.col("date").count().alias("active_days")]
    for m in metrics:
        aggs.append(pl.col(m).median().alias(f"{m}_median"))
    return (features_period
            .group_by("broker_name")
            .agg(*aggs))


def _standardize(X: np.ndarray) -> np.ndarray:
    mean = np.nanmean(X, axis=0); std = np.nanstd(X, axis=0)
    std[std < 1e-9] = 1.0
    return (X - mean) / std


def _kmeans(X: np.ndarray, k: int, seed: int = 20260706, n_iter: int = 100):
    rng = np.random.default_rng(seed)
    n, d = X.shape
    centroids = np.zeros((k, d))
    centroids[0] = X[rng.integers(0, n)]
    for i in range(1, k):
        dist2 = np.min(np.sum((X[:, None, :] - centroids[:i][None, :, :]) ** 2, axis=2), axis=1)
        prob = dist2 / (dist2.sum() + 1e-9)
        centroids[i] = X[rng.choice(n, p=prob)]
    labels = np.zeros(n, dtype=int)
    for _ in range(n_iter):
        d2 = np.sum((X[:, None, :] - centroids[None, :, :]) ** 2, axis=2)
        new_labels = np.argmin(d2, axis=1)
        if np.all(new_labels == labels): break
        labels = new_labels
        for j in range(k):
            mask = labels == j
            if mask.sum() > 0: centroids[j] = X[mask].mean(axis=0)
    return labels, centroids


def cluster_period(features_period: pl.DataFrame, k: int = 4) -> pl.DataFrame:
    """Cluster only branch_dash + active in period,return DataFrame with cluster label。"""
    profiles = _build_profile(features_period)
    # 只 cluster 有名字含 dash + 不含 "-自營"/"-經紀"/"-法人" 的 brokers
    profiles = profiles.filter(
        pl.col("broker_name").str.contains("-")
        & ~pl.col("broker_name").str.contains("-自營")
        & ~pl.col("broker_name").str.contains("-經紀")
        & ~pl.col("broker_name").str.contains("-法人")
        & (pl.col("active_days") >= 20)  # 期內至少 20 天
    )
    if profiles.height < 10:
        return pl.DataFrame({"broker_name": [], "cluster": []})

    for col in LOG_TRANSFORM:
        profiles = profiles.with_columns(
            (pl.col(col).clip(lower_bound=1.0)).log10().alias(col)
        )
    X = profiles.select(FEATURE_COLS).to_numpy()
    mask = ~np.isnan(X).any(axis=1)
    X = X[mask]; profiles = profiles.filter(pl.Series(mask))
    Xz = _standardize(X)
    labels, cent = _kmeans(Xz, k)

    # 決定哪個 cluster 對應「藏單型」(max_row_concentration_median 最高的 cluster)
    cent_orig = cent * np.nanstd(X, axis=0) + np.nanmean(X, axis=0)
    max_conc_idx = np.argmax([cent_orig[i, FEATURE_COLS.index("max_row_concentration_median")]
                              for i in range(k)])

    return profiles.select(["broker_name"]).with_columns(
        pl.Series("cluster", labels),
        pl.Series("is_c1_like", labels == max_conc_idx),  # 該期的「C1-like」
    )


def main() -> None:
    features = pl.read_parquet(FEATURES_PATH)
    lines = ["# Broker Cohort Time Stability\n"]
    lines.append("方法:對三個時期各自 cluster,追蹤同 broker 是否穩定屬「C1-like」")
    lines.append("(當期 max_row_concentration_median 最高的 cluster)。\n")

    period_labels: dict[str, pl.DataFrame] = {}
    for pname, lo, hi in PERIODS:
        sub = features.filter((pl.col("date") >= lo) & (pl.col("date") <= hi))
        n_days = sub["date"].n_unique()
        labeled = cluster_period(sub)
        n_c1 = labeled["is_c1_like"].sum() if labeled.height > 0 else 0
        lines.append(f"### {pname}(n_days={n_days},n_brokers={labeled.height},"
                     f"C1-like={n_c1})")
        period_labels[pname] = labeled

    # v1 完整 clustering(sample 全期)的 C1
    v1_clusters = pl.read_parquet(OUT_DIR / "broker_clusters.parquet")
    v1_c1 = set(v1_clusters.filter(pl.col("cluster") == 1)["broker_name"].to_list())
    lines.append(f"\n## v1(全期)C1 = {len(v1_c1)} brokers")

    # 各時期 C1-like 名單
    for pname, df in period_labels.items():
        c1_brokers = set(df.filter(pl.col("is_c1_like"))["broker_name"].to_list())
        overlap = c1_brokers & v1_c1
        lines.append(f"\n### {pname} C1-like ({len(c1_brokers)}) 與 v1-C1 重疊: {len(overlap)}/{len(v1_c1)}")
        lines.append(f"重疊 brokers: {sorted(overlap)[:15]}")
        new_in_period = c1_brokers - v1_c1
        lines.append(f"該期出現但不在 v1-C1: {sorted(new_in_period)[:10]}")

    # v1 C1 brokers 在三期的表現
    lines.append("\n## v1 的 25 個 C1 brokers 跨三期歷史")
    lines.append("| broker | 2021-2022 C1? | 2023-2024H1 C1? | 2024H2-2025H1 C1? |")
    lines.append("|--------|---------------|-----------------|-------------------|")
    for broker in sorted(v1_c1):
        row = f"| {broker} |"
        for pname in ["2021-2022", "2023-2024H1", "2024H2-2025H1"]:
            df = period_labels[pname]
            in_c1 = broker in df.filter(pl.col("is_c1_like"))["broker_name"].to_list()
            in_data = broker in df["broker_name"].to_list()
            if not in_data:
                row += " (n/a) |"
            else:
                row += " ✓ |" if in_c1 else " ✗ |"
        lines.append(row)

    STABILITY_MD.write_text("\n".join(lines))
    print(f"Written {STABILITY_MD}")
    print("\n" + "\n".join(lines))


if __name__ == "__main__":
    main()
