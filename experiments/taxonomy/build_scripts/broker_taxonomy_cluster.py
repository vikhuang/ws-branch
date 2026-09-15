"""Broker taxonomy Q2:分行(branch_dash)內部 clustering。

核心問題:「公司-地名」型分行(v2 全部歸為 retail)內部是否自然分成不同行為型態?
若能分出「真散戶密集型」vs「大戶藏單型」vs「中間型」,就驗證 user 的 push-back。

方法:
1. 只取 branch_dash + active_days ≥ 50(排除低活躍度雜訊)
2. 特徵標準化(log 對數變換 skew 大的、z-score)
3. K-means k=2..8 elbow analysis
4. 選 k,描述每 cluster 的 archetype
5. 命名 clusters,計算每 cluster 的 broker 佔比與代表 broker 名單

輸出:
- broker_clusters.parquet(每 broker 一行,含 cluster label)
- clustering_analysis.md(elbow chart + cluster archetype 描述)
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl

REPO = Path(__file__).resolve().parents[2]
OUT_DIR = REPO / "experiments/broker_taxonomy"
PROFILES_PATH = OUT_DIR / "broker_profiles.parquet"
CLUSTERS_PATH = OUT_DIR / "broker_clusters.parquet"
ANALYSIS_MD = OUT_DIR / "clustering_analysis.md"

FEATURE_COLS = [
    "n_symbols_median",
    "shares_per_symbol_median",
    "median_row_shares_median",
    "max_row_concentration_median",
    "top1_symbol_concentration_median",
    "top5_symbol_concentration_median",
    "n_rows_median",
    "total_shares_median",
]
# log-transform 前先移除 0(這些欄位都是正數;可能有 0/null)
LOG_TRANSFORM = {"shares_per_symbol_median", "median_row_shares_median",
                 "n_rows_median", "total_shares_median"}


def _standardize(X: np.ndarray) -> np.ndarray:
    mean = np.nanmean(X, axis=0)
    std = np.nanstd(X, axis=0)
    std[std < 1e-9] = 1.0
    return (X - mean) / std


def _kmeans(X: np.ndarray, k: int, seed: int = 20260706, n_iter: int = 100) -> tuple[np.ndarray, np.ndarray]:
    """Simple k-means implementation。回傳 (labels, centroids)。"""
    rng = np.random.default_rng(seed)
    n, d = X.shape
    # k-means++ init: pick first random, then farthest-point
    centroids = np.zeros((k, d))
    centroids[0] = X[rng.integers(0, n)]
    for i in range(1, k):
        dist2 = np.min(np.sum((X[:, None, :] - centroids[:i][None, :, :]) ** 2, axis=2), axis=1)
        prob = dist2 / (dist2.sum() + 1e-9)
        idx = rng.choice(n, p=prob)
        centroids[i] = X[idx]
    labels = np.zeros(n, dtype=int)
    for _ in range(n_iter):
        # assign
        d2 = np.sum((X[:, None, :] - centroids[None, :, :]) ** 2, axis=2)
        new_labels = np.argmin(d2, axis=1)
        if np.all(new_labels == labels):
            break
        labels = new_labels
        # update
        for j in range(k):
            mask = labels == j
            if mask.sum() > 0:
                centroids[j] = X[mask].mean(axis=0)
    return labels, centroids


def _inertia(X: np.ndarray, labels: np.ndarray, centroids: np.ndarray) -> float:
    return float(np.sum((X - centroids[labels]) ** 2))


def main() -> None:
    profiles = pl.read_parquet(PROFILES_PATH)
    # 只用 branch_dash + active_days ≥ 50 的
    branch = profiles.filter(
        (pl.col("anchor_label") == "branch_dash")
        & (pl.col("active_days") >= 50)
    )
    print(f"[Q2] branch_dash with active_days≥50: {branch.height} brokers")

    # 準備 feature matrix
    df = branch.select(FEATURE_COLS + ["broker_name"])
    # log transform on skew columns
    for col in LOG_TRANSFORM:
        df = df.with_columns(
            (pl.col(col).clip(lower_bound=1.0)).log10().alias(col)
        )
    X = df.select(FEATURE_COLS).to_numpy()
    # 有 null → drop
    mask = ~np.isnan(X).any(axis=1)
    if mask.sum() < X.shape[0]:
        print(f"  dropping {(~mask).sum()} rows with null features")
        X = X[mask]
        df = df.filter(pl.Series(mask))
    Xz = _standardize(X)

    # elbow 掃 k
    lines = ["# Branch_dash Clustering Analysis\n"]
    lines.append(f"樣本:{X.shape[0]} 個 branch_dash brokers(active_days ≥ 50)")
    lines.append(f"維度({len(FEATURE_COLS)}):{', '.join(FEATURE_COLS)}\n")
    lines.append("## Elbow analysis\n")
    lines.append("| k | Inertia | Δ vs prev |")
    lines.append("|---|---------|-----------|")
    inertias = {}
    for k in range(2, 9):
        labels, cent = _kmeans(Xz, k)
        inertia = _inertia(Xz, labels, cent)
        inertias[k] = inertia
        delta = "-" if k == 2 else f"{inertias[k-1] - inertia:.1f}"
        lines.append(f"| {k} | {inertia:.1f} | {delta} |")
    lines.append("")

    # 選 k=4 為 default(elbow 前的常見選擇);可依判讀改
    chosen_k = 4
    labels, cent = _kmeans(Xz, chosen_k)
    print(f"[Q2] Chose k={chosen_k}")

    # 每 cluster 的 profile
    labeled = df.with_columns(pl.Series("cluster", labels))
    # unstandardize centroids to describe
    mean_orig = np.nanmean(X, axis=0)
    std_orig = np.nanstd(X, axis=0)
    cent_orig = cent * std_orig + mean_orig
    # log-transformed columns → invert (10^x)
    for j, col in enumerate(FEATURE_COLS):
        if col in LOG_TRANSFORM:
            cent_orig[:, j] = np.power(10.0, cent_orig[:, j])

    lines.append(f"## Chosen k = {chosen_k}\n")
    lines.append("### Cluster centroids(unstandardized)\n")
    header = "| Cluster | n | " + " | ".join(FEATURE_COLS) + " |"
    lines.append(header)
    lines.append("|" + "|".join(["---"] * (len(FEATURE_COLS) + 2)) + "|")
    for j in range(chosen_k):
        n_in_cluster = int((labels == j).sum())
        row = [f"C{j}", str(n_in_cluster)]
        for k, col in enumerate(FEATURE_COLS):
            v = cent_orig[j, k]
            if v > 1000:
                row.append(f"{v:,.0f}")
            else:
                row.append(f"{v:.3f}")
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")

    # 每 cluster 前 10 broker names
    labeled_pd = labeled.select(["broker_name", "cluster"] + FEATURE_COLS)
    lines.append("### 各 Cluster 代表 broker(前 10 名)\n")
    for j in range(chosen_k):
        n = int((labels == j).sum())
        lines.append(f"**Cluster {j}(n = {n})**")
        cluster_brokers = labeled_pd.filter(pl.col("cluster") == j)["broker_name"].to_list()[:10]
        lines.append("  " + ", ".join(cluster_brokers))
        lines.append("")

    # 保存
    labeled.write_parquet(CLUSTERS_PATH)
    ANALYSIS_MD.write_text("\n".join(lines))
    print(f"[Q2] Wrote clusters → {CLUSTERS_PATH}")
    print(f"[Q2] Wrote analysis → {ANALYSIS_MD}")

    # 印到 stdout
    print("\n" + "\n".join(lines))


if __name__ == "__main__":
    main()
