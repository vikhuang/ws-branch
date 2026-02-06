"""重構驗證腳本：確保功能不遺漏"""

import polars as pl
import numpy as np
from pathlib import Path


def verify_ranking_report(new_path: str = "ranking_report.parquet",
                          baseline_path: str = "baseline_v0.10.0/ranking_report.parquet") -> bool:
    """驗證排名報告與基準一致"""

    print("=" * 60)
    print("【排名報告驗證】")
    print("=" * 60)

    new_df = pl.read_parquet(new_path)
    baseline_df = pl.read_parquet(baseline_path)

    # 1. 筆數檢查
    assert len(new_df) == len(baseline_df), f"筆數不符: {len(new_df)} vs {len(baseline_df)}"
    print(f"✓ 筆數一致: {len(new_df)}")

    # 2. 欄位檢查
    assert set(new_df.columns) == set(baseline_df.columns), "欄位不符"
    print(f"✓ 欄位一致: {len(new_df.columns)} 欄")

    # 3. 關鍵數值檢查
    key_brokers = ["1440", "8440", "1470", "1380"]  # 美林, 摩根大通, 摩根士丹利, 台灣匯立

    for broker in key_brokers:
        new_row = new_df.filter(pl.col("broker") == broker)
        base_row = baseline_df.filter(pl.col("broker") == broker)

        if len(new_row) == 0 or len(base_row) == 0:
            continue

        new_pnl = new_row["total_pnl"].item()
        base_pnl = base_row["total_pnl"].item()

        # 允許浮點誤差 0.01%
        diff = abs(new_pnl - base_pnl) / abs(base_pnl) if base_pnl != 0 else 0
        assert diff < 0.0001, f"{broker} PNL 差異過大: {diff:.4%}"
        print(f"✓ {broker} PNL 一致: {new_pnl/1e8:.2f}億")

    # 4. 總和檢查
    new_total = new_df["total_pnl"].sum()
    base_total = baseline_df["total_pnl"].sum()
    diff = abs(new_total - base_total)
    assert diff < 1e6, f"總 PNL 差異過大: {diff}"
    print(f"✓ 總 PNL 一致: {new_total/1e8:.2f}億")

    print("\n✅ 所有驗證通過！")
    return True


def verify_zero_sum() -> bool:
    """驗證零和：已實現 + 未實現 ≈ 0"""

    print("\n" + "=" * 60)
    print("【零和檢驗】")
    print("=" * 60)

    realized = np.load("realized_pnl.npy")
    unrealized = np.load("unrealized_pnl.npy")

    total_realized = realized.sum()
    total_unrealized = unrealized[0, -1, :].sum()
    total = total_realized + total_unrealized

    print(f"已實現: {total_realized/1e8:+.2f}億")
    print(f"未實現: {total_unrealized/1e8:+.2f}億")
    print(f"合計: {total/1e8:+.4f}億")

    # 應該接近零（佔比 < 0.5%，考慮未平倉部位）
    ratio = abs(total) / abs(total_realized) if total_realized != 0 else 0
    assert ratio < 0.005, f"零和比例過大: {ratio:.4%}"

    print("\n✅ 零和檢驗通過！")
    return True


def verify_key_metrics() -> bool:
    """驗證關鍵指標"""

    print("\n" + "=" * 60)
    print("【關鍵指標驗證】")
    print("=" * 60)

    df = pl.read_parquet("ranking_report.parquet")

    # 美林指標
    merrill = df.filter(pl.col("broker") == "1440").row(0, named=True)

    checks = [
        ("美林排名", merrill["rank"], 1),
        ("美林方向", merrill["direction"], "做多"),
        ("美林已實現", round(merrill["realized_pnl"] / 1e8, 2), 97.84),
    ]

    for name, actual, expected in checks:
        assert actual == expected, f"{name}: {actual} != {expected}"
        print(f"✓ {name}: {actual}")

    # 執行 Alpha 範圍檢查
    exec_alpha = merrill["exec_alpha"]
    assert 0.001 < exec_alpha < 0.002, f"執行 Alpha 異常: {exec_alpha}"
    print(f"✓ 美林執行 Alpha: {exec_alpha*100:.4f}%")

    print("\n✅ 關鍵指標驗證通過！")
    return True


def main():
    """執行所有驗證"""

    print("\n" + "=" * 60)
    print("【重構驗證：開始】")
    print("=" * 60 + "\n")

    all_passed = True

    try:
        all_passed &= verify_ranking_report()
    except Exception as e:
        print(f"❌ 排名報告驗證失敗: {e}")
        all_passed = False

    try:
        all_passed &= verify_zero_sum()
    except Exception as e:
        print(f"❌ 零和檢驗失敗: {e}")
        all_passed = False

    try:
        all_passed &= verify_key_metrics()
    except Exception as e:
        print(f"❌ 關鍵指標驗證失敗: {e}")
        all_passed = False

    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 全部驗證通過！可以繼續重構。")
    else:
        print("⚠️  有驗證失敗，請檢查後再繼續。")
    print("=" * 60)

    return all_passed


if __name__ == "__main__":
    main()
