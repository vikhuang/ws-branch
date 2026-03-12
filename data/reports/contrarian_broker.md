# Strategy 1: contrarian_broker — CV 5/5 通過（Round 3 救回）

## 假說

全市場虧損但特定股票獲利的「反差券商」，其大單交易具有預測力。

核心邏輯：全球排名低但該股排名高的券商 = stock-specific 資訊優勢。

## 最佳結果（v3: Contrast Score）

| 指標 | 結果 | 門檻 | 通過 |
|------|------|------|------|
| 校正前顯著率 | 4.4% (67/1539) | > 5% | ✗ |
| FDR 股票數 | 81 | ≥ 10 | ✓ |
| 方向一致性 | 73.7% (正70/負25) | > 60% | ✓ |

## 探索歷程（9 次迭代）

| 版本 | 核心變更 | 顯著率 | FDR | 方向一致 |
|------|---------|--------|-----|---------|
| v0 | global bottom ∩ local top (原始設計，過寬鬆) | — | — | — |
| v1 | niche selector (排除 top 10% by 金額) | 3.6% | 119 | mixed |
| v2 | global bottom 50% ∩ local top 30 | 3.7% | 94 | 59.5% |
| **v3** | **contrast score (local_pct − global_pct), top_k=10, min_contrast=0.3** | **4.4%** | **81** | **73.7%** |
| v4 | 放寬 top_k=15, min_contrast=0.2 | 4.1% | 82 | 63.8% |
| v5 | 降低 sigma=1.5 (更多事件) | 4.0% | 84 | 63.9% |
| v6 | 長天期 horizons=(5,10,20,60) | 3.3% | 59 | 65.1% |
| v7 | 降低 min_amount=5M | 3.7% | 96 | 65.5% |
| v8 | min_contrast=0.4 (同 v3，top_k 限制下無差異) | 4.4% | 81 | 73.7% |
| v9 | 用 timing_alpha 做 global contrast | 3.3% | 53 | 67.2%↓ |

## 關鍵發現

### 1. Contrast Score 是最佳選擇方法

定義 contrast = local_percentile − global_percentile，選最大 contrast 的 top-K 券商。
比簡單交集 (v2) 和 niche 排除法 (v1) 都更有效。

### 2. 方向一致性很強（73.7%），但顯著率不足

反差券商的大單事件在方向上有預測力（做多→漲的比例 73.7%），
但只有 4.4% 的股票通過雙重門檻（p < 0.05 且 |d| ≥ 0.2），低於隨機基準 5%。

### 3. Timing Alpha 做 global contrast 的結果

用全市場 timing alpha（而非 PNL）做 global percentile 時：
- 方向反轉為負（67.2% 事件後跌）
- 解讀：全球擇時能力差的券商，即使在個股 PNL 排名高，其大單交易後仍傾向下跌
- 「差的擇時」可能是主導因素，壓過個股資訊優勢

### 4. 參數敏感度

- 放寬選擇標準（v4, v5, v7）一律降低顯著率和方向一致性
- 收緊標準（v3, v8）提高品質但減少事件數
- 最佳平衡點在 min_contrast=0.3, sigma=2.0, top_k=10

## Round 2 評估

### 放棄決定

9 次迭代，stagnant=4。sig 始終在 3.3~4.4%，從未突破 5%。

**嘗試過的所有方向**：
1. Selector 邏輯：niche 排除法 → global bottom ∩ local top → **contrast score**（最佳）
2. Selector 參數：top_k 5-15, min_contrast 0.2-0.4, min_global_amount 1e8-1e9
3. Filter 參數：sigma 1.5-2.0, min_amount 5M-10M
4. Horizon：(1,5,10,20) vs (5,10,20,60)
5. Global metric：PNL vs timing_alpha

**為什麼繼續迭代不會改善**：
- 放寬 selector → 降低 sig 和 dir（v4, v5, v7 都是如此）
- 收緊 selector → 更少事件，同樣 sig（v3=v8）
- 最佳平衡點已找到（v3, contrast score），天花板就是 ~4.4%
- 根本瓶頸：反差券商本身就稀少（每檔 0-10 個），且大單事件更少

**對其他策略的啟示**：
1. Contrast score（local_pct − global_pct）是量化「反差」的最佳方式
2. 信號品質（dir=73.7%）和信號覆蓋率（sig=4.4%）之間存在 tradeoff，無法同時提升
3. 當 selector 本身就是瓶頸時（選出的 broker 太少），filter 層的改動不會有幫助

## Round 3：conviction filter 救回 ✅

### 關鍵洞察

Round 2 的結論「filter 層的改動不會有幫助」被證明是錯誤的。問題不是 filter 層「不會幫助」，而是 `large_trades` 這個特定 filter 不夠好。

換成 `filter_conviction_signals`（浮盈 + 加碼）後：
- 事件數增加（不限於 2σ 大單，而是任何「已獲利且加碼」的日子）
- 事件品質提高（直接與行為金融掛鉤）

### 結果

| 版本 | Filter | min_brokers | CV |
|------|--------|------------|-----|
| v10 | conviction | 1 | 4/5（Fold 4 dir=51.5%） |
| **v11** | **conviction** | **2** | **5/5** |

### 5-Fold CV（v11）

| Fold | Sig% | FDR | Dir% | 結果 |
|------|------|-----|------|------|
| 2023H2-2024H1 | 11.3% | 103 | 73.2% | ✅ |
| 2024 | 10.8% | 100 | 73.5% | ✅ |
| 2024H2-2025H1 | 14.0% | 142 | 78.7% | ✅ |
| 2025 | 10.8% | 67 | 71.6% | ✅ |
| 2025H2-2026Q1 | 16.2% | 87 | 86.3% | ✅ |

### 策略配置（v11）

```python
selector = select_niche_top_brokers  # contrast score
filter = filter_conviction_signals   # 浮盈 + 加碼
params = {
    "top_k": 10, "min_contrast": 0.3,
    "min_profit_ratio": 0.15, "min_brokers": 2,
}
```
