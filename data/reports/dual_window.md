# Strategy 2: dual_window — CV 5/5 通過（Round 3 救回）

## 假說

同時在 1 年和 3 年滾動 PNL 排名都名列前茅的券商（交集），若加碼浮盈部位 → 強信號。

v3 核心邏輯：持續性贏家（短+長期都贏） + 加碼浮盈（disposition effect 反向） = 雙重高信心。

## 探索歷程

### Round 1: v0（large_trades filter）

單次掃描通過（sig=8.0%, dir=74.5%），但 CV 暴露不穩定：

| Fold | Sig% | FDR | Dir% | 結果 |
|------|------|-----|------|------|
| 2023H2-2024H1 | 3.8% | 104 | 50.3% | ❌ |
| 2024 | 5.3% | 147 | 59.9% | ❌ |
| 2024H2-2025H1 | 6.4% | 277 | 80.8% | ✅ |
| 2025 | 5.6% | 242 | 87.6% | ✅ |
| 2025H2-2026Q1 | 4.0% | 123 | 86.6% | ❌ |

CV 2/5。早期 fold dir ≈ 50%（隨機）。

### Round 3: v2（短窗口 + 縮小 top_k）

short_years=0.5, top_k=10 → CV 1/5。更差——sig 降到 3.8-6.1%。

### Round 3: v3（conviction filter）✅

**關鍵改變**：把 filter 從 `filter_large_trades`（任何 2σ 大單）換成 `filter_conviction_signals`（浮盈 + 加碼）。

- v3a: min_profit_ratio=0.2, min_brokers=3 → CV 4/5（Fold 2 dir=59.7%）
- **v3b: min_profit_ratio=0.15, min_brokers=2** → **CV 5/5 ✅**

## 5-Fold CV 結果（v3b）

| Fold | Sig% | FDR | Dir% | 結果 |
|------|------|-----|------|------|
| 2023H2-2024H1 | 15.4% | 142 | 87.2% | ✅ |
| 2024 | 11.2% | 105 | 64.9% | ✅ |
| 2024H2-2025H1 | 15.7% | 135 | 75.9% | ✅ |
| 2025 | 19.6% | 200 | 77.2% | ✅ |
| 2025H2-2026Q1 | 26.4% | 252 | 84.8% | ✅ |

## 關鍵發現

### 1. Filter 比 Selector 更重要

用同一個 selector（dual_window intersection），只換 filter：
- `filter_large_trades`（2σ 大單）→ CV 2/5
- `filter_conviction_signals`（浮盈加碼）→ CV 5/5

大單事件太頻繁（active broker 經常有 2σ 交易），稀釋了信號。conviction 事件更精準：只有「已經賺錢且繼續加碼」的時刻才觸發。

### 2. 與純 conviction 策略的區別

| | conviction（top_k_by_pnl + conviction filter） | dual_window v3（intersection + conviction filter） |
|---|---|---|
| Selector | 單一 PNL ranking | 1yr ∩ 3yr 交集 |
| 最弱 fold sig | 14.5% | 11.2% |
| 最弱 fold dir | 75.5% | 64.9% |
| 覆蓋 | ~120-170 股 | ~650-730 股 |

dual_window v3 覆蓋率更廣（因為 intersection 篩出更多 broker），但品質略低。兩者互補。

### 3. 策略配置

```python
selector = select_dual_window_intersection  # 1yr ∩ 3yr top-20
filter = filter_conviction_signals           # 浮盈 + 加碼
params = {
    "top_k": 20, "short_years": 1, "long_years": 3,
    "min_profit_ratio": 0.15, "min_brokers": 2,
}
```
