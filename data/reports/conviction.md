# Strategy 3: conviction — 假說成立 ✓（最強信號）

## 假說

PNL 排名前列的券商在浮盈 > 20% 時仍加碼 = 對抗 disposition effect 的強信心信號。

## 結果（v0，首次即通過）

| 指標 | 結果 | 門檻 | 通過 |
|------|------|------|------|
| 校正前顯著率 | **18.0%** (217/1204) | > 5% | ✓ |
| FDR 股票數 | **324** | ≥ 10 | ✓ |
| 方向一致性 | **73.9%** (正459/負162) | > 60% | ✓ |

### 詳細統計

- 有結果：1204 股
- 校正前顯著：217（18.0%）— 隨機基準的 3.6 倍
- BH-FDR 檢定數：4816（1204 × 4 horizons）
- 通過 FDR：621（12.9%）
- FDR 顯著股票數：324/1204（26.9%）

## 策略配置

```python
selector = select_top_k_by_pnl     # top-20 by per-stock PNL
filter = filter_conviction_signals  # 浮盈>20% 且加碼
outcome = outcome_forward_returns
baseline = baseline_unconditional
horizons = (1, 5, 10, 20)
params = {"top_k": 20, "min_brokers": 3, "min_profit_ratio": 0.2}
```

## 為什麼是目前最強的信號

1. **Disposition effect 的逆向操作**：行為金融學指出大多數人傾向賣出獲利部位（落袋為安）。在浮盈 > 20% 時仍選擇加碼，代表強烈信心——這是高成本的行為信號。
2. **18.0% 顯著率**：是隨機基準（5%）的 3.6 倍，是 dual_window（8.0%）的 2.25 倍。
3. **FDR 通過率 12.9%**：遠超其他策略。

## 所有策略對比

| 策略 | 顯著率 | FDR 股票 | 方向一致 | 狀態 |
|------|--------|----------|---------|------|
| **conviction** | **18.0%** | **324** | **73.9%** | ✓ |
| dual_window | 8.0% | 379 | 74.5% | ✓ |
| contrarian_broker | 4.4% | 81 | 73.7% | ✗ |
| large_trade_scar | 2.9% | 0 | N/A | ✗ |
