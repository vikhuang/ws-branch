# Hypothesis Exploration Final Report

## 總覽

10 個券商分點假說策略經全市場掃描（~2800+ 股），評估三項標準：
1. 校正前顯著率 > 5%（超過隨機基準）
2. BH-FDR 校正後通過股票 ≥ 10
3. FDR 通過股票的方向一致性 > 60%

## 結果摘要

| # | 策略 | 顯著率 | FDR | 方向一致 | 結果 |
|---|------|--------|-----|---------|------|
| 3 | **conviction** | **18.0%** | 324 | 73.9% | ✅ 通過（最強） |
| 2 | **dual_window** | **8.0%** | 379 | 74.5% | ✅ 通過 |
| 7 | **contrarian_smart** | **6.3%** | 262 | 73.1% | ✅ 通過 |
| 4 | exodus | 30.3% | 1018 | 53.8% | ❌ 無方向性 |
| 1 | contrarian_broker | 4.4% | 81 | 73.7% | ❌ 顯著率不足 |
| 6 | ta_regime | 1.4% | 55 | 74.0% | ❌ 事件太稀有 |
| 9 | herding | 0.9% | 50 | 87.1% | ❌ 事件太稀有 |
| 0 | large_trade_scar | 2.9% | 0 | N/A | ❌ 均值回歸 |
| 5 | cross_stock | — | — | — | ⏭ 需 cluster 定義 |
| 8 | concentration | — | — | — | ⏭ 需 HHI 注入 |

**通過率：3/8（排除 skipped）= 37.5%**

## 三個通過策略詳解

### 1. conviction（加碼信號）— 18.0% 顯著率

**假說**：PNL top-K 券商在浮盈 > 20% 時仍加碼 = 對抗 disposition effect 的強信心信號。

**為什麼最強**：
- 顯著率是隨機基準的 **3.6 倍**
- 行為金融學基礎紮實：大多數人賣出獲利部位（落袋為安），加碼是反常且高成本的行為
- 1204 股中 217 股顯著，FDR 通過 324 股
- 正向 73.9%（加碼後價格上漲）

### 2. dual_window（雙窗口交集）— 8.0% 顯著率

**假說**：同時在 1yr 和 3yr 滾動 PNL 排名都名列前茅 = 持續能力。

**為什麼有效**：
- 雙重篩選去除運氣成分
- 2859 股全覆蓋（最廣）
- 正向 74.5%

### 3. contrarian_smart（恐慌承接）— 6.3% 顯著率

**假說**：PNL top-K 券商在恐慌日（單日跌 >2% 或 3 日跌 >5%）逆勢買入。

**為什麼有效**：
- 恐慌日承接 = 高成本決策（心理壓力 + 被套風險）
- 至少 3 個 top-K 券商同時買入 → 信息共識
- 正向 73.1%（恐慌買入後大多數情況反彈）

## 未通過策略的教訓

### exodus（波動率信號，非方向性）

最令人驚訝的發現：30.3% 顯著率（最高！）但方向一致性僅 53.8%。
聰明錢集體出場能預測「接下來會有大幅波動」但無法預測漲跌。
**潛在應用**：選擇權策略（straddle/strangle）觸發信號。

### herding（最準但太稀有）

87.1% 方向一致性是所有策略中最高。但只有 0.9% 顯著率。
當散戶群聚且聰明錢缺席「真的」發生時，後續方向極為明確，但這種情境太少見。

### contrarian_broker（方向有效但不夠廣泛）

9 次迭代探索了 niche selector、intersection、contrast score 等方法。
最佳版本（contrast score）達到 73.7% 方向一致性和 81 個 FDR 股票，
但顯著率（4.4%）始終無法突破 5%。效果存在但太稀疏。

## 核心洞察

1. **行為信號 > 統計信號**：conviction（disposition effect）和 contrarian_smart（panic buying）的行為金融學基礎使其比純統計選擇（如 large_trade_scar、ta_regime）更有效。

2. **方向一致性 vs 顯著率是獨立維度**：exodus 有最高顯著率但無方向性；herding 有最強方向性但太稀有。只有同時具備兩者的策略才有實用價值。

3. **雙重篩選提升信號品質**：dual_window（1yr ∩ 3yr）和 conviction（PNL top-K + 浮盈加碼）都通過多重條件過濾，去除噪音。

4. **Contrast Score 是 contrarian 策略的最佳量化方式**：相比簡單交集或排除法，local_pct − global_pct 更能捕捉資訊優勢。

## 迭代統計

| 策略 | 迭代次數 | 代碼修改 |
|------|---------|---------|
| large_trade_scar | 1 | — |
| contrarian_broker | 9 | selector 重寫 3 次 |
| dual_window | 1 | — |
| conviction | 1 | — |
| exodus | 3 | params 調整 |
| ta_regime | 1 | — |
| contrarian_smart | 1 | — |
| herding | 1 | — |
| **合計** | **18** | |
