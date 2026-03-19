# Signal Strength Analysis 方法論檢查清單

> 每次對新的 signal_value 候選指標做 quintile analysis 前後，必須逐項確認。
> 歷史教訓：v1 churn ratio 因未扣 beta + 未 winsorize outlier → 產生虛假「反向」結論。

---

## Pre-analysis 檢查（寫 code 前）

### 1. 資料來源
- [ ] 這個指標用了哪些 DataFrame？列出所有資料來源
- [ ] 每個 DataFrame 的時間範圍是什麼？是否包含未來資料？
- [ ] 指標的計算是否只用到「事件日當天及之前」的資訊？（no look-ahead）

### 2. 與既有指標的關係
- [ ] 這個指標跟 signal_count (n_conviction) 是否機械性相關？
  - 如果是：必須做 partial correlation 控制
  - 範例：churn = gross/net，count 高 → net 高 → churn 低（機械性負相關）
- [ ] 值域是什麼？是否需要轉換？
  - 比例型（churn ratio）→ 用 log 轉換
  - 整數型（count, persistence）→ 不需轉換
  - 金額型（buy_amount）→ 需 per-stock normalize

### 3. Outlier 風險
- [ ] 值域是否跨越多個數量級？（如 churn 1~604,000）
  - 如果是：winsorize 或 log 轉換是必要的
- [ ] 是否有 division by near-zero 產生的極端值？

---

## Code 層檢查（寫 code 時）

### 4. Return 處理
- [ ] forward return 是 **excess return**（扣 IX0001 同期 market return）？
  - 檢查：`ret_df[col] = ret_df[col] - mkt_df[col]`
  - 位置：runner 的 per-symbol loop 內
- [ ] forward return 經過 **per-stock z-score**（除以 stock vol × √horizon）？
  - 檢查：`ret_df[col] = ret_df[col] / (daily_std * sqrt(h) * 10000)`
  - 位置：在 join metadata 之前
- [ ] direction-adjust 在 `analyze_strength` 內部做（不是外部重複做）？

### 5. 時間範圍
- [ ] 指標計算是在 **warmup cutoff (>= 2023-01-01) 之後**？
  - 檢查：`events = events.filter(pl.col("date") >= self._WARMUP_CUTOFF)` 在指標計算之前
- [ ] 如果指標涉及 rolling window，window 是否會回溯到 warmup 期間？
  - 如果是：需要確認 window 內的資料是否可靠

### 6. 分析函數參數
- [ ] `analyze_strength` 的 `group_col` 設為正確的欄位名？
- [ ] `confound_col` 設為 `"signal_count"`（或其他需控制的變因）？
- [ ] `winsorize_pct` 設為 0.01（1%/99%）？
- [ ] 如果指標是「低=強」（如 churn），不要用 invert（已移除），直接觀察 ρ 的正負

---

## Post-analysis 檢查（看結果時）

### 7. Spearman ρ 解讀
- [ ] ρ 的方向符合假設嗎？（正 = 高指標 → 高 return）
  - 如果反向：是否有合理解釋？（如 rarity = information value）
  - 如果反向且無解釋：可能是 artifact，需進一步調查
- [ ] ρ 的絕對值 > 0.1 才值得考慮經濟意義
  - |ρ| < 0.05：幾乎沒有預測力
  - |ρ| 0.05-0.10：統計顯著但經濟意義微弱
  - |ρ| > 0.10：可能值得用於 sizing

### 8. Partial correlation
- [ ] 控制 count 後 partial ρ 是否仍然顯著？
  - 如果 partial ≈ 0：指標只是 count 的代理，無獨立資訊
  - 如果 partial 仍顯著：指標有獨立預測力

### 9. Monotonicity
- [ ] 各 horizon 的 monotonicity 結果是否一致？
  - 全部 ✅：強證據
  - 混合：不穩定
  - 全部 ❌：指標方向可能反了（或無效）

### 10. Group 分佈
- [ ] 各 group 的 n_events 是否合理均勻（每組 >1000）？
- [ ] 是否因為 quantile 邊界重疊只分出 2 組？
  - 如果是：考慮用固定切點取代 quantile

### 11. 交叉驗證
- [ ] 結果是否跟之前的發現一致？
  - churn（空間共識）和 persistence（時間持續）都顯示「越常見 = 越弱」
  - 新指標如果也顯示同方向 → 互相驗證
  - 新指標如果顯示反方向 → 需要解釋為什麼

---

## 已驗證的候選指標結果

| 指標 | ρ (10d) | partial ρ | 方向 | 結論 |
|------|---------|-----------|------|------|
| signal_count | +0.062 | — | 正確但弱 | ❌ 不足以 sizing |
| log(churn) | +0.011 | +0.008 | ≈ 零 | ❌ 無獨立資訊 |
| persistence | +0.027 | -0.003 | 反向（頻繁=差）| ❌ 無獨立資訊 |
| log(amount) | +0.047 | +0.027 | 倒 U 型（中等最好）| ❌ 非單調 |

**核心發現**：conviction alpha 是 binary。rarity = information value。
金額呈倒 U 型：太小=噪音、中等=genuine、太大=crowding。
