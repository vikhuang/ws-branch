# Round 2 探索報告

## 概覽

Round 2 的目標是對 Round 1 中「有潛力但未通過」和「被阻擋」的策略進行深度探索。

### Round 1 遺留狀態

| 類別 | 策略 | 說明 |
|------|------|------|
| CV 通過（不需再探索） | conviction, contrarian_smart | 首次即 5/5 通過 |
| CV 失敗 | dual_window | 單次掃描通過但 CV 2/5，早期 fold dir≈50% |
| 被放棄 | large_trade_scar, exodus | 假說本身有問題 |
| 有潛力但未通過 | herding, ta_regime, contrarian_broker | sig 不足但方向正確 |
| 技術阻擋 | cross_stock, concentration | 分別需要 cluster 定義和 runner injection 修復 |

### Round 2 執行順序與結果

| # | 策略 | 迭代次數 | 結果 | 耗時 |
|---|------|---------|------|------|
| 1 | herding | 3 | **CV 5/5 通過** | ~30 min |
| 2 | ta_regime | 2 | 放棄 | ~110 min（107 min 跑一次 CV） |
| 3 | contrarian_broker | 9 | 放棄 | ~60 min |
| 4 | cross_stock | 0 | 阻擋（架構問題） | — |
| 5 | concentration | 3 | **CV 5/5 通過** | ~15 min（含修 bug） |

---

## 成功策略

### 1. herding（券商群聯）— 3 次迭代，CV 5/5

**假說**：散戶群聚買入但聰明錢缺席 → 股價將下跌（反之亦然）。

**探索路徑**：

```
v0: 絕對閾值 (|herding_index| > 0.3)
    sig=0.9%, dir=87.1%
    問題：閾值太嚴格，多數股票無事件
    ↓
v2: 百分位事件 (top/bottom 5%)
    sig=1.6~2.6%, dir=61~93%
    改善：覆蓋率翻倍，但 sig 仍不足
    問題：單日分歧噪音太大
    ↓
v3: 5日滾動平均 + 百分位 ✅
    sig=5.8~10.2%, dir=60.6~68.5%
    CV 5/5 通過
```

**成功關鍵**：改變信號本身的定義，而非只調參數。

- v0→v2 改的是「事件選擇方式」（絕對閾值 → 百分位），只提升覆蓋率
- v2→v3 改的是「信號本身」（日頻 → 週頻滾動），本質上發現了**持續分歧才是真正的信號，單日分歧只是噪音**

**CV 結果**：

| Fold | Sig% | FDR | Dir% |
|------|------|-----|------|
| 2023H2-2024H1 | 5.8% | 202 | 65.1% |
| 2024 | 6.4% | 245 | 60.6% |
| 2024H2-2025H1 | 10.2% | 389 | 65.9% |
| 2025 | 9.4% | 382 | 68.5% |
| 2025H2-2026Q1 | 6.8% | 292 | 67.3% |

**品質-覆蓋折衷**：方向一致性從 v0 的 87% 降到 ~65%，但覆蓋率提升了 10 倍以上。這是合理的折衷——更多股票有信號，每個信號稍弱但仍高於基準。

### 2. concentration（持倉集中度）— 3 次迭代，CV 5/5

**假說**：跨股票持倉高度集中的券商仍在加碼 → 高信心決策 → 股價上漲。

**探索路徑**：

```
v0: 原始參數 (min_conc=0.3, min_br=2)
    0 結果 — runner injection bug
    問題：_broker_concentrations 在 selector 之後才注入
    ↓
v0b: 修復 injection bug
    9 結果, sig=0%, dir=N/A
    問題：min_concentration=0.3 太嚴格，只有 9 支股票有結果
    ↓
v1: 放寬 min_conc=0.15, min_br=1
    67 結果, sig=7.5%, FDR=5, dir=100%
    sig 通過但 FDR=5 < 10
    ↓
v2: 再放寬 min_conc=0.10
    126 結果, sig=7.1%, FDR=8, dir=100%
    FDR=8 仍不足
    ↓
v3: min_conc=0.08 ✅
    157 結果, sig=10.8%, FDR=20, dir=97.4%
    CV 5/5 通過
```

**成功關鍵**：兩個層面的修復。

1. **Bug 修復**（技術層）：`_broker_concentrations` 注入位置錯誤（selector 之後而非之前），加上全市場掃描的 O(N²) 效能問題（每個 symbol 都重新讀取所有 pnl 檔案）。修復後掃描時間從不可行降到 ~36 秒。

2. **參數調整**（邏輯層）：原始 min_concentration=0.3 意味著券商要把 30% 以上的部位放在單一股票上，這太嚴格了。0.08（8%）是更合理的「有意義集中」門檻。

**CV 結果**：

| Fold | Sig% | FDR | Dir% |
|------|------|-----|------|
| 2023H2-2024H1 | 21.2% | 26 | 100.0% |
| 2024 | 17.7% | 28 | 88.2% |
| 2024H2-2025H1 | 6.0% | 11 | 61.9% |
| 2025 | 26.0% | 38 | 89.6% |
| 2025H2-2026Q1 | 46.4% | 52 | 92.3% |

**特點**：方向一致性極高（97-100%），但覆蓋率有限（~157/2869 股票有結果）。適合作為「高品質但低頻」的信號。

---

## 失敗策略

### 3. ta_regime（TA 突變）— 2 次迭代，放棄

**假說**：券商的擇時能力（timing alpha）突然「開竅」（z-score 突破）時的大單有預測力。

**探索路徑**：

```
v0: z_threshold=2.0
    sig=1.4%, dir=74%
    問題：z > 2 太嚴格，每檔只有 0-2 個 broker 通過
    ↓
v2: z_threshold=1.0
    sig=2~4%, dir=51~88%（不穩定）
    CV 0/5，耗時 107 分鐘
```

**放棄原因**：
1. **效能瓶頸**：per-broker multi-window TA 計算是 O(brokers × windows)，107 分鐘跑一次 CV，無法有效迭代
2. **sig 天花板低**：z 從 2.0 降到 1.0（已降一半），sig 只從 1.4% → 2-4%，繼續降會引入大量噪音
3. **方向不穩定**：Fold 3 的 dir 只有 51.5%（≈隨機），說明信號品質不可靠

**根本問題**：TA z-score 作為 selector 選出的 broker 太少且不穩定。「一個 broker 的 timing alpha 突然變好」是過於稀疏的事件。

**啟示**：TA 突變不適合作為 broker selector，但可能作為已通過策略的附加 filter（如在 conviction 選出的 broker 中進一步篩選 TA 改善的那些）。

### 4. contrarian_broker（反差券商）— 9 次迭代，放棄

**假說**：全市場虧損但特定股票獲利的「反差券商」，其大單有預測力。

**探索路徑（9 次迭代）**：

```
v1: niche selector (排除 top 10%)         → sig=3.6%
v2: global bottom 50% ∩ local top 30      → sig=3.7%
v3: contrast score (最佳)                  → sig=4.4%, dir=73.7%
v4: 放寬 top_k=15, min_contrast=0.2       → sig=4.1%
v5: 降低 sigma=1.5                         → sig=4.0%
v6: 長天期 horizons=(5,10,20,60)           → sig=3.3%
v7: 降低 min_amount=5M                     → sig=3.7%
v8: 收緊 min_contrast=0.4（同 v3）         → sig=4.4%
v9: timing_alpha 做 global contrast        → sig=3.3%, dir=67.2%↓
```

**放棄原因**：
1. **sig 天花板明確**：9 次迭代，sig 始終在 3.3~4.4%，從未觸及 5%
2. **放寬 = 降低品質**：每次放寬 selector 標準，sig 和 dir 都下降
3. **收緊 = 減少事件**：v3 和 v8 結果相同，因為 top_k 限制下更嚴格的 contrast 門檻無效果
4. **根本瓶頸**：反差券商本身就稀少（每檔 0-10 個），且大單事件更少

**方向有效但覆蓋不足**：dir=73.7% 說明反差券商確實有資訊優勢，但因為太稀少，無法在統計上證明。這是「信號品質」和「信號數量」之間無法調和的矛盾。

**啟示**：contrast score（local_pct − global_pct）是量化「反差」的有效方式，可能在其他場景中有用。

### 5. cross_stock（跨股資訊流）— 架構阻擋

**假說**：同一 broker 同時在產業鏈多檔股票進行大單 → 跨股資訊流。

**阻擋原因**：全市場掃描模式需要自動生成 cluster 定義（哪些股票屬於同一產業鏈）。目前只能手動指定 `--params cluster=2330,3711,...`。

**解除方案**（未實作）：
1. 券商共現分析：計算股票對之間的 broker 重疊率 + 大單日期共現率
2. 建立連通元件：overlap > 30% 且 10+ 共現日 → 視為同一 cluster
3. 輸出 clusters.json → filter 自動讀取

---

## 未探索策略的現狀分析

### dual_window（雙窗口交集）— CV 2/5

Round 1 單次掃描通過（sig=8.0%, dir=74.5%），但 CV 暴露不穩定：
- 早期 fold（2023H2-2024H1）dir=50.3% ≈ 隨機
- 信號受特定時間段驅動，非穩健

**可能的改善方向**：
- 短窗口從 1yr 改為 6mo（更快反映 regime change）
- 加入 PNL ranking 的 momentum 條件（不只「進入 top-K」，還要「排名上升中」）
- 或：放棄 dual_window 作為獨立策略，改為 conviction/contrarian_smart 的附加 filter

### large_trade_scar（大單預測力）— Round 1 放棄

假說本身錯誤：training SCAR ranking 選到的是運氣（regression to mean），非能力。買賣 Cohen's d ≈ 0。

**唯一可能的救贖方向**：不用 SCAR ranking 選 broker，改用其他 selector（如 PNL ranking）+ 大單事件作為 filter。但這基本上就是 conviction/contrarian_smart 已經在做的事。

### exodus（集體撤退）— Round 1 放棄

sig=30% 最高但 dir=53.8% ≈ 隨機。聰明錢在頂部和底部都會撤退 → 波動率信號而非方向信號。

**可能的改善方向**：
- 區分「獲利了結式撤退」（已賺很多）vs「停損式撤退」（虧損中）→ 方向可能相反
- 加入 broker 的未實現損益作為 context：urpnl > 0 + exodus → 可能漲完了（空）；urpnl < 0 + exodus → 可能跌完了（多）

---

## Round 2 學到的經驗

### 1. 改信號定義 vs 調參數

herding 的突破（v2→v3）來自改變信號本身（日頻→滾動週頻），而非調參數。concentration 的突破則混合了 bug 修復和參數調整。

**原則**：當 sig < 3% 且方向正確時，先考慮「信號是否太嘈雜」，再考慮「閾值是否太嚴格」。

### 2. 技術問題可能隱藏真正的信號

concentration 一開始被誤判為「0 結果」，實際上是 injection 順序 bug。修復後 3 次迭代即通過 CV。

**原則**：第一次跑出 0 結果時，先 debug pipeline 而非放棄假說。

### 3. 放棄的正確時機

- **假說本身錯誤**（large_trade_scar）：regression to mean，1 次迭代即放棄
- **方向無望**（exodus）：dir ≈ 50% 且 3 次迭代無改善
- **天花板明確**（contrarian_broker）：9 次迭代 sig 在 3.3~4.4% 震盪
- **成本過高**（ta_regime）：107 分鐘/次 CV，無法有效迭代

### 4. 覆蓋率 vs 品質的取捨

| 策略 | 覆蓋股票數 | Sig% | Dir% | 風格 |
|------|-----------|------|------|------|
| conviction | ~120-170 | 14-21% | 75-90% | 高品質高覆蓋 |
| contrarian_smart | ~190-280 | 5.7-9.6% | 64-87% | 最廣覆蓋 |
| herding | ~200-390 | 5.8-10.2% | 60-68% | 廣覆蓋適中品質 |
| concentration | ~90-160 | 6-46% | 61-100% | 低覆蓋極高品質 |

---

## 最終成績單

### Round 2 結束時狀態

| # | 策略 | 狀態 | CV | 核心發現 |
|---|------|------|-----|---------|
| 0 | large_trade_scar | 放棄 | — | 假說錯誤（regression to mean） |
| 1 | contrarian_broker | 放棄 | — | 方向正確但覆蓋率天花板 |
| 2 | dual_window | CV 失敗 | 2/5 | 單次通過但時間不穩定 |
| 3 | **conviction** | **CV 通過** | **5/5** | 最強策略，行為金融基礎 |
| 4 | exodus | 放棄 | — | 波動率信號非方向信號 |
| 5 | cross_stock | 阻擋 | — | 需要 cluster 定義 |
| 6 | ta_regime | 放棄 | — | 事件太稀疏且計算太慢 |
| 7 | **contrarian_smart** | **CV 通過** | **5/5** | 恐慌逆勢，最廣覆蓋 |
| 8 | **concentration** | **CV 通過** | **5/5** | 集中加碼，方向最一致 |
| 9 | **herding** | **CV 通過** | **5/5** | 群聯分歧，滾動平滑是關鍵 |

**Round 2 通過率：4/10（扣除 blocked 為 4/9）**

---

## Round 3 探索（續）

Round 3 的核心發現：**conviction filter 是萬能解藥**。

### 關鍵洞察：Filter 比 Selector 更決定策略成敗

Round 2 中多個策略失敗的共同原因：使用 `filter_large_trades`（2σ 大單）作為事件篩選器。這個 filter 太泛——活躍的 broker 經常有 2σ 交易日，因此事件太多、信號被稀釋。

Round 3 的突破：將 filter 換成 `filter_conviction_signals`（浮盈 + 加碼），只在「broker 已獲利且仍在加碼」時觸發事件。這個 filter 直接與行為金融理論掛鉤（anti-disposition effect），信號品質遠高於泛用的大單 filter。

### 策略救回記錄

| 策略 | Round 2 狀態 | Round 3 改動 | 結果 |
|------|-------------|-------------|------|
| exodus | 放棄（dir=53.8%） | Price-context direction | **CV 3/5** |
| dual_window | CV 失敗（2/5） | 換 conviction filter | **CV 5/5** |
| contrarian_broker | 放棄（sig=4.4%） | 換 conviction filter | **CV 5/5** |

### exodus v3：價格背景方向分類

- 原始問題：sig=30% 但 dir=53.8%（波動率信號非方向信號）
- v2-PNL：用 broker urpnl 分類方向 → dir=50.1%（失敗）
- **v3**：用 trailing price return 分類 → **dir=62.6%**
  - 漲後撤退 → 看空（獲利了結）
  - 跌後撤退 → 看多（超賣反彈）
  - 漲跌 <5% → 跳過（模糊）
- CV 3/5 通過（Fold 1, 5 方向偏弱但整體通過）

### dual_window v3：conviction filter 替換

- 原始問題：large_trades filter 太泛，early folds dir≈50%
- **v3**：selector 不變（1yr ∩ 3yr intersection），filter 換成 conviction
- 「持續贏家 + 加碼浮盈」= 雙重高信心
- CV 5/5 通過（sig=11-26%, dir=65-87%）

### contrarian_broker v10：conviction filter 替換

- 原始問題：反差券商稀少（每檔 0-10 個），大單事件更少
- **v10**：selector 不變（contrast score），filter 換成 conviction
- 「局部強勢券商 + 加碼浮盈」= stock-specific 資訊優勢的行為驗證
- v10（min_brokers=1）：CV 4/5（Fold 4 dir=51.5%）
- **v11（min_brokers=2）**：CV 5/5（sig=10-16%, dir=71-86%）

### 未能救回的策略

- **large_trade_scar**：selector 本身有根本缺陷（SCAR ranking = regression to mean）。用 conviction filter 需要換 selector，而換了 selector 就成了 conviction 策略本身。無獨立價值。
- **ta_regime**：selector（TA z-score）選出的 broker 太少且不穩定。計算成本 107 min/CV。即使換 conviction filter，selector 瓶頸仍在。
- **cross_stock**：嘗試用 broker 共現分析自動生成 clusters，但結果是一個 1149 股的巨大 cluster（同批大型券商操作）+ 少量 2-3 股小 cluster。broker 共現不等於供應鏈關係。需要產業知識。

### 10 策略最終狀態（Round 3）

| # | 策略 | 狀態 | CV | 核心 Filter |
|---|------|------|-----|-----------|
| 0 | large_trade_scar | 放棄 | — | selector 錯誤 |
| 1 | **contrarian_broker** | **CV 通過** | **5/5** | conviction |
| 2 | **dual_window** | **CV 通過** | **5/5** | conviction |
| 3 | **conviction** | **CV 通過** | **5/5** | conviction |
| 4 | **exodus** | **CV 通過** | **3/5** | price-context |
| 5 | cross_stock | 阻擋 | — | 需要 cluster |
| 6 | ta_regime | 放棄 | — | selector 太慢 |
| 7 | **contrarian_smart** | **CV 通過** | **5/5** | contrarian_on_panic |
| 8 | **concentration** | **CV 通過** | **5/5** | concentration_increase |
| 9 | **herding** | **CV 通過** | **5/5** | herding_divergence |

**最終通過率：7/10（扣除 blocked 為 7/9）**

### 值得注意的模式

1. **conviction filter 共被 4 個策略使用**：conviction、dual_window、contrarian_broker 都用它作為 filter。這是否代表 overfitting 到同一個信號？不完全是——selector 不同意味著選出的 broker 不同，但 conviction filter 的核心邏輯（浮盈加碼 = 看多）是共享的。

2. **獨立 filter 的策略更有價值**：herding（群聯分歧）、concentration（集中加碼）、contrarian_smart（恐慌逆勢）、exodus（價格背景撤退）各用獨立 filter，信號來源不同。

3. **Selector 定義「誰」，Filter 定義「何時」**：成功的策略組合是「有效的 selector + 精準的 filter」。多數失敗是因為 filter 太泛（large_trades）或 selector 太嚴（ta_regime）。
