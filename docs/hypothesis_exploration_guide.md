# 假說探索指南：方法論、結果與反思

## 目錄

1. [系統概覽](#1-系統概覽)
2. [十項策略詳解](#2-十項策略詳解)
3. [回測結果](#3-回測結果)
4. [迭代機制：Stop Hook 與 CLAUDE.md](#4-迭代機制stop-hook-與-claudemd)
5. [Overfitting 問題](#5-overfitting-問題)
6. [三個開放問題](#6-三個開放問題)
7. [提示詞設計建議](#7-提示詞設計建議)

---

## 1. 系統概覽

### 假說管線（5-step pipeline）

每個策略都是五個可組合步驟的具體實現：

```
Selector → Filter → Outcome → Baseline → StatTest
  選券商     篩事件   計算報酬    基準報酬    統計檢定
```

- **Selector**：選出哪些券商是「聰明錢」（依 PNL 排名、contrast score 等）
- **Filter**：在這些券商的行為中篩出「事件日」（大單、加碼、恐慌買入等）
- **Outcome**：事件後的 forward returns（1/5/10/20 天）
- **Baseline**：非事件日的 forward returns（同一支股票）
- **StatTest**：permutation test + Cohen's d，判斷事件報酬是否顯著異於基準

### 評估標準

三項全過才算通過：

| 標準 | 門檻 | 意義 |
|------|------|------|
| 校正前顯著率 | > 5% | 超過隨機基準（p<0.05 下隨機期望 5%） |
| BH-FDR 股票數 | ≥ 10 | 多重比較校正後仍有足夠股票通過 |
| 方向一致性 | > 60% | FDR 通過的股票中，正/負方向要一致 |

### 為什麼顯著率門檻是 5%？

如果策略沒有任何預測力，對 ~2800 支股票做 p<0.05 檢定，隨機預期 5% 的股票會「顯著」。所以 5% 是**隨機基準**（null hypothesis 的期望值），不是一個任意的門檻。高於 5% 表示策略能捕捉到超出隨機的信號。

---

## 2. 十項策略詳解

### Strategy 0: large_trade_scar（大單預測力）

**假說**：訓練窗口中大單 SCAR 排名前 K 的券商，在測試窗口中的大單仍有預測力。

**選擇邏輯**：`select_by_large_trade_scar` — 依訓練窗口 SCAR（Size-adjusted Cumulative Abnormal Return）排名選 top-K。

**篩選邏輯**：`filter_large_trades_test_window` — 測試窗口中 2σ 大單 + 金額 > 1000 萬。

**核心論述**：如果某些券商的大單歷史上有超額報酬，未來的大單也應有效。

**結果**：❌ 假說不成立。顯著率 2.9% < 5%，Cohen's d ≈ 0。SCAR 排名選到的是運氣而非能力（regression to the mean）。

**迭代次數**：1 次。第一次掃描即可明確否定。

---

### Strategy 1: contrarian_broker（反差券商）

**假說**：全市場虧損但特定股票盈利的券商 = stock-specific 資訊優勢。

**選擇邏輯**：`select_niche_top_brokers` — 計算 contrast score = local_percentile − global_percentile，選最大反差的券商。

**篩選邏輯**：`filter_large_trades_test_window` — 測試窗口 2σ 大單。

**核心論述**：一個券商全市場排名差但在特定股票排名好，代表它在這支股票上有別人沒有的資訊。

**結果**：❌ 方向正確但不夠廣泛。最佳版本（v3 contrast score）sig=4.4%, dir=73.7%，但顯著率始終無法突破 5%。

**迭代次數**：9 次。探索了 niche selector、intersection、contrast score、sigma/amount/horizon/TA 等變體。

**教訓**：Contrast score 是最佳量化方式，但效果太稀疏。73.7% 的方向一致性說明信號真實存在，只是覆蓋率不夠。

---

### Strategy 2: dual_window（雙窗口交集）

**假說**：同時在 1 年和 3 年 PNL 排名都名列前茅 = 持續能力，非運氣。

**選擇邏輯**：`select_dual_window_intersection` — 1yr top-K ∩ 3yr top-K。

**篩選邏輯**：`filter_large_trades` — 2σ 大單。

**核心論述**：雙重篩選去除運氣成分，只留真正有能力的券商。

**結果**：⚠️ 單次掃描通過（sig=8.0%, FDR=379, dir=74.5%），但 **CV 未通過**（2/5 folds pass）。

**迭代次數**：1 次（第一次即通過單次掃描）。

**CV 詳細結果**：

| Fold | Sig% | FDR | Dir% | 結果 |
|------|------|-----|------|------|
| 2023H2-2024H1 | 3.8% | 104 | 50.3% | ❌ |
| 2024 | 5.3% | 147 | 59.9% | ❌ |
| 2024H2-2025H1 | 6.4% | 277 | 80.8% | ✅ |
| 2025 | 5.6% | 242 | 87.6% | ✅ |
| 2025H2-2026Q1 | 4.0% | 123 | 86.6% | ❌ |

**教訓**：早期 fold 方向一致性僅 50%（= 隨機），近期才出現方向性。整段資料合併時被中後期的強信號拉高平均，但拆開看不穩定。這正是 CV 的價值 — 攔截了一個受特定時段驅動的策略。

---

### Strategy 3: conviction（加碼信號）✅

**假說**：PNL top-K 券商在浮盈 > 20% 時仍加碼 = 對抗 disposition effect 的強信心信號。

**選擇邏輯**：`select_top_k_by_pnl` — 個股 PNL 排名 top-20。

**篩選邏輯**：`filter_conviction_signals` — 浮盈 > 20% 且仍在買入，≥3 家券商同一天。

**核心論述**：行為金融學中，大多數人會在獲利時賣出（disposition effect）。逆向加碼是高成本、反直覺的行為，代表強烈信心。

**結果**：✅ **最強策略**，5/5 folds 全部通過。

**CV 詳細結果**：

| Fold | Sig% | FDR | Dir% | 結果 |
|------|------|-----|------|------|
| 2023H2-2024H1 | 15.1% | 152 | 90.4% | ✅ |
| 2024 | 14.5% | 142 | 77.7% | ✅ |
| 2024H2-2025H1 | 16.2% | 121 | 75.8% | ✅ |
| 2025 | 16.1% | 128 | 75.5% | ✅ |
| 2025H2-2026Q1 | 21.1% | 168 | 82.3% | ✅ |

**迭代次數**：1 次。第一次即通過。

**為什麼穩健**：
- 顯著率穩定在 14~21%（3~4 倍隨機基準），且趨勢上升
- 方向一致性穩定在 75~90%
- 行為金融學基礎紮實：disposition effect 是最被廣泛驗證的行為偏差之一

---

### Strategy 4: exodus（集體撤退）

**假說**：聰明錢集體出場 = 負面信號，預測股價下跌。

**選擇邏輯**：`select_top_k_by_pnl` — 個股 PNL 排名 top-20。

**篩選邏輯**：`filter_collective_exodus` — 20 天窗口內持倉歸零或減半，≥5 家券商。

**核心論述**：當多個賺錢的券商同時離場，代表他們看到了散戶看不到的風險。

**結果**：❌ 波動率信號，非方向性。sig=30.3%（最高！）但 dir=53.8%（≈隨機）。

**迭代次數**：3 次。嘗試調整 reduction_pct 和 min_brokers，方向一致性始終約 52-55%。

**教訓**：最令人驚訝的發現。聰明錢集體離場能預測「接下來會有大幅波動」，但無法預測漲跌方向。因為他們在頂部和底部都會撤退。潛在應用：選擇權策略（straddle/strangle）觸發信號。

---

### Strategy 5: cross_stock（跨股資訊流）⏭

**假說**：同一券商在產業鏈 cluster 內多支股票同時做大單 = 產業鏈資訊。

**選擇邏輯**：`select_top_k_by_pnl` — 個股 PNL 排名 top-20。

**篩選邏輯**：`filter_cluster_accumulation` — cluster 內 ≥2 支股票同日大單。

**結果**：⏭ 跳過。需要預定義的產業鏈 cluster（`--params cluster=2330,3711,...`），無法做全市場自動掃描。

**未完成原因**：缺少 cluster 定義。需先完成「券商共現分析 → 產業知識驗證 → clusters.json」三步流程。

---

### Strategy 6: ta_regime（TA 突變）

**假說**：券商的擇時能力突然「開竅」（timing alpha 的 temporal z-score 突破）= regime change 信號。

**選擇邏輯**：`select_ta_regime_change` — 120 天窗口 z-score > 2.0 的券商。

**篩選邏輯**：`filter_large_trades` — 2σ 大單。

**核心論述**：如果一個券商的擇時能力突然從平庸變成出色，代表它獲得了新的資訊源。

**結果**：❌ 事件太稀有。sig=1.4%, dir=74.0%。方向準確但頻率太低。

**迭代次數**：1 次。

**教訓**：TA regime change 本身就是稀有事件。74% 的方向一致性說明邏輯正確，但 z>2 的門檻太嚴格，降低門檻又會引入噪音。

---

### Strategy 7: contrarian_smart（逆勢操作）✅

**假說**：PNL top-K 券商在恐慌日逆勢買入 = 高成本決策，代表他們有信心。

**選擇邏輯**：`select_top_k_by_pnl` — 個股 PNL 排名 top-20。

**篩選邏輯**：`filter_contrarian_on_panic` — 單日跌 >2% 或 3 日跌 >5% 時，≥3 家 top-K 券商淨買入。

**核心論述**：恐慌日承接需要對抗心理壓力和被套風險，是高成本的逆向決策。多家券商同時行動 = 資訊共識。

**結果**：✅ 5/5 folds 全部通過。

**CV 詳細結果**：

| Fold | Sig% | FDR | Dir% | 結果 |
|------|------|-----|------|------|
| 2023H2-2024H1 | 7.2% | 224 | 87.7% | ✅ |
| 2024 | 5.7% | 187 | 78.3% | ✅ |
| 2024H2-2025H1 | 8.4% | 270 | 64.9% | ✅ |
| 2025 | 8.9% | 278 | 82.2% | ✅ |
| 2025H2-2026Q1 | 9.6% | 242 | 86.7% | ✅ |

**迭代次數**：1 次。第一次即通過。

**為什麼穩健**：
- 顯著率穩定在 5.7~9.6%，趨勢上升
- 方向一致性高達 64.9~87.7%（恐慌買入後大多反彈）
- 2000+ 支股票有事件（覆蓋率最廣）

---

### Strategy 8: concentration（持倉集中度）⏭

**假說**：跨股票持倉高度集中的券商加倉 = 高信心信號。

**選擇邏輯**：`select_concentrated_brokers` — HHI > 30% 的券商。

**篩選邏輯**：`filter_concentration_increase` — 集中券商的加倉日。

**結果**：⏭ 跳過。Scanner 模式下 `_broker_concentrations` 未注入，產生 0 結果。

**未完成原因**：需要跨股票 HHI 計算（目前用 snapshot，需時序版）或修改 runner 注入邏輯。

---

### Strategy 9: herding（券商群聯）

**假說**：散戶群聚一致但聰明錢缺席 = 危險信號（做空方向）。

**選擇邏輯**：`select_top_k_by_pnl` — 個股 PNL 排名 top-20。

**篩選邏輯**：`filter_herding_divergence` — herding index > 0.3（散戶一致性 vs 聰明錢分歧度）。

**核心論述**：當散戶看法一致但聰明錢不參與，代表散戶可能是錯的。

**結果**：❌ 最準但太稀有。sig=0.9%, dir=87.1%（所有策略中最高方向一致性）。

**迭代次數**：1 次。

**教訓**：87.1% 的方向一致性極度驚人，說明邏輯完全正確。但散戶群聚且聰明錢完全缺席的情境極少發生。這是一個「稀有但致命」的信號。

---

## 3. 回測結果

### 第一輪：單次全市場掃描（無 CV）

| # | 策略 | 顯著率 | FDR | 方向一致 | 迭代 | 結果 |
|---|------|--------|-----|---------|------|------|
| 3 | conviction | 18.0% | 324 | 73.9% | 1 | ✅ |
| 2 | dual_window | 8.0% | 379 | 74.5% | 1 | ✅ |
| 7 | contrarian_smart | 6.3% | 262 | 73.1% | 1 | ✅ |
| 4 | exodus | 30.3% | 1018 | 53.8% | 3 | ❌ |
| 1 | contrarian_broker | 4.4% | 81 | 73.7% | 9 | ❌ |
| 6 | ta_regime | 1.4% | 55 | 74.0% | 1 | ❌ |
| 9 | herding | 0.9% | 50 | 87.1% | 1 | ❌ |
| 0 | large_trade_scar | 2.9% | 0 | N/A | 1 | ❌ |
| 5 | cross_stock | — | — | — | 0 | ⏭ |
| 8 | concentration | — | — | — | 1 | ⏭ |

**總迭代次數：18 次**

### 第二輪：5-Fold 滾動窗口 CV

對第一輪通過的三個策略做嚴格驗證：

| 策略 | F1 | F2 | F3 | F4 | F5 | 通過 | 結果 |
|------|----|----|----|----|-----|------|------|
| conviction | ✅ 15.1% | ✅ 14.5% | ✅ 16.2% | ✅ 16.1% | ✅ 21.1% | 5/5 | ✅ 穩健 |
| contrarian_smart | ✅ 7.2% | ✅ 5.7% | ✅ 8.4% | ✅ 8.9% | ✅ 9.6% | 5/5 | ✅ 穩健 |
| dual_window | ❌ 3.8% | ❌ 5.3% | ✅ 6.4% | ✅ 5.6% | ❌ 4.0% | 2/5 | ❌ 不穩健 |

**最終通過：2/10 策略（conviction, contrarian_smart）**

### 5-Fold CV 窗口定義

```
Fold 1: train ≤ 2023-06-30 │ test 2023-07-01 → 2024-06-30  (2023H2-2024H1)
Fold 2: train ≤ 2023-12-31 │ test 2024-01-01 → 2024-12-31  (2024)
Fold 3: train ≤ 2024-06-30 │ test 2024-07-01 → 2025-06-30  (2024H2-2025H1)
Fold 4: train ≤ 2024-12-31 │ test 2025-01-01 → 2025-12-31  (2025)
Fold 5: train ≤ 2025-06-30 │ test 2025-07-01 → 2026-03-31  (2025H2-2026Q1)
```

CV 通過標準：≥3/5 folds 三項指標全過。

---

## 4. 迭代機制：Stop Hook 與 CLAUDE.md

### 機制說明

Claude Code 的 Stop hook 讓 AI 在每次回應結束時自動接收下一個任務。整套機制由三個檔案組成：

### 4.1 v1 的問題（Round 1）

Round 1 使用的 v1 機制有四個核心問題：

1. **「改 selector/filter 邏輯，不只是調參數」太模糊**。Claude 缺乏金融直覺，傾向做機械式修改（加新欄位、改閾值），而不是重新思考假說本身。

2. **沒有要求 CV 驗證**。原始指令只做單次全市場掃描。dual_window 在單次掃描下通過，CV 才暴露其不穩健。

3. **沒有告知何時該放棄**。「50 次迭代」是硬上限，但實際上 Claude 在 1-3 次後就標記 failed。人類的判斷（何時算「夠了」）沒有被編碼進指令。

4. **缺少探索方向指引**。指令說「深入思考為什麼」，但沒有提供可能的思考方向（改假說 vs 改量化方式 vs 改事件定義 vs 改基線）。

### 4.2 v2 改進（Round 2）

v2 針對上述問題做了三項改進：

#### 改進 A：三階段工作流（Think → Execute → Reflect）

CLAUDE.md 中的迭代流程從線性「跑 → 看 → 改」改為：

```markdown
### Phase 1: Think（不執行任何代碼）
1. 閱讀上次結果
2. 寫出：根本原因 → 3 個方向 → 選擇與理由 → 預期結果

### Phase 2: Execute
3. 修改代碼
4. CV 掃描：--scan --cv -s {strategy}
5. 記錄結果

### Phase 3: Reflect
6. 結果是否符合預期？
7. 更新進度檔
```

強制 Claude 在動手前先思考「為什麼」，並在事後反思「是否符合預期」。

#### 改進 B：診斷式 Hook 回饋

v1 hook 對所有策略給出相同的泛用指令。v2 hook 讀取上次的 `last_sig_rate` 和 `last_dir_consistency`，給出針對性診斷：

| 指標組合 | 診斷 | Hook 建議 |
|---------|------|----------|
| sig < 3%, dir 任意 | 遠低於隨機基準 | 考慮假說是否本質錯誤（Cohen's d ≈ 0 → 放棄） |
| sig < 5%, dir > 60% | 方向正確但覆蓋不足 | 放寬事件定義、降低閾值 |
| sig > 5%, dir < 55% | 覆蓋足但無方向性 | 假說可能是波動率信號，考慮放棄或轉型 |
| sig > 5%, dir > 60% | 接近通過 | 邏輯層面改變優先，小心 overfitting |
| stagnant ≥ 5 | 連續無改善 | 建議放棄並寫完整分析 |

#### 改進 C：進度檔新增欄位

```json
{
  "last_sig_rate": 4.4,        // 上次顯著率（供 hook 診斷用）
  "last_dir_consistency": 73.7, // 上次方向一致性
  "stagnant_count": 4,          // 連續無改善次數
  "cv_result": {                // CV 結果（通過後記錄）
    "folds_passed": 5,
    "per_fold": [...]
  },
  "abandon_reason": "..."       // 放棄原因（abandoned 狀態時必填）
}
```

新增的狀態值：
- `cv_passed`：通過 CV 驗證（最終通過）
- `cv_failed`：單次掃描通過但 CV 未通過
- `abandoned`：主動放棄（附原因）

### 4.3 Hook 設定

```json
{
  "hooks": {
    "Stop": [{
      "matcher": "",
      "hooks": [{
        "type": "command",
        "command": "bash .claude/hooks/hypothesis-loop.sh",
        "timeout": 10000
      }]
    }]
  }
}
```

**運作方式**：
- `exit 0`：無事可做，正常結束
- `exit 2`：有工作，將 stdout 內容作為新指令餵給 Claude
- 進度檔 `hypothesis_progress.json` 是狀態機
- 啟用：設定 `completed: false`
- 停用：設定 `completed: true` 或刪除進度檔

**重要**：`permissions.allow` 列出所有允許自動執行的指令。`python -c "..."` 未列入，會觸發人工確認。v2 hook 改為寫 `tmp/_hook_logic.py` 後用 `python3` 執行。

---

## 5. Overfitting 問題

### 5.1 為什麼迭代 50 次會 overfit？

假設有一個完全隨機的策略（沒有預測力）。對 ~2800 支股票做 p<0.05 檢定：
- 隨機期望約 140 支「顯著」（2800 × 5%）
- 每次修改代碼，顯著的股票集合會隨機漂移
- 迭代 50 次，相當於做了 50 次「抽獎」
- 總有某次剛好超過門檻

這叫 **researcher degrees of freedom**（研究者自由度）：你選擇何時停止、選擇哪個版本呈報，本身就是一種 overfitting。

### 5.2 CV 如何防止 overfitting？

滾動窗口 CV 要求策略在**不同時間段都有效**：

```
Fold 1: 2023H2-2024H1
Fold 2: 2024
Fold 3: 2024H2-2025H1
Fold 4: 2025
Fold 5: 2025H2-2026Q1
```

- 如果策略只是碰巧在某段時間有效，其他 fold 會暴露這一點
- 門檻：≥3/5 folds 三項指標全過（majority vote）
- dual_window 正是被這個機制淘汰的（只有 2/5 folds 通過）

### 5.3 CV 的局限

CV 不是萬能的：
- **5 個 fold 不是獨立的**：相鄰 fold 有半年重疊期，不完全獨立
- **仍然可以 overfit CV 本身**：如果你看了 per-fold 結果再修改策略，就是在 overfit CV
- **時間序列的 non-stationarity**：市場結構可能隨時間改變，過去穩健不代表未來穩健

### 5.4 conviction 和 contrarian_smart 為什麼不太可能是 overfit？

1. **都是第一次就通過**：沒有經過任何迭代修改，不存在 researcher degrees of freedom
2. **5/5 folds 全過**：不是靠某個特定時段拉高平均
3. **有行為金融學理論支撐**：
   - conviction 基於 disposition effect（最被廣泛驗證的行為偏差）
   - contrarian_smart 基於恐慌承接（高成本逆向決策 = 信心信號）
4. **效果量穩定**：不是某個 fold 極強拉高平均，而是每個 fold 都在合理範圍內

---

## 6. 三個開放問題

### 6.1 其他八項策略是條件寫錯還是迭代不夠？我什麼時候該放棄？

**分類討論**：

| 策略 | 診斷 | 值得繼續嗎？ |
|------|------|-------------|
| large_trade_scar | 假說本身錯誤（regression to mean） | ❌ 否。SCAR 排名本質是噪音 |
| contrarian_broker | 方向正確（73.7%）但覆蓋率不足（4.4%） | ⚠️ 可能。contrast score 邏輯正確，可能需要更好的 selector |
| dual_window | CV 暴露不穩健 | ❌ 否。時間不穩定性是結構問題 |
| exodus | 波動率信號，非方向性 | ❌ 否（除非改成波動率策略） |
| cross_stock | 未測試（缺 cluster） | ⚠️ 待定。需先完成 cluster discovery |
| ta_regime | 邏輯正確但事件太稀有 | ⚠️ 可能。降低 z_threshold 可能增加覆蓋 |
| concentration | 未測試（runner 注入問題） | ⚠️ 待定。技術問題，非假說問題 |
| herding | 最準（87.1%）但最稀有（0.9%） | ⚠️ 高潛力。能否放寬定義同時保持方向？|

**放棄的判斷框架**：

```
                  方向一致性高？
                  /          \
                是            否
               /                \
        覆蓋率夠？           exodus 類型
        /       \            （波動率信號，
       是        否            放棄方向性假說
      /            \           或改成波動率策略）
  ✅ 通過     覆蓋率有提升空間？
              /              \
             是               否
            /                  \
      繼續迭代              large_trade_scar 類型
      (herding,             （假說本身錯誤，放棄）
       ta_regime,
       contrarian_broker)
```

**具體建議**：

- **herding**（87.1% dir, 0.9% sig）：最值得迭代。嘗試降低 herding_threshold 或改用連續指標而非二元閾值。信號品質極高，只需提升頻率。
- **ta_regime**（74.0% dir, 1.4% sig）：可嘗試降低 z_threshold 到 1.5 或縮短 window_days，增加事件數。
- **contrarian_broker**（73.7% dir, 4.4% sig）：已迭代 9 次，每次都在 4-4.5% 打轉。可能已接近此假說的天花板。
- **cross_stock / concentration**：技術障礙，非假說問題。解決注入問題後值得測試。

### 6.2 通過的兩項策略如何更優化而不 overfit？

**原則：不要碰 selector/filter 邏輯。** 這兩個策略第一次就通過，代碼邏輯不需要修改。修改它們只會引入 overfitting。

**可以安全做的事情**：

1. **組合信號**：conviction + contrarian_smart 同時觸發時，信號是否更強？這不是修改策略，而是研究信號之間的關係。

2. **回測實際交易策略**：目前只驗證了「事件後 forward returns 顯著異於基準」，但沒有模擬實際交易（考慮交易成本、滑價、持倉限制）。用 `domain/backtest.py` 的 `run_backtest()` 做端到端回測。

3. **分析事件特徵**：
   - 哪些市值區間效果最強？（大型股 vs 中小型）
   - 哪些產業效果最強？
   - 信號密度隨時間的變化？
   - 這些分析不會改變策略本身，只是理解它的適用範圍。

4. **Out-of-sample 等待**：最強的驗證是等待新數據。每天 ETL 增量更新後，可以追蹤 conviction/contrarian_smart 事件的實際後果。

**不應該做的事情**：

- 調整 `min_profit_ratio`、`min_brokers`、`drop_pct` 等參數以「改善」結果
- 加入額外的 filter 條件（如市值、產業）來「精煉」信號
- 根據 per-fold 結果微調邏輯

### 6.3 提示詞設計：v1 vs v2

v1 的問題是太過程序化（跑 → 看 → 改 → 跑），Claude 陷入機械式微調。v2 做了以下改進（已實作）：

| 改進 | v1 問題 | v2 解法 | 實作位置 |
|------|---------|---------|---------|
| Think → Execute → Reflect | 直接改代碼 | 強制先寫分析再動手 | CLAUDE.md |
| 診斷式 hook 回饋 | 泛用指令 | 根據 sig/dir 指標給針對性建議 | hypothesis-loop.sh |
| 創意種子 | 沒有方向指引 | 開放式問題列表 | CLAUDE.md |
| 放棄標準 | 只有 50 次硬上限 | 決策樹 + 連續無改善計數 | CLAUDE.md + progress JSON |
| CV 驗證 | 單次掃描即判定 | ≥3/5 folds 全過 | hypothesis_runner.py |
| 進度檔診斷欄位 | 只有 status/iterations | 加入 last_sig/dir/stagnant | progress JSON |

**提示詞架構**：

```
CLAUDE.md
├── Phase 1/2/3 工作流（結構化思考）
├── 放棄判斷決策樹（何時該停）
├── 思考清單（失敗時的 5 個問題）
├── 優化原則（通過策略不碰邏輯）
└── 創意種子（開放式探索方向）
       ↓
hypothesis-loop.sh（動態指令）
├── 讀取 last_sig_rate / last_dir_consistency
├── 根據指標組合給出診斷（4 種情境）
├── stagnant ≥ 5 時建議放棄
└── 要求 --scan --cv（非單次掃描）
       ↓
hypothesis_progress.json（狀態機）
├── last_sig_rate, last_dir_consistency（診斷用）
├── stagnant_count（連續無改善追蹤）
├── cv_result.per_fold（CV 詳細記錄）
├── abandon_reason（放棄原因，必填）
└── status: pending/exploring/cv_passed/cv_failed/abandoned
```

---

## 附錄

### A. CLI 用法

```bash
# 單次掃描（舊模式）
uv run python -m broker_analytics hypothesis --scan -s conviction

# 5-fold CV 掃描（新模式，推薦）
uv run python -m broker_analytics hypothesis --scan --cv -s conviction

# 自訂 fold 通過門檻
uv run python -m broker_analytics hypothesis --scan --cv -s conviction --min-folds 4

# 列出所有策略
uv run python -m broker_analytics hypothesis --list
```

### B. 檔案結構

```
.claude/
├── hooks/
│   └── hypothesis-loop.sh    # Stop hook（自動循環）
├── settings.json              # 權限 + hook 設定
└── settings.local.json        # 本地覆蓋

data/reports/
├── hypothesis_progress.json   # 狀態機（迭代進度）
├── hypothesis_final.md        # 第一輪總結
├── contrarian_broker.md       # 各策略詳細報告
├── dual_window.md
├── conviction.md
├── exodus.md
├── contrarian_smart.md
├── ta_regime.md
└── herding.md

broker_analytics/domain/hypothesis/
├── types.py                   # SymbolData, GlobalContext, CVFold, HypothesisConfig
├── selectors.py               # 7 個 selector 函數
├── filters.py                 # 9 個 filter 函數
├── outcomes.py                # outcome_forward_returns
├── baselines.py               # baseline_unconditional
├── stat_tests.py              # stat_test_permutation
└── registry.py                # 10 個策略配置

broker_analytics/application/services/
└── hypothesis_runner.py       # run_scan(), run_scan_cv()
```

### C. 執行時間參考

| 操作 | 時間 |
|------|------|
| 單次全市場掃描（1 fold） | ~80-120s |
| 5-fold CV（conviction） | ~534s（~9 分鐘） |
| 5-fold CV（contrarian_smart） | ~376s（~6 分鐘） |
| 5-fold CV（dual_window） | ~606s（~10 分鐘） |
