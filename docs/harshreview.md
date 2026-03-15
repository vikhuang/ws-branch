# Harsh Review：ws-branch 假說驗證流程的系統性問題

> 撰寫日期：2026-03-15
> 範圍：ws-branch hypothesis pipeline + ws-quant backtest-external-daily 整合
> 目的：逐節點盤點所有 bias、設計缺陷、跨專案 gap

---

## 問題分類框架

| 類別 | 代號 | 定義 | LLM 根因 |
|------|------|------|---------|
| **偷懶** | LAZY | 資料或工具已存在，卻沒用上 | 傾向「能動就好」，不主動檢查是否有更正確的做法 |
| **不夠謹慎** | CARE | 忽略 look-ahead bias 或未驗證假設 | 對統計檢定的邊界條件缺乏敏感度，不會自發性問「這個資料在當時可得嗎？」 |
| **設計缺陷** | DESIGN | 架構層面的結構性問題 | 傾向最小可行實作，不做端到端的 bias audit |
| **跨專案 gap** | XGAP | 兩個專案各自合理，合在一起出問題 | 沒有「站在消費者角度」審視上游輸出的品質；預設上游是乾淨的 |

---

## 全流程節點圖

```
 pnl_df (全期間累計)     pnl_daily_df (逐日)
        │                       │
        ▼                       │ (有但沒用)
   ┌─────────┐                  │
   │ Selector │ ←── !1 LAZY     │
   └────┬────┘                  │
        ▼                       │
   ┌─────────┐                  │
   │ Filter   │ ←── ✅ 乾淨（但吃了髒 broker list）
   └────┬────┘
        ▼
   ┌─────────┐
   │ Events  │ ←── !2 CARE（含 2021 暖身期事件）
   └────┬────┘
        │
   ┌────┴────────────────┐
   │                     │
   ▼                     ▼
 CV flow              Export flow
 ┌────────┐           ┌─────────┐
 │ per-fold│           │全期間    │ ←── !3 CARE
 │ window  │           │合併判定  │
 └───┬────┘           └────┬────┘
     │                     │
     ▼                     ▼
 CV 結果               Signal CSV
 ←── !4 DESIGN          ┌─────────────────┐
                        │ symbol,date,dir  │ ←── !5 DESIGN
                        │ (無 strength)    │
                        └────────┬────────┘
                                 │
                        ═════════╪═══════════ 專案邊界
                                 │
                                 ▼
                        ws-quant backtest
                        ┌────────────────┐
                        │ T+1 open entry │
                        │ hold N days    │ ←── !6 XGAP
                        │ 每事件獨立 1 lot│ ←── !7 XGAP
                        │ 無 beta 分離   │ ←── !8 XGAP
                        └────────────────┘
```

---

## !1 — Selector 用全期間 PNL（LAZY）

### 問題

`select_top_k_by_pnl()` 直接讀 `data.pnl_df`（2021-01 到 2026-03 的累計
total_pnl），無論是 CV 還是 export 模式都一樣。

```python
# selectors.py:28-30
def select_top_k_by_pnl(data, ctx, params):
    top_k = params.get("top_k", 20)
    df = data.pnl_df.sort("total_pnl", descending=True).head(top_k)
    return df["broker"].cast(pl.Utf8).to_list()
```

2023 年的事件，用了包含 2024-2026 績效的排名來決定「誰是聰明錢」。

### 為什麼說「偷懶」

`pnl_daily_df`（逐日損益）已經存在於 `SymbolData` 中，且
`_rolling_ranking_to_date(pnl_daily_df, years, train_end)` 這個 helper 也已經
寫好了（selectors.py:408-441）。`select_niche_top_brokers`（contrarian_broker
的 selector）已經在用它。

**工具齊全，只是 `select_top_k_by_pnl` 沒接上。**

### 影響範圍

| 策略 | Selector | 受影響？ |
|------|----------|---------|
| conviction | `select_top_k_by_pnl` | **是** |
| contrarian_smart | `select_top_k_by_pnl` | **是** |
| exodus | `select_top_k_by_pnl` | **是** |
| herding | `select_top_and_bottom_k`（同用 pnl_df） | **是** |
| dual_window | `select_dual_window_intersection`（用 `_rolling_top_k`，取 max_date 而非 train_end） | **是** |
| concentration | `select_concentrated_brokers`（用 snapshot HHI） | **是**（間接） |
| contrarian_broker | `select_niche_top_brokers`（用 `_rolling_ranking_to_date` + train_end） | **否** ✅ |

**6/7 通過策略的 selector 有 look-ahead bias。**

### LLM 根因

- **不會自發性問「這個 DataFrame 的時間範圍是什麼？」**。LLM 看到
  `pnl_df.sort("total_pnl")` 覺得「功能正確」就收工了，不會進一步追問
  「這個 total_pnl 是截到什麼時候的？」
- **已有正確實作但沒推廣**。`select_niche_top_brokers` 已經示範了正確做法
  （用 `train_end_date` 截斷），但在實作其他 selector 時沒有複製同樣的
  pattern。這是典型的「每個函數獨立寫，不做 cross-function consistency
  check」。

---

## !2 — Export 事件包含 2021-2022 暖身期（CARE）

### 問題

檢查實際 export 的 CSV：

| 策略 | 最早事件日期 | 問題 |
|------|-------------|------|
| contrarian_smart | **2021-01-05** | FIFO 暖身期（2021-01 ~ 2022-12） |
| concentration | **2021-01-04** | 同上 |
| conviction | **2021-02-22** | 同上 |
| herding | **2021-01-08** | 同上 |
| dual_window | **2021-01-11** | 同上 |
| exodus | **2021-02-01** | 同上 |
| contrarian_broker | **2021-02-18** | 同上 |

CLAUDE.md 明確寫著：「FIFO accumulates from 2021-01, performance measured from
2023-01」。但 export 流程沒有任何日期下限，把 2021-2022 暖身期的事件也匯出了。

### 為什麼嚴重

2021-2022 的 PNL 排名極度不穩定（FIFO 才剛開始累積），用全期間 PNL 選的
top-K 在 2021 年根本不具意義。這些事件在 ws-quant 回測中會被當成有效信號
交易。

### LLM 根因

- **不檢查輸出的合理性**。LLM 寫完 export 後沒有做最基本的 sanity check
  （例如「最早的事件日期是什麼？」）。
- **CLAUDE.md 的 guardrail 是給人看的，不是給程式看的**。「performance
  measured from 2023-01」寫在文件裡，但沒有被硬編碼到 export 流程中。LLM
  不會主動把文件中的約束轉化為程式碼中的 assertion。

---

## !3 — Export 的 Significance 用全期間判定（CARE）

### 問題

`run_export()` 對每支股票跑一次完整 pipeline（不傳 train/test 日期），用全期間
的 forward returns 做 permutation test，判定 `conclusion == "significant"`
才匯出。

假設股票 A：
- 2023 年信號完全隨機（無效）
- 2025 年信號非常強
- 全期間合併 → 被判 significant → 2023 年的無效事件也被匯出

### 為什麼說「不夠謹慎」

CV 流程已經證明了「分時段看很重要」（dual_window 正是被 CV 攔下的例子），
但 export 流程完全沒借鏡這個教訓——用全期間合併的邏輯，等於退回到 CV 之前
的驗證水準。

### LLM 根因

- **CV 和 export 是分開實作的，沒有統一的 bias 審計**。LLM 實作 CV 時很仔細
  地加了 per-fold windowing，但到了 export 時把它當成一個獨立功能來寫，沒有
  問「export 的品質保證應該不低於 CV」。
- **功能導向而非品質導向**。LLM 的目標是「讓 --export 跑出 CSV」，不是
  「讓 CSV 中的每一行都是在實際可得資訊下生成的」。

---

## !4 — CV 流程的 Selector 未 Window 化（DESIGN）

### 問題

`run_scan_cv()` 確實把 `train_end_date`、`test_start_date`、`test_end_date`
注入 params（line 263-268），且 events 被正確截到 test window（line 527-534）。

**但 `select_top_k_by_pnl` 不讀這些日期。** CV 的日期 windowing 只做了一半：
events 有 window，selector 沒有。

```
CV 宣稱做的事：              CV 實際做的事：

 train ≤ 2023-06-30          selector 用 2026 PNL   ← 洩漏
 test 2023-07 → 2024-06     events 截到 test window ← 正確
                             forward returns 在 test ← 正確
                             stat test 在 test       ← 正確
```

### 為什麼是設計缺陷

這不是某個函數的 bug，而是 pipeline 架構的問題：
- `_run_pipeline()` 是統一的 5-step 執行器
- 日期 windowing 只在 step 2（filter）之後做 post-hoc 截斷
- step 1（selector）完全不受 CV fold 日期的約束
- `SymbolData` 載入的 `pnl_df` 是全期間的，沒有 per-fold 版本

正確做法：`_load_symbol_data()` 應該接受 `train_end_date`，對 `pnl_df` 和
`pnl_daily_df` 做截斷後再傳入 pipeline。或者，所有 selector 都應該讀
`params["train_end_date"]` 並自行截斷。

### LLM 根因

- **把 CV windowing 當成「event 的事」而非「整個 pipeline 的事」**。LLM 加
  CV 支援時，思考的是「怎麼讓 events 只在 test window 內」，而非「怎麼確保
  整個 pipeline 在 train_end 之前都不看未來資料」。
- **增量開發的盲點**。CV 是後來加的功能（v2 改進），而 selector 在 v1 就
  寫好了。LLM 在加 CV 時沒有回頭審計 selector 是否兼容 CV 的日期約束。

---

## !5 — Signal Contract 無信號強度（DESIGN）

### 問題

Export 輸出格式：

```csv
symbol,date,direction
2330,2025-10-21,long
2330,2025-10-22,long
```

只有 `direction`（long/short），沒有 `signal_value`。ws-quant 的 Signal
Contract spec 支援 `signal_value` 欄位（optional, default 0.0），但 ws-branch
沒有填。

### 後果

- ws-quant 對每個事件等權重處理（1 lot per signal）
- 「3 家 top-K 券商同時加碼」和「15 家 top-K 券商同時加碼」在回測中完全等價
- 無法做基於信號強度的部位管理

### 為什麼是設計缺陷

hypothesis pipeline 的 filter 已經有豐富的 context 可以量化信號強度：
- conviction：加碼券商數量、浮盈比例
- contrarian_smart：恐慌日跌幅、逆勢券商數
- herding：群聯分歧百分位

這些資訊在 filter 計算過程中都有，但 filter 的 output schema 被定義為
`DataFrame[date, direction]`——二值化後就丟掉了。

### LLM 根因

- **過早標準化**。types.py 定義了 filter output 為 `[date, direction]`，
  LLM 嚴格遵守了這個 schema（「好的軟體工程」），但沒有質疑 schema 本身
  是否足夠。
- **先做統計驗證，後想交易**。hypothesis pipeline 的設計目標是「這個事件的
  forward return 是否顯著異於基準」——對這個問題，二值方向確實夠用。但當
  pipeline 延伸到 export → ws-quant 回測時，二值化就不夠了。LLM 沒有在
  pipeline 延伸時重新審視 schema 設計。

---

## !6 — ws-quant 不知道信號有 look-ahead（XGAP）

### 問題

ws-quant signal_contract.md 的責任劃分（lines 86-96）：

> **Upstream (ws-branch)**: T→T+1 date mapping
> **ws-quant**: Execution, cost modeling, exit rules, no look-ahead by design

ws-quant 假設上游信號是「在 date 當天收盤後可得的資訊」生成的，自己負責
T+1 open 進場。但 ws-branch 的信號實際上用了：

1. 全期間 PNL 排名（2026 資訊回溯到 2023）
2. 全期間 significance 篩選（2025 的強勢讓 2023 的事件被匯出）
3. 2021-2022 暖身期的事件（FIFO 尚未穩定）

**ws-quant 的 T+1 紀律完美解決了「執行層」的 look-ahead，但上游「信號層」
的 look-ahead 它完全看不見也無法防禦。**

### 為什麼是跨專案 gap

兩個專案的 look-ahead 防禦各自合理但互相不銜接：

```
ws-branch 的防禦：          ws-quant 的防禦：
├─ CV per-fold windowing    ├─ T+1 open entry
├─ events 截到 test window  ├─ rolling features .shift(1)
├─ permutation test         ├─ no look-ahead in bar_builder
│                           │
└─ ⚠️ selector 用全期間 PNL └─ ⚠️ 預設上游信號是乾淨的
   （自己沒發現）               （無法驗證）
```

### LLM 根因

- **ws-branch 的 LLM 和 ws-quant 的 LLM 沒有共享 context**。ws-branch 的
  開發過程不知道 ws-quant 會怎麼消費 CSV；ws-quant 的開發過程預設 CSV 是
  乾淨的。
- **Signal Contract 只定義了格式，沒有定義品質保證**。contract 說了
  「欄位是 symbol, date, direction」，但沒有說「date 必須是信號在該日可
  得的」或「不得包含 look-ahead」。這是 contract 設計的疏漏。
- **信任邊界問題**。在微服務/多模組架構中，每個模組應該對輸入做 validation。
  ws-quant 做了格式驗證（重複檢查、方向 normalize），但沒有做語義驗證
  （例如「最早的信號日期是否在合理範圍內？」）。LLM 擅長實作明確的 spec，
  但不擅長質疑 spec 本身的完備性。

---

## !7 — ws-quant 每事件獨立回測、無部位聚合（XGAP）

### 問題

ws-quant `daily_simulator.py` 對每個 `(symbol, date)` 信號建立獨立的 Trade
object，1 lot per signal，不做部位聚合。

conviction 策略 export 了 9,499 個事件 / 219 支股票：
- 每股平均 43 個事件 / ~750 交易日 ≈ 每 17 天觸發一次
- 如果持有 20 天（report 顯示 20d Sharpe 最高 = 6.56），**倉位持續疊加**
- 某些高頻股票（如 006208: 329 事件 = 每 2.3 天一次）幾乎永遠在場

### 後果

- 20d hold 的 Sharpe 6.56 裡面包含了大量**重疊持倉的效果**
- 5/7 策略 long-only → 重疊持倉 ≈ 持續做多 → 在 2023-2026 台股牛市中
  自然獲利
- 回測結果無法區分「信號的 alpha」和「一直做多的 beta」

### 為什麼是跨專案 gap

ws-branch 的 hypothesis pipeline 在統計驗證時，是把每個事件的 forward return
視為**獨立觀測**做 permutation test。這個假設在統計檢定中或許合理（事件間隔
大於 forward return horizon 時近似獨立）。

但 ws-quant 的回測把這些「統計上近似獨立的事件」轉成了「同時在場的重疊部位」，
改變了風險特性。**ws-branch 驗證的是單事件 alpha，ws-quant 回測的是多事件
疊加的 portfolio 效果——兩者不是同一件事。**

### LLM 根因

- **不會做「如果我是 ws-quant，收到這個 CSV 會怎樣？」的角色模擬**。
  ws-branch 的 LLM 專注於統計顯著性，不會想到下游回測的持倉管理問題。
- **事件獨立性假設的隱含傳遞**。hypothesis pipeline 假設事件近似獨立（用於
  permutation test），這個假設被 Signal Contract 隱含地傳遞到 ws-quant，
  但 ws-quant 的持倉疊加機制違反了這個假設。兩邊都沒有意識到這個衝突。

---

## !8 — 60d Sharpe 的 Beta 汙染（XGAP + CARE）

### 問題

回測結果顯示清晰的 horizon 遞增 pattern：

| 策略 | 1d | 5d | 10d | 20d | 60d |
|------|---:|---:|----:|----:|----:|
| conviction | 0.18 | -0.60 | **5.10** | **6.56** | **8.12** |

1-5d 為負或接近零，10d 以上急劇上升。這個 pattern 的最可能解釋：

- **短期**：信號沒有 1-5 天的預測力（或被交易成本吃掉）
- **長期**：信號觸發後長期持有 ≈ long-only exposure ≈ 台股 beta

`hypothesis_research_report.qmd` 已經提到了這個問題（「60 日 Sharpe 極高需
謹慎解讀」），但**沒有實際做 beta 分離**。

### 為什麼是跨專案 gap + 不夠謹慎

- ws-quant 提供了 `portfolio-sim` 命令（有資本限制和帳簿深度），但
  hypothesis report 用的是基礎的 `backtest-external-daily`（無資本限制）
- ws-branch 的 report 指出了問題但沒有解決——典型的「知道但沒做」
- 一個最基本的 beta 分離方法（同期大盤 return 扣除）不需要任何新工具，
  但沒有人做

### LLM 根因

- **傾向報告發現而非修復問題**。LLM 在 report 中寫了「需謹慎解讀」，但
  沒有接著做 beta 分離計算。這是 LLM 的典型行為模式：提出風險警告，但不
  主動執行額外分析來量化風險。
- **研究報告的「完成」定義太寬鬆**。LLM 把「跑完回測 + 寫完報告」當成
  完成，而非「回測結果經過 bias 審計 + beta 分離後仍然成立」才算完成。

---

## !9 — conviction 家族的信號相關性（CARE）

### 問題

7 個通過策略中，3 個共用 `filter_conviction_signals`：

| 策略 | Selector | Filter | 10d Sharpe |
|------|----------|--------|-----------|
| conviction | top_k_by_pnl | conviction_signals | 5.10 |
| dual_window | dual_window_intersection | conviction_signals | 4.08 |
| contrarian_broker | niche_top_brokers | conviction_signals | 4.27 |

這三個策略的事件日期高度重疊（同一個 filter，只是 broker 集合不同）。
在 ws-quant 回測中，如果同時跑三個策略，等於對同一個信號做 3x leverage。

CLAUDE.md 已經標註了「contrarian_broker、dual_window 與 conviction 共用
conviction filter，信號高度相關」，hypothesis_exploration_guide.md 也指出
「獨立信號源 ≈ 4 個」。

但 export 仍然把 7 個策略各自匯出，且 report 的回測把 7 個策略並排比較，
給人「有 7 個獨立信號」的印象。

### LLM 根因

- **按策略組織而非按信號源組織**。整個 pipeline 的架構是「10 個策略各自
  獨立跑」，不會自動偵測策略之間的信號相關性。
- **文件和程式碼的斷裂**。CLAUDE.md 知道相關性問題，但 export 程式碼不
  知道——「知識在文件裡，行為在程式裡」。

---

## 問題嚴重程度總表

| # | 問題 | 類別 | 嚴重度 | 影響範圍 | 修復難度 |
|---|------|------|--------|---------|---------|
| !1 | Selector 用全期間 PNL | LAZY | **致命** | 6/7 策略的 CV + export | 低（接上已有的 helper） |
| !2 | Export 含 2021-2022 暖身期事件 | CARE | **高** | 所有 export CSV | 低（加日期下限） |
| !3 | Export significance 全期間判定 | CARE | **高** | 所有 export CSV | 中（需改 export 流程） |
| !4 | CV 的 selector 未 window 化 | DESIGN | **致命** | 所有 CV 結果的可信度 | 中（需改 data loading 或 selector） |
| !5 | Signal Contract 無信號強度 | DESIGN | **中** | ws-quant 回測品質 | 中（需改 filter output schema） |
| !6 | ws-quant 不知信號有 look-ahead | XGAP | **高** | 回測結果的可信度 | 低（加 contract 約束） |
| !7 | 重疊持倉 ≠ 獨立事件 | XGAP | **高** | 10d+ 回測的 Sharpe 被灌水 | 中（需 portfolio-sim 或去重疊） |
| !8 | Beta 未分離 | XGAP+CARE | **高** | long-only 策略的 alpha 未知 | 低（扣大盤 return） |
| !9 | conviction 家族信號相關 | CARE | **中** | 多策略組合的風險 | 低（文件已知，需 export 反映） |

---

## LLM 行為模式總結

### 模式 A：「能跑就好」

LLM 傾向最小可行實作——函數簽名正確、測試通過、CLI 能用就交付。不會主動
做 bias audit（「每個 input DataFrame 的時間範圍是什麼？在使用時這些資料
是否已可得？」）。

**受影響的問題**：!1, !2, !3

### 模式 B：「增量開發不回頭」

新功能（如 CV）加上去時，只確保新功能本身正確，不回頭審計既有程式碼是否
兼容新功能的假設。CV 加了日期 windowing，但沒回頭檢查 selector 是否尊重
這些日期。

**受影響的問題**：!1, !4

### 模式 C：「報告風險但不修復」

LLM 擅長在報告中寫出風險警告（「60d Sharpe 需謹慎解讀」、「信號高度
相關」），但不會主動執行額外的分析來量化或解決這些風險。風險被降級為
「future work」而非 blocker。

**受影響的問題**：!8, !9

### 模式 D：「預設上游是乾淨的」

在多模組架構中，LLM 實作下游模組時預設上游輸出是正確的。ws-quant 做了
格式驗證但不做語義驗證。Signal Contract 定義了格式但不定義品質保證。

**受影響的問題**：!6, !7

### 模式 E：「統計正確 ≠ 交易正確」

hypothesis pipeline 在統計層面是嚴謹的（permutation test, Cohen's d, BH-FDR,
5-fold CV），但這些統計保證不能直接轉移到交易回測。事件獨立性、部位管理、
beta exposure 都是統計框架不涵蓋但交易框架必須處理的問題。

**受影響的問題**：!5, !7, !8

---

## 修復優先順序建議

### Phase 1：修復 bias（阻擋性問題）

1. **!1 + !4**：`select_top_k_by_pnl` 改用 `_rolling_ranking_to_date(pnl_daily_df, years, train_end)`。所有用 `pnl_df` 的 selector 都要改。
2. **重跑 CV**：修完後重跑 7 個策略的 5-fold CV，看哪些還能通過。
3. **!2**：export 加日期下限（`>= 2023-01-01`），排除暖身期事件。
4. **!3**：export 改為 per-fold 或 rolling window 判定 significance，而非全期間合併。

### Phase 2：修復回測品質

5. **!8**：對回測結果做 beta 分離（扣同期台股加權指數 return）。
6. **!7**：用 ws-quant 的 `portfolio-sim` 重跑（有資本限制），或在 export 時對同一股票的連續事件去重疊（例如：前一事件持有期內不觸發新事件）。
7. **!5**：filter output schema 擴充 `signal_value`，export 時填入。

### Phase 3：強化跨專案契約

8. **!6**：Signal Contract v2 加入 metadata（signal_generation_date_range, look_ahead_free: bool, warmup_excluded: bool）。
9. **!9**：export 時標注策略之間的相關性，或提供 deduplicated 版本。
