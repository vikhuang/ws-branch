# 量化策略開發決策框架

> 版本：2026-03-19
> 適用範圍：event-based signal → 可交易策略的完整生命週期
> 生態系：ws-branch（信號）→ ws-quant（回測）→ ws-trade（執行）

---

## 三階段總覽

```
階段 1: 信號評估          階段 2: 執行優化          階段 3: Portfolio 模擬
ws-branch               ws-quant                 ws-quant portfolio-sim
「信號是真的嗎？」        「怎麼交易最好？」        「實際能賺多少？」

方法：CV + permutation    方法：A/B backtest        方法：capital-aware sim
無本金、per-trade         無本金、per-trade         有本金、有持倉限制
─────────────────────────────────────────────────────────────────────
            Signal CSV                   鎖定 exit spec
            是介面邊界                    + metadata YAML
```

---

## 階段 1：信號評估

### 目的

回答：「這個 alpha hypothesis 是真的嗎？」

### 1.1 CV 穩定性

5-fold rolling CV，每個 fold 三項全過才算 PASS。

| 指標 | 計算方式 | 好的標準 | 看什麼 |
|------|---------|---------|--------|
| CV folds pass | ≥3/5 folds 全過 | ≥ 3/5 | 策略是否在不同時期都有效 |
| Significance rate (sig%) | 顯著股票數 / 總股票數 | > 5%（random baseline = 5%） | 效果的普遍性 |
| Direction consistency (dir%) | 顯著股票中方向正確的比例 | > 60% | 方向預測準確性 |
| FDR-adjusted stocks | BH-FDR 校正後的顯著股票數 | ≥ 10 | 廣度（不是靠少數股票） |

**判斷邏輯**：

```
CV folds pass?
├── 4-5/5 + sig>10% + dir>70% → 強信號 → 進入 1.2
├── 3/5 或 dir 60-70% → 邊緣 → 可嘗試一輪參數調整
├── ≤ 2/5 → 假說不成立 → 放棄
│
特殊情況：
├── sig% 高但 dir% ≈ 50% → 波動率信號（如 exodus），非方向性
├── dir% 高但 sig% < 5% → 方向對但太稀疏（如 ta_regime）
└── 全 5/5 但 bias fix 後崩掉 → 假陽性（如 herding）
```

### 1.2 Alpha 真實性

扣 beta + 去重疊持倉後的 excess Sharpe 是核心指標。

| 指標 | 計算方式 | 好的標準 | 看什麼 |
|------|---------|---------|--------|
| Total Sharpe | per-trade avg/std × √252 | > 3 | 含 beta 的報酬 |
| Excess Sharpe | 扣大盤 IX0001 同期 return | > 2 | 真正的 alpha |
| Dedup Excess Sharpe | 去重疊持倉後 | > 1.5 | 非重疊部位灌水 |
| Win Rate | 正報酬 trades / 全部 trades | > 52% | 不是靠少數大贏 |
| Win/Loss Ratio | avg win / avg loss | > 1.5 | 贏的比輸的大 |
| Profit Factor | 總獲利 / 總虧損 | > 1.5 | 整體獲利能力 |

**判斷邏輯**：

```
Dedup Excess Sharpe?
├── > 3.0 → 強 alpha → 直接進入階段 2
├── 2.0-3.0 → 有 alpha，live 打 50% 折仍 > 1.0 → 可交易
├── 1.0-2.0 → 弱 alpha，打折後可能不足 → 需搭配其他策略
├── 0-1.0 → 大部分是 beta → 重新評估
└── < 0 → 無 alpha → 放棄（如 contrarian_smart 去重後 -0.20）

Live 經驗法則：live Sharpe ≈ backtest Sharpe × 0.5
  backtest excess 3.99 → 預期 live ~2.0
```

### 1.3 信號品質檢查

| 檢查項 | 問什麼 | 紅燈 |
|--------|--------|------|
| 年度穩定性 | 每年 return 都正嗎？ | 某一年貢獻 >60% 總利潤 |
| 股票集中度 | top-10 stock 貢獻多少？ | top-10 貢獻 > 50% |
| 方向偏差 | long/short 比例 | 100% long = 牛市依賴 |
| 事件頻率 | 每月幾次 event？ | < 10/month = 不實用 |
| 樣本量 | dedup 後多少 trades？ | < 200 → 統計不穩定 |

### 1.4 曾犯的錯誤（guardrails）

| 錯誤 | 後果 | 防範 |
|------|------|------|
| Selector 用全期間 PNL | look-ahead bias，假陽性 | 必須用 rolling ranking + train_end_date |
| 未扣 beta | 長持 long-only Sharpe 虛高 | 每筆 trade 扣 IX0001 同期 return |
| 未去重疊持倉 | 重疊 = 持續做多 = beta | dedup by hold period |
| 含 ETF/權證 | 造市行為 ≠ conviction | stocks-only（tickers_tw filter） |
| 含暖身期事件 | FIFO 未穩定的 PNL | events ≥ 2023-01-01 |
| FIFO 空頭當做空 | 出貨帳面效果 ≠ 主動做空 | 維持 long-only |
| 共用 filter code = 不獨立 | 低估了部分策略 | 每個 selector+filter 獨立評估 |

---

## 階段 2：執行優化

### 目的

回答：「已驗證的信號，用什麼交易規則最好？」

### 2.1 Hold Period（已完成）

| Horizon | 測什麼 | 結論 |
|---------|--------|------|
| 1d, 5d | excess Sharpe 為負 | 交易成本吃掉 alpha |
| **10d** | excess Sharpe 最高 | **validated baseline** |
| 20d | excess Sharpe 略高但 beta 佔比上升 | 邊際 |
| 60d | Sharpe 最高但 beta 主導 | 不可信 |

### 2.2 Exit Rule A/B 比較

同一份 Signal CSV，不同 exit spec，比較結果。

| 測試 | 做法 |
|------|------|
| SL/TP 調參 | 3% SL → 試 2%, 5%, 7%。同一份 CSV，不同 yaml |
| 動態出場 | 布林帶、trailing stop。需要 ws-quant 實作新 exit type |
| IS/OOS split | 2023-2024 優化參數，2025-2026 驗證。防 overfitting |

**核心指標**：

| 指標 | 意義 | 好的方向 | 判斷 |
|------|------|---------|------|
| Sharpe | 風險調整報酬 | ↑ | 新 > 舊 × 1.2 才值得換 |
| Max Drawdown | 最大回撤 | ↓ | 新 DD < 舊 DD |
| Calmar | 年化報酬 / MaxDD | ↑ | > 2 很好 |
| Profit Factor | 總獲利 / 總虧損 | ↑ | > 1.5 健康 |
| Avg Trade Return | 每筆平均報酬 | ↑ | > 成本 × 3（~200 bps） |
| Exit Reason 分布 | SL/TP/Time 各佔比 | — | Time > 90% = 有改善空間 |

**判斷邏輯**：

```
新 exit spec 結果 vs baseline（10d fixed）？

Sharpe 提升 > 20% AND OOS 也改善？
├── Yes → 採用新 exit spec
├── Sharpe ≈ baseline → 保留 baseline（簡單 > 複雜）
└── IS 改善但 OOS 退化 → overfitting → 拒絕

Exit Reason 分析：
├── 97% time_exit → SL/TP 幾乎沒觸發 → 可能設太寬，或信號很穩
├── 30%+ stop_loss → 信號有很多錯誤 entry → 可能需要更嚴格的 filter
└── 30%+ take_profit → alpha 快速實現 → 可能可以縮短 hold period
```

### 2.3 Decay Curve 分析

alpha 在持有期間如何衰減。

```
理想：
  Day 1-3:  alpha 快速累積（資訊被市場消化）
  Day 4-10: alpha 穩定（持有期間持續獲利）
  Day 11+:  alpha 消失（資訊已完全反映）
  → 10d hold 恰好在 alpha 消失前出場 ✅

問題：
  Day 1-3:  alpha 快速累積
  Day 4-7:  alpha 開始回吐
  Day 8-10: alpha 已回吐一半
  → 7d 出場可能比 10d 好 → 需要測試

更大問題：
  Day 1-5:  alpha 為負（成本 + 短期反向）
  Day 6-15: alpha 累積
  Day 16+:  alpha 穩定
  → 可能需要更長的持有期
```

分析方法：用 ws-branch 的 `compute_daily_car()`（已實作），看每日 direction-adjusted CAR 曲線。

### 2.4 Entry 優化（進階）

| 優化 | 做法 | 預期效果 |
|------|------|---------|
| T+1 open vs VWAP | 比較不同進場價 | 省 10-50 bps/trade |
| Limit order at support | 設限價單在支撐位 | 省滑價但可能 miss entry |
| 進場時段 | 開盤 vs 盤中 vs 收盤 | 依流動性最佳時段 |

需要 tick data，目前只有 2025+ 的。暫不可行。

---

## 階段 3：Portfolio 模擬

### 目的

回答：「以我的資金，這個策略實際能賺多少？」

### 3.1 設定

| 參數 | 意義 | 你的情境 |
|------|------|---------|
| 初始本金 | 可投入的總資金 | 200 萬 TWD |
| 每筆部位 | 單筆交易佔本金比例 | 5%（10 萬）或 10%（20 萬） |
| 最大同時持倉 | 資金能支撐的最大持倉數 | 10-20 支 |
| 信號頻率 | dedup 後每月觸發次數 | conviction ~50/month |
| 持倉佔用 | 10d hold × 信號頻率 | 同時 ~17 個持倉 |
| 資金缺口 | 持倉需求 vs 可用資金 | 200 萬 / 10 萬 = 20 → 勉強夠 |

### 3.2 Portfolio 指標

| 指標 | 計算方式 | 好的標準 | 意義 |
|------|---------|---------|------|
| Portfolio Sharpe | daily return 的 mean/std × √252 | > 1.5 可交易，> 2.0 優秀 | 整體風險調整報酬 |
| Portfolio MaxDD | equity curve 最大回撤 % | < 15% 可承受 | 最壞情況虧多少 |
| 年化報酬 | ending / starting - 1，年化 | > 20% 值得做 | 絕對報酬 |
| Calmar Ratio | 年化報酬 / MaxDD | > 2.0 | 報酬 vs 風險的效率 |
| 資金使用率 | avg(invested / total capital) | 40-80% 健康 | 資金效率 |
| 月度勝率 | 正報酬月份 / 總月份 | > 60% | 穩定性 |
| 最大連續虧損月 | 連續 return < 0 的月數 | ≤ 2 個月 | 心理承受力 |
| 換手率 | monthly trades / avg positions | — | 交易成本壓力 |
| 集中度風險 | top stock 貢獻 / total PnL | < 30% | 分散化 |

**判斷邏輯**：

```
Portfolio Sharpe?
├── > 2.0 → 優秀，可上線
├── 1.5-2.0 → 良好，可上線但保守操作
├── 1.0-1.5 → 一般，考慮加更多策略
└── < 1.0 → 不足，alpha 被約束吃掉

MaxDD?
├── < 10% → 保守，適合大部分人
├── 10-20% → 中等風險
├── 20-30% → 激進
└── > 30% → 太危險

如果 Portfolio Sharpe 低但 Per-trade Sharpe 高：
  → 問題在「資金不夠，太多信號被跳過」
  → 解法：加資金、減少信號（更嚴格 filter）、或加長 dedup 窗口

如果 MaxDD 高但 Sharpe 也高：
  → 問題在「集中度太高」或「單月大虧」
  → 解法：降低每筆部位比例、加 portfolio-level stop loss
```

### 3.3 多策略組合

| 組合 | 預期效果 | 條件 |
|------|---------|------|
| 單策略 conviction | Sharpe ~2.0（live 估計） | — |
| + concentration | Sharpe ↑ ~25%（獨立 alpha） | 兩者 event overlap 低 |
| + momentum_conviction | Sharpe ↑ 有限（74% event overlap） | 主要增加覆蓋而非分散 |

獨立策略組合的 Sharpe 估算：

```
如果兩個策略的相關性 ρ ≈ 0：
  S_combined ≈ √(S₁² + S₂²)

conviction S=2.0 + concentration S=1.5：
  S_combined ≈ √(4 + 2.25) = √6.25 ≈ 2.5

conviction S=2.0 + momentum_conviction S=3.0（但 ρ ≈ 0.7）：
  S_combined ≈ √(4 + 9 + 2×0.7×2×3) ≈ √21.4 ≈ 4.6
  但這是理論值，event overlap 限制了實際分散效益
```

---

## 完整決策鏈

```
                    階段 1                              階段 2                         階段 3
               信號是真的嗎？                       怎麼交易最好？                   實際能賺多少？
              ─────────────                        ─────────────                    ─────────────

Step 1.1      CV 4/5?
              ├── Yes ──→ Step 1.2
              └── No ──→ 放棄

Step 1.2      Excess Sharpe > 2?
              ├── Yes ──→ Step 1.3
              └── No ──→ 放棄

Step 1.3      品質檢查通過？
              (年度穩定、不集中、樣本夠)
              ├── Yes ──→ ─────────────→ Step 2.1
              └── 有問題 → 標註風險，繼續

                                        Step 2.1    Hold period A/B
                                                    (已完成: 10d 最優)
                                                    │
                                        Step 2.2    Exit rule A/B
                                                    新 > 舊×1.2 AND OOS 驗證?
                                                    ├── Yes → 採用
                                                    └── No → 保留 10d fixed
                                                    │
                                        Step 2.3    Decay curve 確認
                                                    alpha 在 10d 內還活著?
                                                    ├── Yes → 鎖定 exit spec
                                                    └── No → 調整 hold period
                                                    │
                                                    ─────────────→ Step 3.1

                                                                   Step 3.1    Portfolio sim
                                                                               Sharpe > 1.5?
                                                                               MaxDD < 15%?
                                                                               │
                                                                               ├── 都通過 → 上線
                                                                               │
                                                                               ├── Sharpe 低
                                                                               │   → 加策略?
                                                                               │   → 加資金?
                                                                               │
                                                                               └── MaxDD 高
                                                                                   → 減部位?
                                                                                   → 加 portfolio stop?
```

---

## 目前進度

| 階段 | 狀態 | 結果 |
|------|------|------|
| 1.1 CV | ✅ | conviction 4/5, momentum 4/5, concentration 4/5 |
| 1.2 Excess Sharpe | ✅ | conviction 3.99, momentum 6.22, concentration 3.08 |
| 1.3 品質檢查 | ⚠ | 100% long-only, momentum 樣本小(195) |
| 2.1 Hold period | ✅ | 10d validated |
| 2.2 Exit A/B | ❌ | 未做（97% time_exit = 有改善空間） |
| 2.3 Decay curve | ❌ | 未做 |
| 3.1 Portfolio sim | ❌ | 需要簡化版（不依賴 book depth） |

---

## 術語定義

| 術語 | 定義 | 我們的東西 |
|------|------|----------|
| Factor | 連續值，每支股票都有分數，可排序 | ❌ 我們不是 factor |
| Event Signal | binary，偶爾觸發，不能排序 | ✅ conviction event |
| Alpha Hypothesis | selector + filter 不可分割的行為假說 | ✅ 「誰做了什麼」|
| Signal Overlay | 用一個 signal 確認另一個 signal | ✅ conviction × watchlist |
| Exit Spec | 出場規則（持有期、SL/TP、技術指標） | YAML in ws-quant |
| IS/OOS | In-Sample / Out-of-Sample | CV folds 是 rolling IS/OOS |
| Decay Curve | alpha 在持有期間的衰減曲線 | compute_daily_car() |
| Signal Contract | ws-branch → ws-quant 的介面規格 | CSV + metadata YAML |
