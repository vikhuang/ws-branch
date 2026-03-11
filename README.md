# ws-branch

全市場券商分點交易資料的 PNL 回測系統。回答五個問題：

1. **哪些券商在賺錢？** — 依 FIFO 計算每家券商的已實現 + 未實現損益，全市場排名
2. **聰明錢在買什麼？** — 給定一支股票，看「在這支股票上歷史績效最好的券商」現在站哪邊
3. **大單有預測力嗎？** — 對個股進行大單偵測、統計驗證、TA 加權信號建構與回測
4. **聰明錢的累積行為能預測報酬嗎？** — 事件研究：PNL top-K 券商的買賣異常 → 多期 forward return 統計檢定
5. **各種券商行為假說是否成立？** — 可組合五步假說檢定框架，9 種策略（反差券商、加碼信號、集體撤退、逆勢操作等）

## 快速開始

```bash
uv sync

# Pipeline（全市場 2,839 股，約 15 分鐘）
uv run python etl.py                        # → data/daily_summary/
uv run python pnl_engine.py                 # → data/pnl_daily/ + data/pnl/ + data/derived/

# 合併版（將已停用券商代碼對應到存續券商，重新跑 FIFO）
uv run python generate_merge_map.py         # → data/derived/broker_merge_map.json
uv run python pnl_engine.py --merged        # → data/pnl_daily_merged/ + data/pnl_merged/ + data/derived/

# 查詢
uv run python -m broker_analytics ranking                  # 全市場券商排名
uv run python -m broker_analytics query 1440               # 單一券商績效
uv run python -m broker_analytics query 1440 --breakdown   # 含個股明細
uv run python -m broker_analytics symbol 2330               # 個股 smart money signal
uv run python -m broker_analytics symbol 2330 --detail 5    # 近 5 日明細
uv run python -m broker_analytics symbol 2330 --years 3     # 用 3 年滾動排名
uv run python -m broker_analytics rolling                   # 三年滾動 PNL 排名
uv run python -m broker_analytics rolling --years 2         # 指定窗口
uv run python -m broker_analytics rolling --xlsx            # 匯出 Excel
uv run python -m broker_analytics verify                    # 資料完整性驗證

# 事件研究（smart money accumulation → forward returns）
uv run python -m broker_analytics event-study 6285                          # 預設 top-20, 5d, 2σ
uv run python -m broker_analytics event-study 6285 --top-k 10 --window 10  # 調參數
uv run python -m broker_analytics event-study 6285 --threshold 1.5         # 降低門檻（更多事件）
uv run python -m broker_analytics event-study 6285 --no-robustness         # 跳過穩健性檢查

# 合併版查詢（所有命令均支援 --merged）
uv run python -m broker_analytics ranking --merged         # 合併版排名
uv run python -m broker_analytics query 1650 --merged      # 含原瑞士信貸持倉

# 個股信號分析
uv run python -m broker_analytics signal 2345              # 大單信號回測報告
uv run python -m broker_analytics signal 2345 --train-start 2023-01-01 --train-end 2024-06-30

# 全市場掃描（~7 分鐘）
uv run python -m broker_analytics scan                     # 預設：2億成交額、0.50%成本、1% FDR
uv run python -m broker_analytics scan --min-turnover 200000000 --cost 0.005 --fdr 0.01

# 信號匯出
uv run python -m broker_analytics export                   # 匯出信號 CSV
uv run python -m broker_analytics export --symbols 3665,2345

# 假說檢定框架（10 策略 × 可組合五步流水線）
uv run python -m broker_analytics hypothesis --list                      # 列出 10 策略
uv run python -m broker_analytics hypothesis 2330 -s contrarian_broker   # 單一策略
uv run python -m broker_analytics hypothesis 6285 --all                  # 全部 10 策略
uv run python -m broker_analytics hypothesis 2330 -s conviction --params top_k=30
uv run python -m broker_analytics hypothesis --batch 2330,2454 -s exodus --workers 4
```

## Pipeline

```
~/r20/data/fugle/broker_tx/ (9 GB, per-day parquet, managed by ws-admin)
    │ etl.py
    ▼
data/daily_summary/{symbol}.parquet (4.2 GB, 2839 檔)
    │ pnl_engine.py (prices via ws-core → ~/r20/data/tej/prices.parquet)
    ▼
data/pnl_daily/{symbol}.parquet (12 GB, 2839 檔)  ← Layer 1.5：每日明細
data/fifo_state/{symbol}.parquet (289 MB, 2839 檔) ← FIFO checkpoint
    │ aggregate
    ▼
data/pnl/{symbol}.parquet (79 MB, 2839 檔)  ← 個股維度
data/derived/broker_ranking.parquet (56 KB)  ← 券商維度

    │ pnl_engine.py --merged (broker code remap → re-FIFO)
    ▼
data/pnl_daily_merged/  ← 合併版 Layer 1.5
data/pnl_merged/         ← 合併版個股維度
data/derived/broker_ranking_merged.parquet  ← 合併版券商維度
```

### Layer 0：供應商原始資料

`~/r20/data/fugle/broker_tx/` — per-day parquet 檔案（由 ws-admin 每日自動拉取），2021-01 ~ 2026-03。

### Layer 1：daily_summary/{symbol}.parquet

`etl.py` 將原始資料 streaming 聚合為每日摘要，按股票分檔。

| 欄位 | 型態 | 說明 |
|------|------|------|
| `broker` | Categorical | 券商代碼 |
| `date` | Date | 交易日期 |
| `buy_shares` | Int32 | 當日買入股數 |
| `sell_shares` | Int32 | 當日賣出股數 |
| `buy_amount` | Float32 | 當日買入金額 |
| `sell_amount` | Float32 | 當日賣出金額 |

排序：`broker, date`（FIFO 計算最佳化）

### Layer 1.5：pnl_daily/{symbol}.parquet + fifo_state/{symbol}.parquet

`pnl_engine.py` FIFO 逐日計算的中間結果，按股票分檔。支援滾動窗口 PNL 查詢和增量更新。

**pnl_daily/{symbol}.parquet** — 每日 PNL 事件

| 欄位 | 型態 | 說明 |
|------|------|------|
| `broker` | Utf8 | 券商代碼 |
| `date` | Date | 交易日 |
| `realized_pnl` | Float64 | 當日已實現損益 |
| `unrealized_pnl` | Float64 | 當日未實現損益（EOD mark-to-market） |

排序：`broker, date`。只存有交易或有持倉的 (broker, date)。所有日期（含 backtest_start 前）均保存，供滾動窗口回溯。

**fifo_state/{symbol}.parquet** — FIFO 持倉 checkpoint

| 欄位 | 型態 | 說明 |
|------|------|------|
| `broker` | Utf8 | 券商代碼 |
| `side` | Utf8 | "long" / "short" |
| `shares` | Int64 | 股數 |
| `cost_per_share` | Float64 | 成本價 |
| `open_date` | Date | 建倉日 |

Layer 2（pnl/ + broker_ranking）從 Layer 1.5 聚合導出。

### Layer 3a：pnl/{symbol}.parquet

從 Layer 1.5 聚合，輸出**個股維度**的排名。

| 欄位 | 型態 | 說明 |
|------|------|------|
| `rank` | UInt32 | 該股票內的 PNL 排名 |
| `broker` | String | 券商代碼 |
| `total_pnl` | Float64 | 該券商在該股票的總損益 |
| `realized_pnl` | Float64 | 已實現損益 |
| `unrealized_pnl` | Float64 | 未實現損益 |
| `timing_alpha` | Float64 | 該股票的擇時能力 |

排序：`total_pnl DESC`

### Layer 3b：derived/broker_ranking.parquet

同一次 `pnl_engine.py` 執行，將所有個股結果聚合為**券商維度**的全市場排名。

| 欄位 | 型態 | 說明 |
|------|------|------|
| `rank` | UInt32 | 全市場 PNL 排名 |
| `broker` | String | 券商代碼 |
| `total_pnl` | Float64 | 總損益（已實現 + 未實現） |
| `realized_pnl` | Float64 | 已實現損益 |
| `unrealized_pnl` | Float64 | 最終未實現損益 |
| `total_buy_amount` | Float64 | 總買入金額 |
| `total_sell_amount` | Float64 | 總賣出金額 |
| `total_amount` | Float64 | 總成交金額 |
| `timing_alpha` | Float64 | 擇時能力 |

## 指標定義

### PNL（FIFO）

每個 **(股票, 券商)** 視為獨立帳戶，使用 FIFO 追蹤持倉：

- **賣出**：先平多倉（賣最早買的 lot），剩餘開空
- **買入**：先平空倉（回補最早的空單），剩餘開多
- **已實現**：平倉時鎖定的損益
- **未實現**：期末持倉以收盤價估值
- **總損益** = 已實現 + 未實現

邊界情況：無庫存先賣 → 開空倉；多翻空 → 先平多再開空；空翻多 → 先平空再開多。

### 回測窗口

- FIFO 從 **2021-01-01** 開始累積持倉歷史
- PNL 與 Timing Alpha 僅從 **2023-01-01** 起算
- 前兩年只建倉不計分，避免冷啟動偏差

### Timing Alpha

衡量擇時能力：前一天買超多的券商，隔天股價是否漲？

```
timing_alpha = Σ((net_buy[t-1] - avg_net_buy) × return[t]) / std(net_buy)
```

- 除以 `std(net_buy)` 正規化，消除交易量偏差（大量交易不會自動高分）
- **正值**：買超後漲、賣超後跌（擇時正確）
- **負值**：買超後跌、賣超後漲（擇時錯誤）
- 減去平均值排除方向偏好干擾（永遠買超不會自動高分）

存在於兩個維度：個股層級（`pnl/{symbol}.parquet`）和全市場層級（`broker_ranking.parquet`）。

### Smart Money Signal

給定一支股票，看「在這支股票上歷史績效最好的券商」現在站在買方還是賣方。

**計算方式**：

1. 取指定窗口（1/5/10/20/60 交易日）內各券商的淨買超
2. 淨買超 TOP 15 → 查他們在**該股**的 PNL 排名 → 加總 = 買方力道
3. 淨賣超 TOP 15 → 同理 = 賣方力道

**解讀**：

| 力道值 | 意義 |
|--------|------|
| 120（理論最小） | TOP 15 全是該股排名 1~15 的高手 |
| ~4,000 | 混合 |
| 13,650（理論最大） | TOP 15 全是排名最差的 |

買方力道遠低於賣方 → 在這支股票上賺過錢的人正在買入。

注意：使用的是**個股 PNL 排名**（`pnl/{symbol}.parquet`），不是全市場排名。在台積電排名第 1 的券商和在智邦排名第 1 的券商是不同的。

### Signal Report（大單信號分析）

`broker_analytics signal` 對個股執行 4 步分析，輸出 `data/reports/{symbol}.md` + `.json`：

1. **大單偵測**：每個券商的 `|net_buy - mean| > 2σ` 為大單日
2. **統計驗證**：大單日 vs 非大單日的 return spread + t-test（< 5% 顯著 → early exit）
3. **TA 加權信號**：`signal[t] = Σ(TA_b × dev_b[t] / σ_b)`，TA 僅用 train period 計算（test |t| < 2 → early exit）
4. **回測**：open→close return，扣除 0.435% 交易成本，計算 Sharpe / MaxDD / Calmar

預設 train 2023-01~2024-06，test 2024-07~2025-12。開盤價從 ws-core 讀取。

### Market Scan（全市場信號掃描）

`broker_analytics scan` 對全市場 ~2,400 支股票執行 6 層篩選 + 回測，使用 BH-FDR 控制多重檢定：

| 階段 | 說明 |
|------|------|
| F0a | 排除 ETF/ETN（代碼以 "00" 開頭） |
| F0b | 排除股票拆分/減資 |
| F0c | 排除資料不足（train < 30 天、test < 250 天） |
| F1 | 日均成交額 > 門檻（預設 2億 NTD，train period） |
| F2 | 顯著正向券商 > 5% |
| F3 | Benjamini-Hochberg FDR < 1% |

通過 F3 的股票執行完整回測（0.50% 保守成本），輸出 `data/derived/market_scan.json` + `.md`，以及每支個股的 `data/reports/{symbol}.json`。

兩階段平行架構：Phase 1 篩選（12 workers），FDR 校正後 Phase 2 批次拉取 OHLC + 回測。

### Event Study（聰明錢事件研究）

`event-study` 子命令對個股執行事件研究，檢驗 PNL top-K 券商的個別大單（per-broker 2σ）是否預測中期報酬：

1. **事件偵測**：使用 rolling PNL ranking（每天只用過去 3 年的 PNL，避免 2021-2022 持倉建置期噪音），偵測 top-K 券商的個別異常大單（per-broker |net_buy - mean| > 2σ），累積計數超過門檻 → accumulation / distribution 事件
2. **門檻校準**：分析 per-broker z-score 分佈形狀（偏態、峰態），確認 2σ 門檻的實際觸發百分位
3. **方向分拆**：accumulation（大單買超）和 distribution（大單賣超）獨立分析，避免方向對沖稀釋信號
4. **統計檢定**：Permutation test（10,000 次）取代 Bonferroni，搭配 Cohen's d 效果量
5. **SCAR 跨股票池化**：標準化後跨股票合併，解決 per-stock 樣本不足問題
6. **衰減曲線**：逐日 direction-adjusted CAR，判斷 alpha 消化速度與最佳持倉期
7. **穩健性**：Placebo test（隨機券商取代 top-K）

### Hypothesis Testing（可組合假說檢定框架）

`broker_analytics hypothesis` 提供可組合的五步流水線，驗證各種券商行為假說：

```
Selector → Filter → Outcome → Baseline → StatTest
(選券商)   (篩事件)   (量報酬)   (做基準)   (跑統計)
```

每步是純函數，策略只是「五個函數的組合 + 參數」，新增策略零改框架。

**10 策略**：

| # | 策略 | 說明 |
|---|------|------|
| 1 | `contrarian_broker` | 全市場績效差但個股績效好的券商（反差信號） |
| 2 | `dual_window` | 1 年 ∩ 3 年滾動 PNL 均 top-K 的券商 |
| 3 | `conviction` | 績優券商加碼（持倉 > 0 且繼續買入） |
| 4 | `exodus` | 多個績優券商同日淨賣出 |
| 5 | `cross_stock` | A 股績優券商大單 → B 股報酬（跨股資訊流） |
| 6 | `ta_regime` | 擇時能力突變的券商（rolling TA z-score） |
| 7 | `contrarian_smart` | 績優券商在大跌日逆勢買入 |
| 8 | `concentration` | 持倉集中度突增（HHI breakout） |
| 9 | `herding` | 績優 vs 績差券商方向一致時的信號 |

統計檢定：parametric（Welch t-test + Bonferroni）或 permutation（10,000 次），均要求 `p < 0.05` AND `|Cohen's d| >= 0.2`。結論：2+ 個 horizon 顯著 = significant，1 個 = marginal，0 = no_effect。

## 效能

M3 Pro 12 核，全市場 2,839 股 × 917 券商 × 1,209 交易日：

| 階段 | 時間 | 記憶體 | Big-O |
|------|------|--------|-------|
| ETL | ~10 min | ~2 GB (streaming) | O(N)，N=20.8億 |
| PNL 計算 + Layer 1.5 | ~6 min | ~10 MB/核 | O(S×B×T)，12核並行 |
| 查詢 | 0.01s | 1 MB | O(B log B)，預聚合表 |
| 全市場掃描 | ~7 min | ~100 MB | O(S×B×D)，12核並行 |

全程記憶體峰值 ~2 GB，遠低於 36 GB 可用記憶體。

## 設計決策

### FIFO vs 加權平均成本

| 方法 | 做法 | 結果 |
|------|------|------|
| 加權平均 | 所有股票成本相同 | 已實現損益較平滑 |
| **FIFO** | 先買先賣 | 符合實際交易邏輯 |

範例（買 100@$10，再買 100@$20，賣 100@$25）：
- 加權平均：100 × ($25 - $15) = **$1,000**
- FIFO：100 × ($25 - $10) = **$1,500**（賣最早買的）

產品需求選擇 FIFO。

### By Symbol Parquet vs Dense Tensor

| 方案 | 存儲 | 記憶體 | 查詢 |
|------|------|--------|------|
| Dense Tensor | 26 GB | 26 GB (OOM) | O(S×T + B log B) |
| **By Symbol + 預聚合** | ~4.2 GB | ~5 MB | **O(B log B)** |

選擇 By Symbol：Tensor 放不進記憶體，且預聚合後查詢更快、支援增量更新。

### Pre-partition 並行策略

`price_lookup` 有 260 萬筆，直接傳給 12 個 worker 需 pickle 序列化 12 次，IPC 開銷遠大於計算。解法：按 symbol 預分割，每個 worker 只收 ~1,200 筆。

## 外部依賴

### ws-core

價格資料統一透過 `ws-core` 讀取 `~/r20/data/tej/prices.parquet`（由 ws-admin 每日自動更新）。
