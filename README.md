# ws-branch

全市場券商分點交易資料的 PNL 回測系統。回答兩個問題：

1. **哪些券商在賺錢？** — 依 FIFO 計算每家券商的已實現 + 未實現損益，全市場排名
2. **聰明錢在買什麼？** — 給定一支股票，看「在這支股票上歷史績效最好的券商」現在站哪邊

## 快速開始

```bash
uv sync

# Pipeline（全市場 2,839 股，約 15 分鐘）
uv run python etl.py broker_tx.parquet      # → data/daily_summary/
uv run python sync_prices.py                # → data/price/
uv run python pnl_engine.py                 # → data/pnl/ + data/derived/

# 查詢
uv run python -m pnl_analytics ranking                  # 全市場券商排名
uv run python -m pnl_analytics query 1440               # 單一券商績效
uv run python -m pnl_analytics query 1440 --breakdown   # 含個股明細
uv run python -m pnl_analytics symbol 2330               # 個股 smart money signal
uv run python -m pnl_analytics symbol 2330 --detail 5    # 近 5 日明細
uv run python -m pnl_analytics verify                    # 資料完整性驗證
```

## Pipeline

```
broker_tx.parquet (10GB, 20.8億筆)
    │ etl.py
    ▼
data/daily_summary/{symbol}.parquet (4.2 GB, 2839 檔)
    │ sync_prices.py
    ▼
data/price/close_prices.parquet (4 MB)
    │ pnl_engine.py
    ▼
data/pnl/{symbol}.parquet (79 MB, 2839 檔)  ← 個股維度
data/derived/broker_ranking.parquet (56 KB)  ← 券商維度
```

### Layer 0：供應商原始資料

`broker_tx.parquet` — 供應商提供的分點買賣明細，20.8 億筆，2021-01 ~ 2025-12。

### Layer 1：daily_summary/{symbol}.parquet

`etl.py` 將原始資料 streaming 聚合為每日摘要，按股票分檔。

| 欄位 | 型態 | 說明 |
|------|------|------|
| `broker` | Categorical | 券商代碼 |
| `date` | Date | 交易日期 |
| `buy_shares` | Int32 | 當日買入張數 |
| `sell_shares` | Int32 | 當日賣出張數 |
| `buy_amount` | Float32 | 當日買入金額 |
| `sell_amount` | Float32 | 當日賣出金額 |

排序：`broker, date`（FIFO 計算最佳化）

### Layer 2：price/close_prices.parquet

`sync_prices.py` 從 BigQuery 同步收盤價，用於計算未實現損益和日報酬率。

### Layer 3a：pnl/{symbol}.parquet

`pnl_engine.py` 對每支股票的每家券商做 FIFO 回測，輸出**個股維度**的排名。

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

## 效能

M3 Pro 12 核，全市場 2,839 股 × 917 券商 × 1,209 交易日：

| 階段 | 時間 | 記憶體 | Big-O |
|------|------|--------|-------|
| ETL | ~10 min | ~2 GB (streaming) | O(N)，N=20.8億 |
| 價格同步 | ~1 min | 200 MB | O(S×D) |
| PNL 計算 | ~5 min | ~5 MB/核 | O(S×B×T)，12核並行 |
| 查詢 | 0.01s | 1 MB | O(B log B)，預聚合表 |

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

### BigQuery

- **Project**: `gen-lang-client-0998197473`
- **Dataset**: `wsai`
- **Table**: `tej_prices`（分區: year，聚類: coid）
