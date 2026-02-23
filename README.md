# ws-branch

高速 PNL 運算：分點資料 → By Symbol Parquet → 並行計算

## Pipeline 架構

```
┌───────────────────┐
│ broker_tx.parquet │  供應商分點資料 (10GB, 20.8億筆, 2839股)
└────────┬──────────┘
         │ etl.py (streaming)
         ▼
┌───────────────────┐
│ daily_summary/    │  每日交易摘要 (by symbol)
│   ├── 2330.parquet│  ~4.2 GB total
│   └── ...         │
└────────┬──────────┘
         │ sync_prices.py (BigQuery)
         ▼
┌───────────────────┐
│ price/            │
│ close_prices.parquet  收盤價 (~4 MB)
└────────┬──────────┘
         │ pnl_engine.py (pre-partition + 12核並行, ~5分鐘)
         ▼
┌───────────────────┐
│ pnl/              │  Per-stock PNL (by symbol, ~79 MB)
│   ├── 2330.parquet│  每支股票的券商 PNL 排名
│   └── ...         │
├───────────────────┤
│ derived/          │  預聚合表 (查詢用)
│ broker_ranking.parquet  (~100 KB)
└───────────────────┘
```

## 設計決策

### 為什麼用 FIFO 而非加權平均成本？

| 方法 | 做法 | 結果 |
|------|------|------|
| 加權平均 | 所有股票成本相同 | 已實現損益較平滑 |
| **FIFO** | 先買先賣 | 符合實際交易邏輯 |

範例（買 100@$10，再買 100@$20，賣 100@$25）：
- 加權平均：100 × ($25 - $15) = **$1,000**
- FIFO：100 × ($25 - $10) = **$1,500**（賣最早買的）

產品需求選擇 FIFO。

### 為什麼用 By Symbol Parquet 而非 Tensor？

全市場 2,839 股 × 1,209 天 × 917 券商：

| 方案 | 存儲 | 記憶體 | Query A/B |
|------|------|--------|-----------|
| Dense Tensor | 26 GB | 26 GB (OOM) | O(S×T + B log B) |
| **By Symbol + 預聚合** | ~4.2 GB | ~5 MB | **O(B log B)** |

選擇 By Symbol 因為：
1. Tensor 26 GB 無法放進記憶體
2. 預聚合表只有 100 KB，查詢反而更快
3. 增量更新只需重算單股，不用全部重算

### 多股票規模化策略

2,839 支股票，M3 Pro 12 核：

| 方案 | 時間 | 說明 |
|------|------|------|
| 單線程 Python | ~10 分鐘 | 2839 symbols 逐一處理 |
| **Python 多進程** | **~5 分鐘** | Pre-partition + 12 核並行 |

**為什麼不能直接用 ProcessPoolExecutor？**

`price_lookup` 有 260 萬筆，直接傳給 12 個 worker 需要 pickle 序列化 12 次，
IPC 開銷遠大於計算本身。解法是 **pre-partition**：按 symbol 預分割，
每個 worker 只收到 ~1,200 筆（自己那支股票的價格），序列化成本可忽略。

詳見 `ref.md`。

## 資料欄位說明

### daily_summary/{symbol}.parquet

| 欄位 | 型態 | 說明 |
|------|------|------|
| `broker` | Categorical | 券商代碼 |
| `date` | Date | 交易日期，台灣時區 |
| `buy_shares` | Int32 | 當日買入張數 |
| `sell_shares` | Int32 | 當日賣出張數 |
| `buy_amount` | Float32 | 當日買入金額 |
| `sell_amount` | Float32 | 當日賣出金額 |

排序：`ORDER BY broker, date`（FIFO 計算最佳化）

### pnl/{symbol}.parquet

| 欄位 | 型態 | 說明 |
|------|------|------|
| `rank` | UInt32 | 該股票內的 PNL 排名 |
| `broker` | String | 券商代碼 |
| `total_pnl` | Float64 | 該券商在該股票的總損益 |
| `realized_pnl` | Float64 | 已實現損益 |
| `unrealized_pnl` | Float64 | 未實現損益 |
| `timing_alpha` | Float64 | 該股票的擇時能力 |

排序：`ORDER BY total_pnl DESC`（排名用）

### derived/broker_ranking.parquet

| 欄位 | 型態 | 說明 |
|------|------|------|
| `rank` | UInt32 | 全市場 PNL 排名 |
| `broker` | String | 券商代碼 |
| `total_pnl` | Float64 | 總損益（2023+ 已實現 + 最終未實現）|
| `realized_pnl` | Float64 | 已實現損益 |
| `unrealized_pnl` | Float64 | 最終未實現損益 |
| `total_buy_amount` | Float64 | 總買入金額 |
| `total_sell_amount` | Float64 | 總賣出金額 |
| `total_amount` | Float64 | 總成交金額 |
| `timing_alpha` | Float64 | 擇時能力：Σ((net_buy[t-1] - avg) × return[t]) |

**總損益公式**：
```python
# 預聚合表已計算完成，直接查詢
df = pl.read_parquet("derived/broker_ranking.parquet")
top_10 = df.sort("total_pnl", descending=True).head(10)
```

## PNL 計算邏輯 (FIFO)

每個 **(股票, 券商)** 視為獨立帳戶，使用 `list` 追蹤每筆買入記錄（lot）。

### 賣出時（平多倉）

```python
while remaining_sell > 0 and long_lots:
    lot = long_lots.pop(0)  # 取最早的 lot
    realized += shares × (sell_price - lot.cost)
```

### 買入時（回補空倉）

```python
while remaining_buy > 0 and short_lots:
    lot = short_lots.pop(0)  # 取最早的空單
    realized += shares × (short_price - buy_price)
```

### 邊界情況

| 情況 | 處理 |
|------|------|
| 無庫存先賣 | 開空倉 |
| 多翻空 | 先平多，再開空 |
| 空翻多 | 先平空，再開多 |

## 使用方式

```bash
uv sync

# 完整 Pipeline（全市場）
uv run python etl.py broker_tx.parquet          # → daily_summary/*.parquet
uv run python sync_prices.py                    # → price/close_prices.parquet
uv run python pnl_engine.py                     # → pnl/*.parquet + derived/

# CLI 指令
uv run python -m pnl_analytics ranking          # 顯示排名
uv run python -m pnl_analytics query 1440       # 查詢單一券商
uv run python -m pnl_analytics symbol 2330      # 個股買賣力道（smart money signal）
uv run python -m pnl_analytics symbol 2330 --detail 5  # 近5日明細
uv run python -m pnl_analytics verify           # 資料驗證
```

## 回測窗口

- FIFO 狀態從 **2021-01-01** 開始累積（建立持倉歷史）
- 績效指標（PNL、Timing Alpha）僅從 **2023-01-01** 起算
- 目的：避免「冷啟動」偏差，前兩年只建倉不計分

## Timing Alpha

衡量券商的擇時能力：前一天買超多的，隔天是否漲？

```python
timing_alpha = Σ((net_buy[t-1] - avg_net_buy) × return[t])
```

| 結果 | 解讀 |
|------|------|
| 正值 | 買超後漲、賣超後跌（擇時正確） |
| 負值 | 買超後跌、賣超後漲（擇時錯誤） |

減去平均值是為了排除方向偏好干擾（永遠買超的人不會自動得到高分）。

## Smart Money Signal（symbol 指令）

給定一支股票，看「買它的人」和「賣它的人」在**這支股票上**的歷史績效。

**核心問題**：現在買這支股票的人，過去在這支股票上賺不賺錢？

### 信號計算

1. 取指定窗口（1/5/10/20/60 交易日）內的淨買超/賣超
2. 排序取 TOP 15 淨買超券商 → 查詢他們在**該股**的 PNL 排名 → 加總 = 買方力道
3. 同理取 TOP 15 淨賣超券商 → 賣方力道

### 解讀

| 力道值 | 意義 |
|--------|------|
| 120（理論最小） | 前 15 名都是該股排名 1~15 的高手 |
| ~4,000 | 混合，部分高手部分散戶 |
| 13,650（理論最大） | 前 15 名都是該股排名最差的 |

**買方力道遠低於賣方力道** → 在這支股票上賺過錢的券商正在買入。

## 效能基準 (全市場 2,839 股)

| 階段 | 時間 | 記憶體 | Big-O | 說明 |
|------|------|--------|-------|------|
| ETL | ~10 min | ~2 GB | O(N) | N=20.8億筆，streaming |
| 價格同步 | ~1 min | 200 MB | O(S×D) | 2839股 × 1209天 |
| PNL 計算 | **~5 min** | ~5 MB/核 | O(S×B×T) | 12核並行，輸出 pnl/ + derived/ |
| Query | **0.01s** | 1 MB | O(B log B) | 預聚合表 |

### 複雜度說明

```
ETL:        O(N)           N = 20.8億筆（streaming 處理）
Sync:       O(S × D)       S=2839, D=1209（可快取）
PNL Engine: O(S × B × T)   2839 × 917 × 1209（並行分攤）
Query:      O(B log B)     預聚合表只有 917 行
```

### 記憶體控制

| 階段 | 策略 |
|------|------|
| ETL | `pl.scan_parquet()` + streaming |
| PNL | 每股獨立處理，單核 ~5 MB |
| Query | 預聚合表 100 KB |

全程記憶體峰值 ~2 GB，遠低於 36 GB 可用記憶體。

## BigQuery

- **Project**: `gen-lang-client-0998197473`
- **Dataset**: `wsai`
- **Table**: `tej_prices` (分區: year, 聚類: coid)
