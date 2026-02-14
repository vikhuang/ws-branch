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
│   ├── 2330.parquet│  ~8 GB total
│   └── ...         │
└────────┬──────────┘
         │ sync_prices.py (BigQuery)
         ▼
┌───────────────────┐
│ price/            │
│ close_prices.parquet  收盤價 (~50 MB)
└────────┬──────────┘
         │ pnl_engine.py (12核並行, ~5分鐘)
         ▼
┌───────────────────┐
│ pnl/              │  PNL 結果 (by symbol)
│   ├── 2330.parquet│
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

全市場 2,839 股 × 1,214 天 × 943 券商：

| 方案 | 存儲 | 記憶體 | Query A/B |
|------|------|--------|-----------|
| Dense Tensor | 26 GB | 26 GB (OOM) | O(S×T + B log B) |
| **By Symbol + 預聚合** | ~8 GB | ~5 MB | **O(B log B)** |

選擇 By Symbol 因為：
1. Tensor 26 GB 無法放進記憶體
2. 預聚合表只有 100 KB，查詢反而更快
3. 增量更新只需重算單股，不用全部重算

### 多股票規模化策略

2,839 支股票，M3 Pro 12 核：

| 方案 | 時間 | 說明 |
|------|------|------|
| 單線程 Python | 83 分鐘 | 2839 × 1.75s |
| **Python 多進程** | **~5 分鐘** | 12 核並行處理 |

正確方向是**並行化 + By Symbol 分檔**。

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

### derived/broker_ranking.parquet

| 欄位 | 型態 | 說明 |
|------|------|------|
| `broker` | Categorical | 券商代碼 |
| `total_pnl` | Float64 | 總損益（2023+ 已實現 + 最終未實現）|
| `total_buy_amount` | Float64 | 總買入金額 |
| `total_sell_amount` | Float64 | 總賣出金額 |
| `win_count` | Int32 | 獲利平倉次數 |
| `loss_count` | Int32 | 虧損平倉次數 |

**總損益公式**：
```python
# 預聚合表已計算完成，直接查詢
df = pl.read_parquet("derived/broker_ranking.parquet")
top_10 = df.sort("total_pnl", descending=True).head(10)
```

## PNL 計算邏輯 (FIFO)

每個 **(股票, 券商)** 視為獨立帳戶，使用 `deque` 追蹤每筆買入記錄（lot）。

### 賣出時（平多倉）

```python
while remaining_sell > 0 and long_lots:
    lot = long_lots.popleft()  # 取最早的 lot
    realized += shares × (sell_price - lot.cost)
```

### 買入時（回補空倉）

```python
while remaining_buy > 0 and short_lots:
    lot = short_lots.popleft()  # 取最早的空單
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
uv run python -m pnl_analytics verify           # 資料驗證
```

## 查詢輸出

| Query | 說明 | 排序 |
|-------|------|------|
| A | 一年金額 | buy_amount + sell_amount |
| B | 一年獲利 | realized.sum() + unrealized[-1] |

輸出：`ranking_report.xlsx`

## 效能基準 (全市場 2,839 股)

| 階段 | 時間 | 記憶體 | Big-O | 說明 |
|------|------|--------|-------|------|
| ETL | ~10 min | ~2 GB | O(N) | N=20.8億筆，streaming |
| 價格同步 | ~1 min | 200 MB | O(S×D) | 2839股 × 1214天 |
| PNL 計算 | **~5 min** | ~5 MB/核 | O(S×B×T) | 12核並行 |
| Query A/B | **0.01s** | 1 MB | O(B log B) | 預聚合表 |

### 複雜度說明

```
ETL:        O(N)           N = 20.8億筆（streaming 處理）
Sync:       O(S × D)       S=2839, D=1214（可快取）
PNL Engine: O(S × B × T)   2839 × 943 × 1214（並行分攤）
Query:      O(B log B)     預聚合表只有 943 行
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
