# ws-branch

高速 PNL 運算：JSON → Parquet → 3D Tensor

## Pipeline 架構

```
┌─────────────────┐
│  2345.json      │  原始分點交易數據 (164MB, 42萬筆)
│  (券商API)      │
└────────┬────────┘
         │ etl.py (1.7s)
         ▼
┌─────────────────┐
│ daily_trade_    │  每日交易摘要 (1.4MB)
│ summary.parquet │
└────────┬────────┘
         │ sync_prices.py (3.3s, 含 BigQuery)
         ▼
┌─────────────────┐
│ price_master.   │  收盤價快取 (2KB)
│ parquet         │
└────────┬────────┘
         │ pnl_engine.py (1.75s)
         ▼
┌─────────────────┐
│ realized_pnl.npy│  已實現損益 3D 矩陣
│ unrealized_pnl. │  未實現損益 3D 矩陣
│ npy             │  Shape: (1, 729, 940)
│ index_maps.json │
└─────────────────┘
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

### 為什麼用 Dense Tensor 而非 Sparse？

```
資料特性：729天 × 940券商 = 685,260 cells
實際交易：~50,000 筆（93% 是零）
```

| 方案 | 儲存 | Top N 查詢 | 點查詢 |
|------|------|------------|--------|
| **Dense Tensor** | 2.74 MB | O(B) 向量化 | O(1) |
| Sparse Matrix | ~0.3 MB | O(nnz) | O(log n) |

選擇 Dense 因為：
1. Query A/B 需全掃描找 Top N，向量化更快
2. 2.74 MB 完全放進 CPU cache
3. O(1) 點查詢支援未來券商詳細分析

### 為什麼不用 Rust 優化？

目前 Python 已達 1.75s，瓶頸分析：

| 環節 | 底層 | 說明 |
|------|------|------|
| Polars I/O | Rust | 已是原生速度 |
| deque FIFO | C | CPython 原生實作 |
| NumPy 運算 | C | 向量化操作 |

Rust 重寫預估 0.5s，省 1.25s，但增加：
- 雙語言維護成本
- 編譯流程複雜度
- FFI 資料轉換開銷

**結論**：單股場景下 ROI 不划算。

### 多股票規模化策略

未來 1700 支股票時：

| 方案 | 時間 | 說明 |
|------|------|------|
| 單線程 Python | 50 分鐘 | 1700 × 1.75s |
| 單線程 Rust | 14 分鐘 | 1700 × 0.5s |
| **Python 多進程** | **3 分鐘** | 16 核平行處理 |

正確方向是**平行化**，不是單線程優化。

## 資料欄位說明

### daily_trade_summary.parquet

| 欄位 | 型態 | 說明 |
|------|------|------|
| `date` | String | 交易日期 (YYYY-MM-DD)，台灣時區 |
| `symbol_id` | String | 股票代碼 |
| `broker` | String | 券商代碼 |
| `buy_shares` | Int32 | 當日買入股數 |
| `sell_shares` | Int32 | 當日賣出股數 |
| `buy_amount` | Float32 | 當日買入金額 |
| `sell_amount` | Float32 | 當日賣出金額 |

### realized_pnl.npy / unrealized_pnl.npy

- **Shape**: `(n_symbols, n_dates, n_brokers)`
- **Dtype**: `float32`
- **索引對照**: `index_maps.json`

**總損益公式**：
```python
total_pnl = realized[:, :, broker_idx].sum() + unrealized[:, -1, broker_idx]
#           ^^^^^^^^^^^^^^^^^^^^^^^^^^^        ^^^^^^^^^^^^^^^^^^^^^^^^^^^
#           累計已實現（所有日期加總）            最後一天未實現（快照）
```

⚠️ 錯誤做法：`(realized + unrealized).sum()` 會重複計算未實現損益。

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

# 完整 Pipeline
uv run python etl.py 2345.json
uv run python sync_prices.py daily_trade_summary.parquet
uv run python pnl_engine.py daily_trade_summary.parquet price_master.parquet

# 排行榜
uv run python query_ranking.py
```

## 查詢輸出

| Query | 說明 | 排序 |
|-------|------|------|
| A | 一年金額 | buy_amount + sell_amount |
| B | 一年獲利 | realized.sum() + unrealized[-1] |

輸出：`ranking_report.xlsx`

## 效能基準 (2345 單股)

| 階段 | 時間 | 說明 |
|------|------|------|
| ETL | 1.70s | JSON 164MB → Parquet 1.4MB |
| 價格同步 | 3.33s | BigQuery 網路延遲 |
| PNL 計算 | 1.75s | 729天 × 940券商 FIFO |
| **總計** | **~7s** | |

## BigQuery

- **Project**: `gen-lang-client-0998197473`
- **Dataset**: `wsai`
- **Table**: `tej_prices` (分區: year, 聚類: coid)
