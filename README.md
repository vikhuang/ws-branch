# ws-branch

高速 PNL 運算：JSON → Parquet → 3D Tensor

## Pipeline 架構

```
┌─────────────────┐
│  2345.json      │  原始分點交易數據 (74MB)
│  (券商API)      │
└────────┬────────┘
         │ etl.py (Module A)
         ▼
┌─────────────────┐
│ daily_trade_    │  每日交易摘要 (1.4MB)
│ summary.parquet │  欄位: date, symbol_id, broker,
└────────┬────────┘        total_buy_amount, total_sell_amount, net_shares
         │
         │ sync_prices.py (Module B)
         ▼
┌─────────────────┐
│ price_master.   │  收盤價快取 (2KB)
│ parquet         │  欄位: coid, date, close_price
└────────┬────────┘  來源: BigQuery tej_prices
         │
         │ pnl_engine.py (Module C+D)
         ▼
┌─────────────────┐
│ realized_pnl.npy│  已實現損益 3D 矩陣
│ unrealized_pnl. │  未實現損益 3D 矩陣
│ npy             │  Shape: (symbols, dates, brokers)
│ index_maps.json │
└─────────────────┘
```

## 資料欄位說明

### daily_trade_summary.parquet

| 欄位 | 型態 | 說明 |
|------|------|------|
| `date` | String | 交易日期 (YYYY-MM-DD)，已轉換為台灣時區 |
| `symbol_id` | String | 股票代碼 |
| `broker` | String | 券商代碼 |
| `buy_shares` | Int32 | 當日買入總股數 |
| `sell_shares` | Int32 | 當日賣出總股數 |
| `buy_amount` | Float32 | 當日買入總金額 |
| `sell_amount` | Float32 | 當日賣出總金額 |

### price_master.parquet

| 欄位 | 型態 | 說明 |
|------|------|------|
| `coid` | String | 股票代碼 |
| `date` | String | 交易日期 |
| `close_price` | Float32 | 收盤價 |

### realized_pnl.npy / unrealized_pnl.npy

| 檔案 | 內容 |
|------|------|
| `realized_pnl.npy` | 每日已實現損益（平倉鎖定） |
| `unrealized_pnl.npy` | 每日未實現損益（帳面損益） |

- **Shape**: `(n_symbols, n_dates, n_brokers)`
- **Dtype**: `float32`
- **索引對照**: `index_maps.json` 內含 `dates`, `symbols`, `brokers` 映射表
- **總損益**: `realized + unrealized`

## PNL 計算公式

### 核心概念

每個 **(股票, 券商)** 組合視為一個獨立帳戶，追蹤其持倉成本與損益。

### 名詞定義

| 名詞 | 定義 |
|------|------|
| **持倉 (position)** | 累計淨股數。正數=做多，負數=做空 |
| **成本基礎 (cost basis)** | 持倉的加權平均成本價 |
| **已實現損益 (realized PNL)** | 平倉時鎖定的損益，不再隨價格變動 |
| **未實現損益 (unrealized PNL)** | 未平倉持股的帳面損益，隨收盤價變動 |

### 計算邏輯

#### 1. 做多情境 (position > 0)

```
avg_cost = 累計買入總額 / 累計買入股數

已實現 = 賣出金額 - (賣出股數 × avg_cost)
未實現 = 持倉股數 × (收盤價 - avg_cost)
```

**範例**：
- 買 100 股 @ $10 → avg_cost = $10
- 賣 60 股 @ $12 → 已實現 = 60×($12-$10) = $120
- 剩 40 股，收盤 $11 → 未實現 = 40×($11-$10) = $40

#### 2. 做空情境 (position < 0)

```
avg_short_price = 累計賣出總額 / 累計賣出股數

已實現 = (買回股數 × avg_short_price) - 買回金額
未實現 = |持倉| × (avg_short_price - 收盤價)
```

**範例**：
- 先賣 100 股 @ $12（放空）→ avg_short_price = $12
- 買回 60 股 @ $10 → 已實現 = 60×($12-$10) = $120
- 剩空 40 股，收盤 $11 → 未實現 = 40×($12-$11) = $40

#### 3. 邊界情況

| 情況 | 處理方式 |
|------|----------|
| 第一天就賣出（無庫存） | 視為放空，avg_short_price = 當日賣出均價 |
| 從多翻空 | 先平掉多倉（已實現），再建立空倉 |
| 從空翻多 | 先平掉空倉（已實現），再建立多倉 |
| 當日沖銷 | 全部計入已實現 |

### 總損益

```
總 PNL = 已實現 PNL + 未實現 PNL
```

## 使用方式

```bash
uv sync

# Step 1: JSON → Parquet (Module A)
uv run python etl.py 2345.json

# Step 2: 同步收盤價 (Module B, 需要 BigQuery 權限)
uv run python sync_prices.py daily_trade_summary.parquet

# Step 3: 計算 PNL Tensor (Module C+D)
uv run python pnl_engine.py daily_trade_summary.parquet price_master.parquet
```

## 範例查詢

```python
import json
import numpy as np

realized = np.load('realized_pnl.npy')
unrealized = np.load('unrealized_pnl.npy')
with open('index_maps.json') as f:
    maps = json.load(f)

# O(1) 查詢特定券商的 PNL
broker_idx = maps['brokers']['1021']
sym_idx = maps['symbols']['2345']

total_pnl = realized[sym_idx, :, broker_idx] + unrealized[sym_idx, :, broker_idx]

# 計算勝率 (以總損益計)
wins = np.sum(total_pnl > 0)
total = np.sum(total_pnl != 0)
print(f'勝率: {wins/total*100:.1f}%')

# 累計已實現損益
cum_realized = realized[sym_idx, :, broker_idx].sum()
print(f'累計已實現: {cum_realized:,.0f}')
```

## 特定 Query 需求

> 此區塊記錄特定的查詢需求，供後續開發參考。

| 需求 | 說明 | 狀態 |
|------|------|------|
| (待討論) | | |

## Docker

```bash
docker build -t ws-branch .
docker run -v $(pwd):/app ws-branch etl.py 2345.json
```

## BigQuery 資訊

- **Project ID**: `gen-lang-client-0998197473`
- **Dataset**: `wsai`
- **Price Table**: `tej_prices` (分區: year, 聚類: coid)
