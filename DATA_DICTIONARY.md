---
title: "Data Dictionary"
subtitle: "供應商 Parquet 資料格式文檔"
date: "2026-02-13"
format:
  html:
    toc: true
    toc-depth: 3
    toc-location: left
    theme: cosmo
    code-copy: true
    code-overflow: wrap
    highlight-style: github
    embed-resources: true
    self-contained: true
---

協助開發者快速了解各資料表的結構與使用方式。

**資料時間範圍**：2021-01 ~ 2025-12（5 年）

---

## 目錄

1. [broker_tx.parquet — 分點交易明細](#1-broker_txparquet--分點交易明細)
2. [director_holding.parquet — 董監事持股](#2-director_holdingparquet--董監事持股)
3. [stock_transfer.parquet — 內部人轉讓申報](#3-stock_transferparquet--內部人轉讓申報)
4. [stock_transfer_suspend.parquet — 內部人轉讓暫緩](#4-stock_transfer_suspendparquet--內部人轉讓暫緩)
5. [tdcc_distribution.parquet — 集保股權分散表](#5-tdcc_distributionparquet--集保股權分散表)

---

## 1. broker_tx.parquet — 分點交易明細

> 記錄每支股票、每個券商、每個價位的買賣張數（分價量資料）

### 資料規格

| 項目 | 值 |
|------|-----|
| 檔案大小 | 10 GB |
| 總行數 | 20.8 億 |
| 股票數 | 2,839 |
| 券商數 | 943 |
| 時間範圍 | 2021-01-04 ~ 2025-12-31（1,214 天）|
| 更新頻率 | 每日 |
| 原始來源 | 券商分點進出明細 |

### Schema

| 欄位 | 型態 | 範例 | 說明 |
|------|------|------|------|
| `symbol_id` | String | `"2330"` | 股票代碼 |
| `date` | Datetime[ms, UTC] | `2024-01-15T00:00:00Z` | 交易日期 |
| `broker` | String | `"1440"` | 券商代碼 |
| `broker_name` | String | `"美林"` | 券商名稱 |
| `price` | String | `"580.00"` | 成交價位 |
| `buy` | Int64 | `150` | 買入張數 |
| `sell` | Int64 | `80` | 賣出張數 |

### 特殊處理

#### 自營商價位為 "-"

自營商（broker 代碼結尾為 `T`）不揭露分價量，僅提供總量：

| 項目 | 值 |
|------|-----|
| 影響行數 | 935,056 筆（0.04%）|
| 識別方式 | `price == "-"` |

```python
# ETL 處理建議
df = df.with_columns([
    pl.when(pl.col("price") == "-")
      .then(None)
      .otherwise(pl.col("price").str.replace_all(",", "").cast(pl.Float32))
      .alias("price")
])
```

#### 大檔案讀取

```python
# ⚠️ 10GB 檔案必須使用 streaming，否則會 OOM
import polars as pl

lf = pl.scan_parquet("broker_tx.parquet")

# 範例：查詢單一股票的每日券商彙總
daily = (
    lf.filter(pl.col("symbol_id") == "2330")
      .group_by(["date", "broker"])
      .agg([
          pl.col("buy").sum().alias("buy_shares"),
          pl.col("sell").sum().alias("sell_shares"),
      ])
      .collect(engine="streaming")
)
```

---

## 2. director_holding.parquet — 董監事持股

> 追蹤內部人每月持股及質押變化

### 資料規格

| 項目 | 值 |
|------|-----|
| 總行數 | 2,829,062 |
| 股票數 | 2,389 |
| 時間範圍 | 2021-01 ~ 2025-12（60 個月）|
| 更新頻率 | 每月 |
| 原始來源 | [公開資訊觀測站](https://mopsov.twse.com.tw/mops/web/stapap1) |

### Schema

| 欄位 | 型態 | 範例 | 說明 |
|------|------|------|------|
| `symbolId` | String | `"2330"` | 股票代碼 |
| `date` | Datetime | `2024-01-01` | 申報年月（月初） |
| `title` | String | `"董事長"` | 職稱 |
| `name` | String | `"魏哲家"` | 姓名 |
| `holdingShares` | Int64 | `5000000` | 持有股數 |
| `holdingRatio` | Float64 | `0.19` | 持股比例 (%) |
| `pledgedShares` | Int64 | `0` | 質押股數 |
| `pledgedRatio` | Float64 | `0.0` | 質押比例 (%) |

### 使用範例

```python
import polars as pl

df = pl.read_parquet("director_holding.parquet")

# 查詢特定公司董監事持股變化
holding = (
    df.filter(pl.col("symbolId") == "2330")
      .group_by("date")
      .agg(pl.col("holdingShares").sum().alias("total_insider_shares"))
      .sort("date")
)
```

---

## 3. stock_transfer.parquet — 內部人轉讓申報

> 內部人預告賣股（證交法要求事前申報）

### 資料規格

| 項目 | 值 |
|------|-----|
| 總行數 | 19,080 |
| 股票數 | 1,768 |
| 時間範圍 | 2021-01 ~ 2025-12（1,227 天）|
| 更新頻率 | 每日 |
| 原始來源 | [公開資訊觀測站](https://mopsov.twse.com.tw/mops/web/t56sb21_q3) |

### Schema

| 欄位 | 型態 | 範例 | 說明 | Null 說明 |
|------|------|------|------|-----------|
| `symbolId` | String | `"2330"` | 股票代碼 | |
| `date` | Datetime | `2024-03-15` | 申報日期 | |
| `declarantTitle` | String | `"董事本人"` | 申報人身份 | |
| `declarantName` | String | `"王大明"` | 申報人姓名 | |
| `transferType` | String | `"一般交易"` | 轉讓方式 | |
| `declareVolume` | Int64 | `1000` | 申報張數 | 贈與/信託為 null |
| `maxAllowedVolume` | Int64 | `500` | 每日可轉讓上限 | 贈與/信託為 null |
| `transferStart` | Date | `2024-03-20` | 轉讓起日 | |
| `transferEnd` | Date | `2024-04-19` | 轉讓迄日 | |
| `notCompleted` | Int64 | — | 未轉讓張數 | 全為 null |

### 特殊處理

#### Null 值說明

| 欄位 | Null 情境 | 原因 |
|------|-----------|------|
| `declareVolume` | 贈與、信託、洽特定人 | 非公開市場交易，無每日限制 |
| `maxAllowedVolume` | 同上 | 同上 |
| `notCompleted` | 全部 | 供應商未提供此欄位 |

```python
# 判斷轉讓是否已過期（替代 notCompleted）
from datetime import date

df = df.with_columns([
    (pl.col("transferEnd") < date.today()).alias("is_expired")
])
```

### 使用範例

```python
import polars as pl

df = pl.read_parquet("stock_transfer.parquet")

# 查詢近期大額轉讓申報（公開市場交易）
large_transfers = (
    df.filter(
        (pl.col("declareVolume") > 1000) &
        (pl.col("transferType").str.contains("一般交易|盤後定價"))
    )
    .sort("date", descending=True)
    .head(20)
)
```

---

## 4. stock_transfer_suspend.parquet — 內部人轉讓暫緩

> 內部人申報後暫停轉讓（通常因股價不理想或個人因素）

### 資料規格

| 項目 | 值 |
|------|-----|
| 總行數 | 1,189 |
| 股票數 | 400 |
| 時間範圍 | 2021-01 ~ 2025-12（666 天）|
| 更新頻率 | 每日 |
| 原始來源 | [公開資訊觀測站](https://mopsov.twse.com.tw/mops/web/t56sb21_q4) |

### Schema

| 欄位 | 型態 | 範例 | 說明 |
|------|------|------|------|
| `symbolId` | String | `"2312"` | 股票代碼 |
| `date` | Datetime | `2024-05-10` | 暫緩申報日 |
| `declarantTitle` | String | `"董事本人"` | 申報人身份 |
| `declarantName` | String | `"黃浩泉"` | 申報人姓名 |
| `suspendVolumeOwned` | Int64 | `5000` | 本人暫緩張數 |
| `suspendVolumeReserved` | Int64 | `0` | 保留張數 |
| `holdVolumeOwned` | Int64 | `50000` | 本人持有張數 |
| `holdVolumeReserved` | Int64 | `0` | 保留持有張數 |
| `declareVolumeOwned` | Int64 | `3000` | 原申報張數 |
| `declareVolumeReserved` | Int64 | `0` | 保留申報張數 |
| `reason` | String | `"股價不理想"` | 暫緩原因 |

### 使用範例

```python
import polars as pl

df = pl.read_parquet("stock_transfer_suspend.parquet")

# 統計暫緩原因分布
reason_stats = (
    df.group_by("reason")
      .len()
      .sort("len", descending=True)
)
```

---

## 5. tdcc_distribution.parquet — 集保股權分散表

> 散戶 vs 大戶持股結構（每週快照）

### 資料規格

| 項目 | 值 |
|------|-----|
| 總行數 | 14,552,833 |
| 股票數 | 4,614 |
| 時間範圍 | 2021-01 ~ 2025-12（257 週）|
| 更新頻率 | 每週六 |
| 原始來源 | [公開資訊觀測站](https://mopsov.twse.com.tw/mops/web/t16sn02) |

### Schema

| 欄位 | 型態 | 範例 | 說明 |
|------|------|------|------|
| `symbolId` | String | `"2330"` | 股票代碼 |
| `date` | Date | `2024-01-12` | 統計週五日期 |
| `range_string` | String | `"1-999"` | 持股級距（股數）|
| `holders` | Int64 | `58432` | 股東人數 |
| `shares` | Int64 | `12345678` | 持有股數 |
| `proportion` | Float64 | `2.34` | 占比 (%) |

### 持股級距對照

| range_string | 說明 |
|--------------|------|
| `1-999` | 零股戶（< 1 張）|
| `1,000-5,000` | 1-5 張 |
| `5,001-10,000` | 5-10 張 |
| ... | ... |
| `400,001-600,000` | 400-600 張 |
| `600,001-800,000` | 600-800 張 |
| `800,001-1,000,000` | 800-1000 張 |
| `1,000,001以上` | 千張大戶 |
| `異動` | **需過濾** |
| `合計` | **需過濾** |

### 特殊處理

#### 過濾特殊行

```python
# ⚠️ 必須過濾，否則 proportion 總和會超過 100%
df = df.filter(~pl.col("range_string").is_in(["異動", "合計"]))
```

#### 春節資料斷層

以下週次無資料，屬正常現象：

| 年份 | 缺失期間 | 天數 |
|------|----------|------|
| 2021 | 02-08 → 02-19 | 11 |
| 2022 | 01-27 → 02-10 | 14 |
| 2023 | 01-18 → 02-03 | 16 |
| 2025 | 01-23 → 02-07 | 15 |

### 使用範例

```python
import polars as pl

df = pl.read_parquet("tdcc_distribution.parquet")

# 過濾特殊行
df = df.filter(~pl.col("range_string").is_in(["異動", "合計"]))

# 計算千張大戶占比變化
big_holders = (
    df.filter(pl.col("range_string") == "1,000,001以上")
      .filter(pl.col("symbolId") == "2330")
      .select(["date", "proportion"])
      .sort("date")
)
```

---

## 附錄

### A. 資料來源對照

| 資料表 | 公開資訊觀測站頁面 | 更新頻率 |
|--------|-------------------|----------|
| 董監事持股 | mopsov.twse.com.tw/mops/web/stapap1 | 月 |
| 持股轉讓申報 | mopsov.twse.com.tw/mops/web/t56sb21_q3 | 日 |
| 持股轉讓暫緩 | mopsov.twse.com.tw/mops/web/t56sb21_q4 | 日 |
| 集保股權分散表 | mopsov.twse.com.tw/mops/web/t16sn02 | 週六 |

### B. 缺失資料

| 資料表 | 狀態 | 備註 |
|--------|------|------|
| 內部人設質解質彙總公告 | ❌ 未提供 | 可考慮從 director_holding.pledgedShares 推算 |

### C. 檔案位置

```
branch_parquet_file/
├── broker_tx.parquet              # 10 GB
├── 董監事持股/
│   └── director_holding.parquet
├── 內部人轉讓與轉讓暫緩/
│   ├── stock_transfer.parquet
│   └── stock_transfer_suspend.parquet
└── 集保股權分散表/
    └── tdcc_distribution.parquet
```

### D. Polars 讀取速查

```python
import polars as pl

# 小檔案（< 1GB）：直接讀取
df = pl.read_parquet("path/to/file.parquet")

# 大檔案（> 1GB）：使用 LazyFrame + streaming
lf = pl.scan_parquet("path/to/large_file.parquet")
result = lf.filter(...).group_by(...).agg(...).collect(engine="streaming")

# 僅讀取特定欄位
df = pl.read_parquet("path/to/file.parquet", columns=["symbolId", "date", "shares"])
```
