# DuckDB CLI 常用指令

> 用 DuckDB 直接查詢 Parquet 檔案，不需要載入 Python。
> 適合快速探索、驗證、除錯。

## 安裝

```bash
brew install duckdb
```

---

## 基本語法

```bash
# 直接執行 SQL（-c 模式）
duckdb -c "SELECT count(*) FROM 'path/to/file.parquet';"

# 互動模式
duckdb
# 進入後輸入 SQL，Ctrl+D 退出

# 讀多個檔案（glob）
duckdb -c "SELECT * FROM 'data/daily_summary/*.parquet' LIMIT 10;"
```

---

## 探索資料結構

```bash
# 看 schema
duckdb -c "DESCRIBE SELECT * FROM 'file.parquet';"

# 看前幾筆
duckdb -c "SELECT * FROM 'file.parquet' LIMIT 5;"

# 看基本統計
duckdb -c "
SELECT
    count(*) as rows,
    count(DISTINCT symbol_id) as symbols,
    min(date) as min_date,
    max(date) as max_date
FROM 'file.parquet';
"
```

---

## 本專案常用查詢

### broker_tx.parquet（原始分點資料）

```bash
FILE='branch_parquet_file/券商分點/broker_tx.parquet'

# 總覽
duckdb -c "
SELECT count(*) as rows,
       count(DISTINCT symbol_id) as symbols,
       count(DISTINCT broker) as brokers
FROM '$FILE';
"

# 查特定股票特定日期的分點
duckdb -c "
SELECT broker, broker_name, price, buy, sell
FROM '$FILE'
WHERE symbol_id = '2330' AND date = '2025-12-31'
ORDER BY (buy + sell) DESC
LIMIT 20;
"

# 查自營商資料（price = '-'）
duckdb -c "
SELECT broker, broker_name, buy, sell, buy - sell as net
FROM '$FILE'
WHERE symbol_id = '2330' AND price = '-'
ORDER BY date DESC
LIMIT 20;
"
```

### daily_summary/（ETL 後的 per-symbol 檔案）

```bash
# 查單一股票
duckdb -c "SELECT * FROM 'data/daily_summary/2330.parquet' LIMIT 10;"

# 跨股票查詢（glob）
duckdb -c "
SELECT filename, count(*) as rows
FROM 'data/daily_summary/*.parquet'
GROUP BY filename
ORDER BY rows DESC
LIMIT 10;
"
```

### broker_ranking.parquet（最終排名）

```bash
# Top 10
duckdb -c "
SELECT rank, broker,
       round(total_pnl / 1e8, 1) as '總損益(億)',
       round(timing_alpha / 1e6, 1) as 'Alpha(百萬)'
FROM 'data/derived/broker_ranking.parquet'
ORDER BY rank
LIMIT 10;
"

# 查特定券商
duckdb -c "
SELECT * FROM 'data/derived/broker_ranking.parquet'
WHERE broker = '1440';
"
```

---

## 分析技巧

### 一般分點 vs 自營商比較

自營商的特徵：broker 代碼尾綴 `T`、名稱含「自營」、price = `-`。

```bash
duckdb -c "
WITH tagged AS (
    SELECT *,
        CASE WHEN price = '-' THEN '自營商' ELSE '一般分點' END as type
    FROM 'branch_parquet_file/券商分點/broker_tx.parquet'
    WHERE symbol_id = '2330' AND date = '2025-06-30'
)
SELECT type,
       count(*) as 筆數,
       sum(buy) as 總買,
       sum(sell) as 總賣,
       sum(buy) - sum(sell) as 淨買超
FROM tagged
GROUP BY type;
"
```

**已驗證的結論**：自營商的買賣量 = 一般分點的賣買量（完全鏡像）。
自營商是一般分點交易的對手方，price="-" 是為了避免重複計算金額。
ETL 跳過 price="-" 是正確的。

驗證方式（隨機抽樣）：

```bash
duckdb -c "
WITH proprietary_days AS (
    SELECT DISTINCT symbol_id, date
    FROM 'branch_parquet_file/券商分點/broker_tx.parquet'
    WHERE price = '-'
),
sampled AS (
    SELECT * FROM proprietary_days
    ORDER BY hash(symbol_id || date::varchar)
    LIMIT 10
),
tagged AS (
    SELECT t.*,
        CASE WHEN t.price = '-' THEN '自營商' ELSE '一般分點' END as type
    FROM 'branch_parquet_file/券商分點/broker_tx.parquet' t
    INNER JOIN sampled s ON t.symbol_id = s.symbol_id AND t.date = s.date
)
SELECT symbol_id, date::date as date, type,
       sum(buy) as 總買, sum(sell) as 總賣,
       sum(buy) - sum(sell) as 淨買超
FROM tagged
GROUP BY symbol_id, date, type
ORDER BY symbol_id, date, type;
"
```

### 匯出 CSV

```bash
duckdb -c "
COPY (
    SELECT * FROM 'data/derived/broker_ranking.parquet'
    ORDER BY rank
) TO 'ranking.csv' (HEADER, DELIMITER ',');
"
```

### 匯出 Excel（需要 spatial extension）

```bash
duckdb -c "
INSTALL spatial; LOAD spatial;
COPY (
    SELECT * FROM 'data/derived/broker_ranking.parquet'
    ORDER BY rank
) TO 'ranking.xlsx' WITH (FORMAT GDAL, DRIVER 'xlsx');
"
```

---

## 效能提示

| 技巧 | 說明 |
|------|------|
| 直接查 parquet | DuckDB 自動做 predicate pushdown，不需要先載入 |
| `LIMIT` | 探索時永遠加 LIMIT，避免輸出 20 億行 |
| `DESCRIBE` | 看 schema 比 SELECT * 快很多 |
| `count(DISTINCT col)` | 比 `SELECT DISTINCT` 再 count 快 |
| glob `*.parquet` | 一次查詢所有 per-symbol 檔案 |
| `EXPLAIN` | 看查詢計畫，確認 filter 是否被 pushdown |
