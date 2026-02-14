# 資料 Pipeline 效能優化參考筆記

> 這份文件記錄本專案 pipeline 在效能優化過程中遇到的三類問題與解法。
> 適合作為 Python 資料工程的入門案例學習。

---

## 目錄

### A. PNL Engine 平行化（IPC 瓶頸）
1. [問題定義](#1-問題定義)
2. [為什麼 Python 需要特別處理平行化](#2-為什麼-python-需要特別處理平行化)
3. [本專案的具體瓶頸](#3-本專案的具體瓶頸)
4. [採用的解法：Pre-partition](#4-採用的解法pre-partition)
5. [不採用的解法與原因](#5-不採用的解法與原因)

### B. ETL 重複掃描
6. [重複掃描問題](#6-重複掃描問題)
7. [採用的解法：批次掃描](#7-採用的解法批次掃描)

### C. Pipeline 更新策略
8. [增量處理 vs 全量重跑](#8-增量處理-vs-全量重跑)

### 附錄
9. [關鍵字與學習路線](#9-關鍵字與學習路線)

---

## 1. 問題定義

### 現象

`pnl_engine.py` 處理 2,839 個 symbol 的 FIFO 損益計算，耗時約 10 分鐘。
每個 symbol 的計算彼此獨立（embarrassingly parallel），理論上可以用 12 核心加速到 ~1 分鐘，
但實際上程式碼一直是單線程 for 迴圈。

### 根本原因

兩個共用的查找表太大，無法有效傳遞給子進程：

| 資料結構 | 內容 | 筆數 | 估計大小 |
|----------|------|------|----------|
| `price_lookup` | `{(symbol, date): close_price}` | ~3,400,000 | ~200 MB |
| `returns_lookup` | `{(symbol, date): daily_return}` | ~3,400,000 | ~200 MB |

Python 的 `ProcessPoolExecutor` 透過 **pickle 序列化**將參數傳給子進程。
如果每個 worker 都收到完整的 400 MB 資料，12 個 worker 就要序列化 12 次、反序列化 12 次，
光是資料搬運的時間就超過了計算本身。

### 用一句話總結

> 每個 worker 只需要 0.035% 的資料（1,200 / 3,400,000），卻被迫接收 100%。

---

## 2. 為什麼 Python 需要特別處理平行化

### GIL（Global Interpreter Lock）

Python 有一個全域鎖叫 GIL，同一時間只允許一個線程執行 Python bytecode。

這代表：

| 任務類型 | threading（線程） | multiprocessing（進程） |
|----------|-------------------|------------------------|
| **I/O-bound**（等網路、讀檔） | 有效 | 有效但 overkill |
| **CPU-bound**（數學計算、迴圈） | 無效（被 GIL 卡住） | **必須用這個** |

FIFO 計算是 CPU-bound → 必須用 multiprocessing。

### multiprocessing 的代價

進程之間**不共享記憶體**（和線程不同）。傳遞資料的方式：

```
主進程                              子進程
┌──────────┐     pickle.dumps()    ┌──────────┐
│ Python   │ ──── 序列化為 bytes ──►│ 從 bytes │
│ 物件     │     透過 pipe/socket   │ 重建物件  │
└──────────┘                       └──────────┘
```

這就是為什麼傳遞大型 dict 是致命的——它不是「共享」，而是「複製」。

### 關鍵概念

- **序列化成本與物件大小成正比**：3.4M 個 tuple key 的 dict，pickle 很慢
- **反序列化在每個 worker 都要做一次**：12 workers = 12 次反序列化
- **計算與通訊比（compute-to-communication ratio）**：如果搬運資料的時間 > 計算時間，平行化反而更慢

---

## 3. 本專案的具體瓶頸

### 資料流分析

```
price_lookup: {(symbol, date): price}
                 ↑
                 │
          這個 key 結構是問題所在
          symbol 有 2,839 種
          date 有 ~1,200 天
          總共 ~3.4M 筆

          但 process_symbol("2330") 只需要 symbol="2330" 的 ~1,200 筆
```

### 量化瓶頸

假設用 12 workers 直接平行化（不做任何優化）：

```
序列化 price_lookup:    ~2 秒 × 1 次 = 2 秒
反序列化 price_lookup:  ~2 秒 × 12 workers = 24 秒
序列化 returns_lookup:  ~2 秒 × 1 次 = 2 秒
反序列化 returns_lookup: ~2 秒 × 12 workers = 24 秒
                                        ─────
                        IPC 開銷合計:     ~52 秒

但每個 symbol 的計算時間: ~3.5 ms
2839 symbols ÷ 12 cores = ~237 symbols/core
237 × 3.5 ms = ~0.8 秒/core

結論: IPC 開銷 52 秒 >> 實際計算 0.8 秒
      平行化不是免費的！
```

---

## 4. 採用的解法：Pre-partition

### 核心思路

在主進程中，先按 symbol 把大 dict 拆成小 dict，再只傳每個 symbol 需要的部分。

```
Before (傳整包):
  main ──── 3,400,000 entries ────► worker    ✗ 慢

After (預分割):
  main ──── 1,200 entries ─────────► worker    ✓ 快
```

### 程式碼骨架

```python
# Step 1: 預分割（主進程，一次性，~2 秒）
prices_by_sym: dict[str, dict[date, float]] = defaultdict(dict)
for (sym, d), price in price_lookup.items():
    prices_by_sym[sym][d] = price

returns_by_sym: dict[str, dict[date, float]] = defaultdict(dict)
for (sym, d), ret in returns_lookup.items():
    returns_by_sym[sym][d] = ret

# Step 2: 平行 dispatch，每個 worker 只拿自己的切片
with ProcessPoolExecutor(max_workers=12) as executor:
    futures = {
        executor.submit(
            process_symbol, sym, paths,
            prices_by_sym[sym],     # ~1,200 entries, not 3,400,000
            returns_by_sym[sym],
            backtest_start,
        ): sym
        for sym in symbols
    }

    for future in as_completed(futures):
        for r in future.result():
            broker_totals[r.broker] += r  # 聚合
```

### 為什麼有效

| 指標 | 改動前 | 改動後 |
|------|--------|--------|
| 每次 IPC 傳輸量 | 3,400,000 entries | ~1,200 entries |
| 序列化時間/worker | ~2 秒 | ~0.001 秒 |
| 總 IPC 開銷 | ~52 秒 | < 1 秒 |
| 總執行時間 | ~10 分鐘 | ~1 分鐘 |

### 為什麼是最佳方案

1. **改動極小**：只改資料傳遞方式，不改計算邏輯
2. **零新依賴**：只用 Python 標準庫的 `concurrent.futures`
3. **跨平台**：不依賴 OS 特性
4. **好理解**：「只傳需要的資料」是直覺的設計原則

---

## 5. 不採用的解法與原因

### 5.1 fork() + Copy-on-Write (COW)

```
原理: Unix fork() 建立子進程時，不會立即複製記憶體，
      而是共享同一份實體頁面（copy-on-write）。
      如果子進程只讀不寫，就等於零成本共享。

用法: multiprocessing.get_context('fork')

優點: 零序列化成本、零額外記憶體
```

**不採用的原因：**

- macOS 從 Python 3.8 起預設使用 `spawn`（不是 `fork`），因為 `fork` 在 macOS 上與某些 C 函式庫（如 Objective-C runtime）衝突，會產生 deadlock
- Python 3.14 開始，`fork` 在多線程環境下正式被標記為 deprecated
- CPython 的 reference counting 會觸發 COW：即使子進程「只讀」dict，每次存取都會更新物件的 reference count，導致頁面被複製（COW 失效）

**學習價值：** 理解 COW 機制對學 OS 很有幫助，但在 Python 中實際效果打折扣。

### 5.2 multiprocessing.shared_memory

```
原理: 在共享記憶體區段（/dev/shm）中放置資料，
      所有進程直接存取同一塊記憶體。

用法: multiprocessing.shared_memory.SharedMemory
```

**不採用的原因：**

- `shared_memory` 只支援 raw bytes，不能直接放 Python dict
- 需要自己設計序列化格式（類似資料庫的 memory layout）
- 生命週期管理麻煩（忘記 unlink 會殘留在系統中）
- 對 2,839 個 symbol 的問題來說 overkill

**適用場景：** 超大型 numpy array 的跨進程共享（例如影像處理 pipeline）。

### 5.3 Ray / Dask

```
原理: 分散式計算框架，自帶物件存儲（Object Store），
      可透過 shared memory + Apache Arrow 實現零拷貝。

用法:
  import ray
  price_ref = ray.put(price_lookup)       # 放入 Object Store
  futures = [process.remote(price_ref)]   # 零拷貝傳遞
```

**不採用的原因：**

- 引入大型依賴（Ray ~200 MB）只為了解決一個 10 分鐘的批次任務
- 學習成本高，debug 複雜
- 對單機 12 核的場景，stdlib 就夠了

**適用場景：** 多機叢集、需要 auto-scaling、任務圖複雜的情境。

### 5.4 Apache Arrow IPC / Memory-Mapped Files

```
原理: 將 price_lookup 寫成 Arrow IPC 格式的檔案，
      用 mmap 映射到記憶體，所有進程讀同一份。

用法:
  # 主進程寫入
  pa.ipc.RecordBatchFileWriter(...)

  # 子進程讀取（零拷貝）
  pa.ipc.open_file(memory_mapped_file)
```

**不採用的原因：**

- 需要把 dict 轉成 columnar format，再在子進程中做 lookup（需要建 index 或 binary search）
- 增加了架構複雜度
- Pre-partition 已經夠快，不需要這層抽象

**適用場景：** 資料量到 TB 等級、需要零拷貝的 real-time pipeline。

### 方案比較總覽

```
                    改動量    新依賴    跨平台    記憶體    加速比
                    ─────    ──────    ──────    ──────    ─────
Pre-partition       極小      無        是        ~0       8-10x   ◄ 採用
fork COW            小        無        否(macOS) ~0       8-10x
shared_memory       中        無        是        ~0       8-10x
Ray/Dask            大        大        是        中       8-10x
Arrow mmap          中        小        是        ~0       8-10x
```

所有方案的加速比幾乎相同（因為瓶頸在 IPC 不在計算），差別只在實作複雜度。
**選最簡單的。**

---

## 6. 重複掃描問題

### 現象

`etl.py` 將 10 GB 的 `broker_tx.parquet`（20.8 億筆）拆成 2,839 個 per-symbol 檔案。
原本的做法：先掃一次取得 symbol 清單，再對每個 symbol 各掃一次全檔。

```
broker_tx.parquet (10 GB)

  Scan 0:    取 unique symbols                     ← 掃全檔
  Scan 1:    lf.filter(symbol == "0050").collect()  ← 又掃全檔
  Scan 2:    lf.filter(symbol == "0051").collect()  ← 又掃全檔
  ...
  Scan 2839: lf.filter(symbol == "9958").collect()  ← 第 2,840 次

  總掃描次數: 2,840
  實測時間: 30-40 分鐘
```

### 為什麼這麼慢

即使 Polars 有 **predicate pushdown**（利用 parquet row group 的 min/max 統計跳過不相關區塊），
每次掃描仍有固定開銷：讀 metadata、檢查每個 row group、建構 lazy plan。

```
每次掃描的固定開銷: ~0.5-1 秒
2,840 次 × 0.7 秒 ≈ 33 分鐘

其中真正讀取和計算的時間可能只有 5 分鐘，
剩下的 28 分鐘是重複的 metadata 解析和 I/O 排程。
```

### 核心問題

這是 **N+1 查詢問題**的檔案版本：

```
SQL 版本 (N+1 query):
  SELECT DISTINCT symbol FROM trades;          -- 1 次
  SELECT * FROM trades WHERE symbol = '0050';  -- N 次
  SELECT * FROM trades WHERE symbol = '0051';
  ...

Parquet 版本 (N+1 scan):
  lf.select("symbol_id").unique().collect()    -- 1 次
  lf.filter(symbol == "0050").collect()        -- N 次
  lf.filter(symbol == "0051").collect()
  ...

解法相同: 一次讀取，批次處理。
```

### 為什麼不能一次 collect 全部？

最直覺的修法是一次 streaming collect + group_by + partition_by：

```python
df = (
    lf.group_by(["symbol_id", "broker", "date"])
    .agg([...])
    .collect(engine="streaming")   # ← 嘗試一次搞定
)
```

**實測結果：OOM (exit code 137)**。

原因：streaming group_by 需要維護一個 hash table，key 是所有 unique 的
`(symbol_id, broker, date)` 組合。估算：

```
unique groups ≈ 574,596,937（實測值）
每個 group 的 hash entry: ~36 bytes (key + 4 accumulators)
hash table 大小: 574M × 36 ≈ 20 GB
加上 hash table overhead (load factor ~2x): ~40 GB
機器記憶體: 36 GB → OOM
```

---

## 7. 採用的解法：批次掃描

### 核心思路

不是一次處理 1 個 symbol（太多次掃描），也不是一次處理全部（OOM），
而是每次處理 500 個 symbol：

```
原本:     2,840 次掃描，每次 1 symbol      → 30-40 min
一次全部:  1 次掃描，5.7 億 groups          → OOM
批次:     7 次掃描，每次 500 symbols        → 2 min    ◄ 採用
```

### 程式碼骨架

```python
BATCH_SIZE = 500

# Scan 1: 取 symbol 清單（輕量）
symbols = lf.select("symbol_id").unique().collect(engine="streaming")

# Scan 2-7: 每次處理 500 symbols
for batch in batched(symbols, BATCH_SIZE):
    batch_df = (
        lf.filter(pl.col("symbol_id").is_in(batch))
        .group_by(["symbol_id", "broker", "date"])
        .agg([...])
        .collect(engine="streaming")   # ~1 GB per batch, safe
    )

    # 分割寫出
    for symbol_df in batch_df.partition_by("symbol_id"):
        symbol_df.write_parquet(f"daily_summary/{symbol}.parquet")
```

### 實測結果

| 指標 | 改動前 | 改動後 |
|------|--------|--------|
| 掃描次數 | 2,840 | 7 |
| Wall time | 30-40 min | **2 min 5 sec** |
| 記憶體峰值 | ~2 GB | ~2 GB |
| 輸出行數 | 574,596,937 | 574,596,937（一致） |

### 批次大小的取捨

```
BATCH_SIZE    掃描次數    記憶體/batch    總時間（估）
─────────     ────────    ───────────    ──────────
1             2,840       ~1 MB          30-40 min
100           29          ~200 MB        ~5 min
500           6           ~1 GB          ~2 min     ◄ 採用
1000          3           ~2 GB          ~1.5 min
ALL           1           ~20 GB         OOM
```

**經驗法則**：選一個讓每批記憶體在可用 RAM 的 10-20% 以內的 batch size。

---

## 8. 增量處理 vs 全量重跑

### 問題

資料每天更新。四個階段都要重跑嗎？能不能只處理「新的那一天」？

### 逐階段分析

```
階段        能增量嗎？   為什麼？
──────────  ──────────   ──────────────────────────────────────
① ETL       可以         只抽新日期，append 到 daily_summary
② Sync      可以         只從 BigQuery 抓新日期的價格
③ PNL       不行         FIFO 是路徑依賴的（見下方說明）
④ Query     —            讀 100 KB，已經瞬間完成
```

### 為什麼 FIFO 不能增量

FIFO 損益取決於歷史上**每一筆交易的先後順序**：

```
假設券商 A 在三個時間點交易同一支股票：

  2021: 買 100 張 @ $10   ← lot 1
  2023: 買 100 張 @ $50   ← lot 2
  2025: 賣 100 張 @ $60   ← 賣哪一批？

FIFO: 先進先出，賣 lot 1 → 賺 100 × ($60 - $10) = $5,000
LIFO: 後進先出，賣 lot 2 → 賺 100 × ($60 - $50) = $1,000

如果只看 2025 的新資料，不知道要平的是 $10 還是 $50 的倉位。
必須從 2021 年的第一筆交易開始重算。
```

這叫做 **路徑依賴（path dependency）**：結果取決於完整的歷史路徑，
不能只看終點狀態。

### 增量的理論可行性

技術上可以透過**儲存中間狀態**來實現增量 PNL：

```python
# 每次跑完後存下 FIFO 帳戶的狀態
state = {
    ("2330", "1440"): FIFOAccount(long_lots=[Lot(100, 10.0), ...]),
    ...
}
pickle.dump(state, open("fifo_state.pkl", "wb"))

# 下次只處理新的一天
state = pickle.load(open("fifo_state.pkl", "rb"))
for new_trade in today_trades:
    state[key].process_day(new_trade)
```

**為什麼不這樣做：**

| 考量 | 全量重跑 | 增量 + 狀態 |
|------|----------|-------------|
| 正確性保證 | 冪等（跑兩次結果相同） | 狀態損壞 = 全部錯 |
| 歷史修正 | 自動修復 | 需要偵測 + 重算 |
| 程式碼複雜度 | 無狀態，簡單 | 狀態序列化 + 版本管理 |
| Debug 難度 | 隨時可重現 | 需要保存完整狀態快照 |
| 每日成本 | **9 分鐘** | ~5 分鐘（省 4 分鐘） |

### 結論

> **對目前的規模（9 分鐘/天），全量重跑是 best practice。**

這在資料工程中叫 **idempotent full refresh**：

- **冪等性（idempotent）**：跑 N 次結果都一樣
- **全量刷新（full refresh）**：不依賴上次的狀態，每次從頭算
- **自我修復（self-healing）**：供應商修正歷史資料時，下次跑自動修正

增量處理是當全量重跑耗時 **數小時以上** 才值得引入的優化。
引入的時機判斷標準：

```
全量重跑時間    建議策略
─────────────   ──────────────────────────
< 30 分鐘       全量重跑（現在的情況）
30 min ~ 2 hr   考慮增量，但要有全量重跑的退路
> 2 小時        增量處理 + 定期全量校驗
> 1 天          必須增量，設計 checkpoint 機制
```

---

## 9. 關鍵字與學習路線

### 第一層：理解問題（為什麼需要平行化）

| 關鍵字 | 說明 | 建議資源 |
|--------|------|----------|
| **GIL (Global Interpreter Lock)** | Python 最重要的並行限制 | 搜尋 "David Beazley GIL talk" |
| **CPU-bound vs I/O-bound** | 決定用 thread 還是 process | Python docs: `concurrent.futures` |
| **Embarrassingly parallel** | 子任務間無依賴，最容易平行化 | Wikipedia 同名條目 |
| **Amdahl's Law** | 平行化的理論加速上限 | 公式：`S = 1 / ((1-P) + P/N)` |

### 第二層：理解瓶頸（為什麼不能直接平行）

| 關鍵字 | 說明 | 建議資源 |
|--------|------|----------|
| **IPC (Inter-Process Communication)** | 進程間如何傳遞資料 | 搜尋 "Python multiprocessing IPC overhead" |
| **Pickle serialization** | Python 預設的物件序列化 | `pickle` 模組文件 |
| **Compute-to-communication ratio** | 計算時間 vs 通訊時間的比值 | HPC 教材常見概念 |
| **Data locality** | 資料靠近計算的原則 | 搜尋 "data locality principle" |

### 第三層：解法原理

| 關鍵字 | 說明 | 建議資源 |
|--------|------|----------|
| **Partition pruning** | 只傳送需要的資料分區 | 資料庫/Spark 最佳化常見概念 |
| **ProcessPoolExecutor** | Python stdlib 的進程池 | `concurrent.futures` 官方文件 |
| **`as_completed` vs `map`** | 兩種收集結果的模式 | 官方文件有清楚範例 |
| **Copy-on-Write (COW)** | fork 後的記憶體共享機制 | OS 教材（恐龍書 ch3） |
| **Shared memory** | 進程間共享記憶體區段 | `multiprocessing.shared_memory` |
| **Apache Arrow IPC** | 零拷貝跨進程資料格式 | Arrow 官方文件 |

### 第四層：ETL 與資料處理模式

| 關鍵字 | 說明 | 建議資源 |
|--------|------|----------|
| **N+1 query problem** | 對每筆資料各做一次查詢（本案的檔案版） | 搜尋 "N+1 query problem ORM" |
| **Predicate pushdown** | 把 filter 條件下推到 I/O 層，跳過不需要的資料區塊 | Polars / Spark 文件 |
| **Parquet row group** | Parquet 檔案的分塊單位，每個 row group 有獨立的統計資訊 | 搜尋 "parquet file format internals" |
| **Streaming vs collect** | Polars 的兩種執行模式：全量載入 vs 串流處理 | Polars 官方文件 |
| **partition_by** | 按欄位值分割 DataFrame，用於 fan-out 寫出多檔案 | Polars `DataFrame.partition_by` |

### 第五層：Pipeline 架構決策

| 關鍵字 | 說明 | 建議資源 |
|--------|------|----------|
| **Idempotent full refresh** | 每次從頭算，跑 N 次結果相同 | 搜尋 "idempotent data pipeline" |
| **Incremental processing** | 只處理新增/變更的資料 | 搜尋 "incremental vs full refresh ETL" |
| **Path dependency** | 結果取決於完整歷史路徑（如 FIFO） | 經濟學/物理學概念，金融常見 |
| **Self-healing pipeline** | 上游修正資料時，下次重跑自動修復 | dbt 文件中常見此概念 |
| **Checkpoint / state management** | 儲存中間狀態供增量使用 | Spark Structured Streaming 文件 |

### 建議學習順序

```
--- 平行化 ---

1. 先讀 concurrent.futures 官方文件（30 分鐘）
   → 理解 ProcessPoolExecutor 的基本用法

2. 寫一個 toy example 驗證 IPC 開銷（1 小時）
   → 傳大 dict vs 小 dict 給 worker，用 time.time() 量測差異

3. 搜尋 "David Beazley understanding the GIL"（1 小時）
   → 最經典的 GIL 講解

4. 讀 Amdahl's Law 的維基百科（15 分鐘）
   → 理解為什麼不是 12 核 = 12 倍

--- ETL 模式 ---

5. 讀 Polars User Guide 的 Lazy / Streaming 章節（30 分鐘）
   → 理解 scan_parquet → lazy plan → collect 的執行模型

6. 搜尋 "N+1 query problem" 並對照本案的 N+1 scan（15 分鐘）
   → 同一個 anti-pattern 在 ORM、API、檔案處理中反覆出現

7. 讀 Apache Parquet 格式規格的 row group 章節（30 分鐘）
   → 理解 predicate pushdown 為什麼能跳過資料區塊

--- Pipeline 架構 ---

8. 搜尋 "idempotent data pipeline"（15 分鐘）
   → 理解為什麼全量重跑比增量簡單、可靠

9. (進階) 讀 dbt 的 incremental models 文件
   → 理解增量處理在什麼規模才值得引入

10. (進階) 讀 Ray 的架構文件
    → 理解大型系統如何用 shared object store 解決 IPC 問題
```

### 三句話總結

> **平行化不難，難的是搬運資料。減少搬運量，問題就解決了。**
>
> **不要掃描 N 次，掃描 1 次（或盡量少次）。這是 N+1 問題的通用解法。**
>
> **能全量重跑就全量重跑。增量是規模逼出來的，不是預設選項。**
