# Python 平行化參考筆記：PNL Engine 案例

> 這份文件記錄 `pnl_engine.py` 從單線程改為平行處理的思考過程。
> 適合作為 Python multiprocessing 的入門案例學習。

---

## 目錄

1. [問題定義](#1-問題定義)
2. [為什麼 Python 需要特別處理平行化](#2-為什麼-python-需要特別處理平行化)
3. [本專案的具體瓶頸](#3-本專案的具體瓶頸)
4. [採用的解法：Pre-partition](#4-採用的解法pre-partition)
5. [不採用的解法與原因](#5-不採用的解法與原因)
6. [關鍵字與學習路線](#6-關鍵字與學習路線)

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

## 6. 關鍵字與學習路線

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

### 建議學習順序

```
1. 先讀 concurrent.futures 官方文件（30 分鐘）
   → 理解 ProcessPoolExecutor 的基本用法

2. 寫一個 toy example 驗證 IPC 開銷（1 小時）
   → 傳大 dict vs 小 dict 給 worker，用 time.time() 量測差異

3. 搜尋 "David Beazley understanding the GIL"（1 小時）
   → 最經典的 GIL 講解

4. 讀 Amdahl's Law 的維基百科（15 分鐘）
   → 理解為什麼不是 12 核 = 12 倍

5. (進階) 讀 Ray 的架構文件
   → 理解大型系統如何用 shared object store 解決 IPC 問題
```

### 一句話總結

> **平行化不難，難的是搬運資料。減少搬運量，問題就解決了。**
