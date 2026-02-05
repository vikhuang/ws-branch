# 專案文檔：股票交易大數據 PNL 高速運算架構

## 1. 任務目標 (Mission Statement)
將分散且巢狀的 JSON 交易明細數據，轉換為高效能的 **Tidy Data (Parquet)** 格式，並最終建構為 **記憶體內 3D 張量 (3D Tensor)**。此設計旨在達成 $O(1)$ 時間複雜度的數據存取，並支撐億級規模的矩陣運算與策略分析。

---

## 2. 數據規模與維度 (Data Scale)
本專案處理的資料量體如下：
* **股票 (Symbols)**: ~1,700 支
* **時間 (Days)**: ~720 天 (約 3 年交易日)
* **券商 (Brokers)**: ~1,000 家
* **總記錄數**: $1700 	imes 720 	imes 1000 pprox 1.2 	imes 10^9$ (12 億筆 PNL 記錄)

---

## 3. 架構設計選擇與理由 (Design Rationales)

### A. 預壓縮 PNL (Flattening)
* **做法**: 在 ETL 階段直接將 `buy`/`sell` 明細依據公式 $PNL = \sum (SellQty 	imes Price) - \sum (BuyQty 	imes Price)$ 壓扁。
* **理由**: 原始 JSON 的巢狀結構會導致運算時重複進行解析與浮點運算。預先聚合能將數據複雜度降低一個維度。

### B. 選用 Parquet 存儲
* **理由**: Parquet 支援 **Columnar Storage (行式存儲)**。當我們只需要計算某個券商的損益時，系統只需讀取 `pnl` 與 `broker_id` 欄位，無需加載整張表，極大降低 I/O 壓力。

### C. 採用 Polars 運算引擎
* **理由**: 針對 10 億級數據，Pandas 的記憶體管理會崩潰。Polars 基於 Rust 與 Apache Arrow，支援多執行緒並行與 SIMD 指令集，是目前處理此類數據的最快選擇。

### D. 終極運算形態：3D Tensor (NumPy)
* **結構**: `matrix[Symbol_Index][Date_Index][Broker_Index]`
* **理由**: 
    * **存取速度**: $O(1)$ 直達。
    * **記憶體優化**: 使用 `float32` 存儲，總佔用空間約 $4.9 	ext{ GB}$ ($1.2B 	imes 4 	ext{ bytes}$)，可完整放入現代伺服器 RAM。

---

## 4. 實作指導 (Implementation Guide)

### 第一步：ETL 數據清洗 (JSON to Parquet)
使用 Polars 將 JSON 壓扁成 Tidy Data 格式。
```python
import polars as pl

# 核心邏輯：展開巢狀 struct 並聚合 PNL
df = pl.read_json("data.json")
tidy_df = (
    df.explode("data")
    .with_columns([
        (pl.col("data").struct.field("sell").cast(pl.Int64) * pl.col("data").struct.field("price").cast(pl.Float32) -
         pl.col("data").struct.field("buy").cast(pl.Int64) * pl.col("data").struct.field("price").cast(pl.Float32)
        ).alias("pnl_component")
    ])
    .group_by(["date", "symbol_id", "broker"])
    .agg(pl.col("pnl_component").sum().alias("pnl"))
)
tidy_df.write_parquet("tidy_pnl.parquet")
```

### 第二步：建立整數索引 (Indexing)
為了轉化為矩陣，必須建立映射表（Mapping Table）：
* `date_map`: `{ "2026-01-29": 0, ... }`
* `symbol_map`: `{ "2313": 0, ... }`
* `broker_map`: `{ "6163": 0, ... }`

### 第三步：填充 3D 矩陣
```python
import numpy as np
# 初始化 3D 矩陣
pnl_tensor = np.zeros((1700, 720, 1000), dtype=np.float32)

# 根據 mapping 將數據填入相對應的座標
# pnl_tensor[s_idx, d_idx, b_idx] = pnl_value
```

---

## 5. 給後續開發者/LLM 的提示
1. **避免 Loops**: 絕對不要在 12 億筆資料上使用 Python `for` 迴圈。請務必使用向量化運算（Vectorized Operations）。
2. **記憶體管理**: 在轉換過程中，手動執行 `del` 刪除暫時性的 DataFrame 並調用 `gc.collect()`。
3. **類型檢查**: 確保 PNL 使用 `float32`而非 `float64`，這可以節省一半的記憶體空間且不影響金融計算精度。

---
**Document Status**: Final Version 1.0
**Author**: Gemini Collaborative AI
