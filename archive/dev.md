# 股票分點 PNL 高速運算系統開發手冊 (High-Speed PNL Analytics System)

## 1. 系統架構與設計哲學

本系統旨在處理 **1.2 億筆等級 (1700 股票 × 720 天 × 1000 券商)** 的交易數據。

### 核心設計選擇：

- **3D Tensor (NumPy/ndarray)**: 將數據維度化，達成 $O(1)$ 的極速存取。
- **Tidy Data (Parquet)**: 捨棄 JSON 的巢狀結構，改用列式存儲以優化 I/O 並節省 90% 空間。
- **Hybrid Cloud Strategy**:
  - **雲端 (BigQuery)**: 僅存放輕量化的「收盤價」與「真值原始數據」。
  - **地端 (Polars/Local)**: 執行大規模 Join 與 PNL 預運算，省下雲端計算費用。

---

## 2. 數據流與抽象模組架構

### A. Data Ingestion Module (地端預處理)

- **任務**: 將地端 JSON 壓扁並聚合。
- **邏輯**:
  1.  讀取原始 JSON，利用 `Explode` 展開交易明細。
  2.  執行 **Intraday Aggregation**: 對 (Date, Symbol, Broker) 進行 GroupBy，產出「當日總買入額」、「當日總賣出額」、「當日成交淨股數」。
- **輸出**: `daily_trade_summary.parquet`

### B. Cloud Sync Module (BigQuery 輕量同步)

- **任務**: 下載並快取 $S \times D$ 的收盤價矩陣。
- **邏輯**:
  1.  透過 API 僅查詢 `date`, `symbol`, `close_price` 欄位。
  2.  實施 **Incremental Sync**: 僅下載地端缺乏的日期數據，並與歷史快取合併。
- **輸出**: `price_master.parquet`

### C. Dimension Mapping Module (維度映射)

- **任務**: 將字串標籤轉換為整數索引。
- **邏輯**:
  1.  建立三張映射表：日期 (0-719)、股票 (0-1699)、券商 (0-999)。
  2.  將 Tidy Data 裡的字串欄位轉換為 `int16` 索引，為填入 Tensor 做準備。

### D. Fusion & PNL Engine (融合運算引擎)

- **任務**: 計算最終 PNL 並建構 3D 矩陣。
- **邏輯**:
  1.  **Left Join**: 以交易表為主，併入收盤價。
  2.  **Cumulative Inventory**: 依時間序列計算每個分點的「累計持股餘額」。
  3.  **MtM Calculation**:
      - $PNL = (賣出總額 - 買入總額) + (今日持股 \times 今日收盤價) - (昨日持股 \times 昨日收盤價)$
- **輸出**: 記憶體中的 **3D PNL Tensor (Shape: 1700, 720, 1000)**

---

## 3. 需求實作邏輯 (Business Logic Layer)

系統完成後，所有分析需求應直接操作 **3D Tensor** 以獲得最優 Big O：

| 需求             | 實現方式                                                         | 複雜度 (Big O)                   |
| :--------------- | :--------------------------------------------------------------- | :------------------------------- |
| **勝率排序**     | 對 Tensor 的時間軸 (Axis 1) 執行 `count_if(pnl > 0)` 並排序。    | $O(N)$ 聚合 / $O(B \log B)$ 排序 |
| **5日總價量**    | 使用 Sliding Window 對成交量軸進行移動加總。                     | $O(N)$ (透過增量更新)            |
| **分點持股明細** | 對分點軸 (Axis 2) 進行切片 (Slicing)，提取該分點對應的 2D 矩陣。 | $O(1)$ 記憶體尋址                |
| **異常提醒**     | 結合勝率 Top 10 的 Mask 矩陣與 5 日增量矩陣進行 `AND` 位運算。   | $O(N)$ (向量化運算)              |

---

## 4. 開發與部署指導

### 第一步：環境準備

- 安裝 **Polars**: 用於地端 PB 級別數據的清洗。
- 安裝 **NumPy**: 用於構建 3D 運算矩陣。
- 配置 **Google Cloud SDK**: 確保能高效訪問 BigQuery。

### 第二步：核心轉換腳本

- 實作 `json_to_parquet` 轉換函數，確保資料型態轉換為最小可行型態（如 `float32`, `int16`）。
- 實作 `pnl_calc_logic`，注意處理「除權息」或「持股歸零」的邊界條件。

### 第三步：效能驗證

- 確認矩陣載入記憶體後，單次分點勝率計算應在 **< 10ms** 完成。
- 確認 3D Tensor 的持久化（推薦使用 `numpy.memmap` 或 `zarr`）以達成即時讀取。

---

## 5. 給開發者的建議 (Pro-tips)

1.  **不要使用 Python 原始 Loop**: 在這 12 億筆數據前，任何 Python `for` 迴圈都是災難。請始終使用 NumPy 向量化或 Polars Lazy API。
2.  **記憶體優化**: 若 5GB 依然太吃緊，可將 Tensor 的 PNL 轉換為 `int` (放大 100 倍儲存) 以節省空間。
3.  **預先計算**: 對於「5日增量」這類頻繁需求，應在 Fusion 階段就作為一個維度預先算好。
