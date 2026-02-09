# 重構計畫書

## 一、現有功能清單（重構後必須保留）

### 1. ETL 管線
| 功能 | 檔案 | 輸入 | 輸出 | 驗證方式 |
|------|------|------|------|----------|
| JSON 轉 Parquet | `etl.py` | `2345.json` | `daily_trade_summary.parquet` | 檔案存在 + 筆數一致 |
| 價格同步 | `sync_prices.py` | BigQuery | `price_master.parquet` | 日期範圍正確 |
| FIFO PNL 計算 | `pnl_engine.py` | parquet files | `realized_pnl.npy`, `unrealized_pnl.npy`, `closed_trades.parquet` | 零和檢驗 |

### 2. 指標計算
| 功能 | 公式 | 驗證方式 |
|------|------|----------|
| 日報酬率 | `(close[t] - close[t-1]) / close[t-1]` | 抽樣驗算 |
| Pearson 相關 | 標準公式 | 對比 numpy.corrcoef |
| 執行 Alpha | `trade_return - benchmark_return` | 加權平均 ≈ 0 |
| 擇時 Alpha | `Σ((net_buy[t-1] - avg) × return[t])` | 正負對稱 |
| Permutation Test | shuffle 1000x, 計算 p-value | 5% 顯著 ≈ 預期 |
| Lead 相關 | `corr(net_buy[t-1], return[t])` | 數值範圍 [-1, 1] |
| Lag 相關 | `corr(return[t-1], net_buy[t])` | 數值範圍 [-1, 1] |

### 3. 報告輸出
| 功能 | 輸出格式 | 驗證方式 |
|------|----------|----------|
| 完整排名報告 | CSV, Parquet, Excel | 940 筆 + 欄位完整 |
| 兩日報告 | Excel | 期初歸零計算正確 |
| 券商評分卡 | 終端輸出 | 6 維度完整 |

### 4. 資料結構
| 類別 | 欄位 | 用途 |
|------|------|------|
| `Lot` | shares, cost_per_share, buy_date | FIFO 成本批次 |
| `ClosedTrade` | symbol, broker, shares, buy_date, buy_price, sell_date, sell_price, realized_pnl, trade_type | 平倉記錄 |
| `FIFOAccount` | long_lots, short_lots, realized_pnl, closed_trades | 帳戶狀態 |

---

## 二、重複程式碼統計

| 函數 | 出現次數 | 檔案列表 |
|------|----------|----------|
| `load_data()` | 8 | analyze_*.py, generate_ranking_report.py, query_ranking.py |
| `calculate_returns()` | 6 | 同上 |
| `pearson_correlation()` | 4 | analyze_broker_scorecard.py, analyze_timing_alpha.py, generate_ranking_report.py, analyze_predictive.py |
| `calculate_timing_alpha()` | 3 | analyze_timing_permutation.py, analyze_broker_scorecard.py, generate_ranking_report.py |
| `permutation_test()` | 2 | analyze_timing_permutation.py, generate_ranking_report.py |

---

## 三、重構六步驟

### Step 1: 建立 domain/models.py
- 抽取 `Lot`, `ClosedTrade` 資料類別
- 加入防禦性驗證
- **驗證**: import 成功 + 單元測試

### Step 2: 建立 infrastructure/repositories/
- `TradeRepository`: 讀取交易資料
- `PriceRepository`: 讀取價格資料
- `BrokerRepository`: 讀取券商名稱
- **驗證**: 替換 `load_data()` 後功能不變

### Step 3: 建立 domain/returns.py
- `calculate_returns()`
- `pearson_correlation()`
- **驗證**: 數值結果與原始一致

### Step 4: 建立 domain/metrics/
- `execution_alpha.py`
- `timing_alpha.py`
- `statistical.py` (permutation_test)
- **驗證**: 美林指標數值一致

### Step 5: 建立 application/services/
- `PnlService`: 協調 FIFO 計算
- `RankingService`: 協調排名報告
- **驗證**: 生成報告與 v0.10.0 一致

### Step 6: 建立 interfaces/cli.py
- 統一命令列入口
- 刪除舊的獨立腳本
- **驗證**: 所有功能可透過 CLI 呼叫

---

## 四、驗證檢查點

### 每步驟後執行：
```bash
# 1. 確認 import 無錯誤
uv run python -c "from pnl_analytics import *"

# 2. 確認核心功能
uv run python -m pnl_analytics.cli ranking --verify

# 3. 對比輸出（與 v0.10.0）
diff ranking_report_new.csv ranking_report_v0.10.0.csv
```

### 最終驗證：
| 檢查項目 | 預期結果 |
|----------|----------|
| 美林 PNL | 97.97 億 |
| 美林執行 Alpha | +0.1318% |
| 總券商數 | 940 |
| 零和檢驗 | 已實現 + 未實現 ≈ 0 |

---

## 五、不重構的部分

| 項目 | 原因 |
|------|------|
| `etl.py` | 簡單、獨立、無重複 |
| `sync_prices.py` | 簡單、獨立、無重複 |
| `analysis.md` | 文檔 |

---

## 六、刪除清單（重構後）

```
# 被模組取代
analyze_alpha.py        # 被 v2 取代
analyze_alpha_v2.py     # 併入 metrics/
analyze_timing_alpha.py # 併入 metrics/
analyze_timing_permutation.py # 併入 metrics/
analyze_broker_scorecard.py   # 併入 services/
analyze_high_performers.py    # 探索性，封存
analyze_predictive.py         # 探索性，封存
query_ranking.py              # 併入 cli/

# 暫存檔
~$*.xlsx
predictive_analysis_results.json
```

---

## 七、目標結構

```
pnl_analytics/
├── __init__.py
├── domain/
│   ├── __init__.py
│   ├── models.py           # Lot, ClosedTrade
│   ├── returns.py          # calculate_returns, pearson_correlation
│   ├── fifo.py             # FIFOAccount
│   └── metrics/
│       ├── __init__.py
│       ├── execution_alpha.py
│       ├── timing_alpha.py
│       └── statistical.py
├── infrastructure/
│   ├── __init__.py
│   ├── config.py
│   └── repositories/
│       ├── __init__.py
│       ├── trade_repo.py
│       ├── price_repo.py
│       └── broker_repo.py
├── application/
│   ├── __init__.py
│   └── services/
│       ├── __init__.py
│       ├── pnl_service.py
│       └── ranking_service.py
└── interfaces/
    ├── __init__.py
    └── cli.py

# 保留根目錄
etl.py
sync_prices.py
pnl_engine.py  # 之後可移入 application/
```
