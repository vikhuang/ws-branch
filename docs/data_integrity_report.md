# Data Integrity Report: broker_tx 日期修復影響評估

> 調查日期：2026-03-17
> 觸發原因：ws-admin 發現 broker_tx Google Drive 批次下載有 21.1% 日期標記錯誤
> 結論：**ws-branch pipeline 不受影響**

---

## 事件摘要

ws-admin 於 2026-03-17 發現 Fugle broker_tx 歷史資料存在日期標記錯誤
（詳見 `ws-admin/docs/fugle_api/error_fix_20260317.md`）：

| 階段 | 期間 | 問題 | 檔案數 |
|------|------|------|--------|
| Phase 1 | 2021-01 ~ 2025-02 | 檔名日期 = 實際交易日 +2 天（週五→週日） | 224 檔 |
| Phase 2 | 2025-03 ~ 2026-03 | 正確+錯誤檔案並存（重複） | 52 檔 |
| API 增量 | 2026-03-10+ | 正確 | 5 檔 |

修復腳本已套用：224 檔 rename + 修正 date 欄位，52 檔 delete 重複。
修復後 broker_tx：1,258 檔 = TEJ 1,258 交易日，0 不匹配。

---

## 影響評估

### 關鍵時序

| 事件 | 日期 | 說明 |
|------|------|------|
| daily_summary 建立 | 2026-02-14 | ETL 全量重建（`etl.py`） |
| broker_tx migration | 2026-03-10 | Google Drive 批次檔案匯入 `~/r20/data/fugle/broker_tx/` |
| daily_summary 增量更新 | 2026-03-14 | `etl.py --incr`（只加 3/10 以後的新日期） |
| broker_tx 日期修復 | 2026-03-17 | `scripts/fix_broker_tx_dates.py --apply` |

**ETL 全量重建（2/14）發生在 Google Drive 匯入（3/10）之前。**

2/14 的 ETL 使用的是當時已存在的 broker_tx 資料（非 Google Drive 批次下載），
該資料的日期標記是正確的。3/14 的增量更新只處理了 3/10 以後的新資料
（API 增量，日期正確）。

---

### 驗證結果

#### 1. daily_summary 審計

掃描全部 2,869 個 `daily_summary/{symbol}.parquet`：

| 指標 | 結果 |
|------|------|
| 總 entries | 600,698,449 |
| 非交易日 entries | **0**（0.0%） |
| 非交易日 unique dates | 0 |
| 受影響 symbols | 0 / 2,869 |
| Duplicate (broker, date) | 0（以 2330 驗證） |

Phase 1 特徵驗證（2330）：

| 日期 | 在 daily_summary 中？ | 預期 |
|------|---------------------|------|
| 2024-01-05（週五） | ✅ 有 | 正確 |
| 2024-01-07（週日） | ❌ 無 | 正確（如果資料有錯會出現在這裡） |
| 2023-03-03（週五） | ✅ 有 | 正確 |
| 2023-06-30（週五） | ✅ 有 | 正確 |
| 2024-06-28（週五） | ✅ 有 | 正確 |
| 2024-12-27（週五） | ✅ 有 | 正確 |

所有 Phase 1 期間的週五都有資料，對應的週日都沒有。

#### 2. broker_tx 原始檔案審計（修復後）

| 指標 | 結果 |
|------|------|
| 總檔案 | 1,258 |
| TEJ 交易日（同範圍） | 1,258 |
| 檔名 vs 內部 date 不匹配 | 0 |
| 非交易日檔案 | 0 |
| 缺少交易日 | 0 |

#### 3. daily_summary 日期覆蓋率（2330）

| 指標 | 結果 |
|------|------|
| daily_summary 日期數 | 1,252 |
| TEJ 交易日（同範圍） | 1,258 |
| 缺少 | 6 天 |

缺少的 6 天中 5 天是 2330 無交易（正常），1 天是 2026-03-16（ETL 在 3/14 跑的）。

---

## 結論

| Pipeline 層 | 受影響？ | 原因 |
|-------------|---------|------|
| daily_summary | **否** | 2/14 建立時使用正確的 broker_tx 資料 |
| pnl_daily / pnl | **否** | 基於正確的 daily_summary 產出 |
| hypothesis CV 結果 | **否** | 基於正確的 pipeline output |
| ws-quant 回測結果 | **否** | 基於正確的 export CSV |

**broker_tx 的日期修復不影響 ws-branch 現有的任何分析結果。**

修復是針對 2026-03-10 Google Drive 批次匯入的檔案。
ws-branch 的 pipeline output 在匯入前（2/14）就已建立，
增量更新（3/14）只處理了 API 增量資料（日期正確）。

---

## 建議

1. **不需要重跑 ETL/PNL** — 現有資料正確
2. **未來 ETL 全量重建安全** — broker_tx 已修復，重跑結果應與現有一致
3. **增量更新安全** — 3/16 的資料可在下次 `etl.py --incr` 時自動加入
4. **建議加入 ETL 健檢** — 在 ETL output 驗證步驟中加入「非交易日 entries = 0」斷言
