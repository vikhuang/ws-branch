# 給 ws-admin session 的任務:tej_shareholding 168 檔股票歷史缺列(ws-branch audit A9)

> 貼給在 `~/r20/wp/ws-admin`(或 ws-tejdata 產線)工作的 session。本文只陳述觀察與要交付的東西,
> 不預設根因。所有數字由 ws-branch 於 2026-09-22 從 `~/r20/data/tej/shareholding.parquet`
> (meta updated_at 2026-09-21T13:40)實測。

## 觀察(已查證)

1. `tej_shareholding`(`TWN/APISHRACT`,producer ws-tejdata)對 **168 檔普通股**只有
   **2026-03-20 起**的列;2016–2025 整段**零列**(其中 1 檔例外,2020–2023 有零星列),
   2026-01-02 → 2026-03-19 這 47 個交易日也沒有。
2. 這 168 檔的代號集中在三段:**8089–8499、8905–8996、9802–9962**(完整清單:
   ws-branch `docs/a9_missing_coids.txt`)。上市、上櫃各約一半,都是 `stock_attr` 裡的
   普通股,且分點資料(fugle broker_tx)顯示外資券商席位在其中 80% 的股票日有成交
   ——**不是**「法人沒交易所以 TEJ 省略」。
3. 同一檔案裡其他股票 2016 起連續;2026 全市場逐日列數 p10/p50/max = 2,124 / 2,413 /
   2,447,3-20 之前每天恰好少這 168 檔上下。
4. 另有 260 列(2026)存在但八個金額欄全 null,未分析是否同源。

## 假設(請驗證,不要假設哪個對)

- A. ws-tejdata 的 coid 清單(拉取 universe)在 2026-03-20 前不含這段代號
  (例如清單來源是某個 tickers 快照、或以代號範圍/市場別分批而漏了一批)。
- B. 歷史回填(backfill)當時用的清單與日更清單不同,回填沒涵蓋這 168 檔。
- C. TEJ API 端本身對這批 coid 在早期無資料(需用 API 直接查一檔一天,例如 9941 2025-07-01,
   若 API 有回列則排除 C)。

## 要交付的

1. **根因**:指出 ws-tejdata 產線中決定「拉哪些 coid、哪些日期」的那段程式/設定,以及
   2026-03-20 那天發生了什麼(commit、設定變更、或清單來源更新)。
2. **對 C 的直接證據**:對清單中 3 檔(建議 9941、8440、8929)各查一個 2025 日期的 API 回應。
3. **補拉**:若 API 有資料,補 2016-01-04 → 2026-03-19 這 168 檔;寫入方式沿用既有
   append/去重規則(`coid, mdate` 唯一),**不得改動既有列**;更新 `shareholding_meta.json`
   的 row_count / date_range;若 API 額度不允許一次補完,先補 2025-01-02 → 2026-03-19
   (ws-branch 目前只物化 2025、2026)。
4. **防再犯**:doctor 加一項「逐日 coid 數對照 stock_attr 普通股數」的檢查(2026 的 3-19 前
   後差 168 檔就會抓到),或等價的覆蓋率告警。
5. **回報給 ws-branch**:補拉完成後的 (年, 股票數, 列數) 表;ws-branch 端會重建
   `t3_official_daily` / `t3b_accounting_bounds` / `t4_broker_measure` 2025–2026 並重跑
   Phase 4 2025(audit A9 結案條件)。

## 驗證查詢(補拉前後各跑一次)

```python
import polars as pl
sh = pl.read_parquet("~/r20/data/tej/shareholding.parquet")
miss = open("~/r20/wp/ws-branch/docs/a9_missing_coids.txt").read().strip().split(",")
print(sh.filter(pl.col("coid").is_in(miss))
        .with_columns(pl.col("mdate").dt.year().alias("y"))
        .group_by("y").agg(pl.col("coid").n_unique().alias("stocks"), pl.len().alias("rows")).sort("y"))
# 期望:2016–2026 每年 stocks ≈ 168(扣除當年尚未上市/已下市者)
```

## 不要做的

- 不要重建整個 shareholding.parquet;不要動 2026-03-20 之後既有列。
- 不要用零填補缺的股票日(ws-branch 下游會把「有列但 0」當成真實的零)。
- 不要順手改 catalog 的 schema 或欄名。
