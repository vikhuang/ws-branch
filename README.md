# ws-branch

> **v1.0.0(2026-09-15)重設計**:本 repo 使命改為「分點資料的清洗聚合與歸因量測」。
> 藍圖 `docs/REDESIGN_2026-09.md`、體檢帳本 `docs/audit_ledger.md`。
> P4-P6 重心已轉移至觀測站架構 `docs/OBSERVATORY_2026-09.md`
> (四層維度:行動者/部位/執行/關係,O1-O6 分期)。
>
> - 表:T1 分點×股票×日(逐 raw 日聚合建表,2025-2026 物化)/T2 價位表(ws-core lazy 視圖)/
>   T3 官方法人流(2016+,單位=股/元;v2 含自營金額)/**T3b 會計硬界限**
>   (v1.1.0,股票×日×側:官方外資量有多少必須在外資席位之外)/T4 分點日行為特徵
>   (v2 rank 版)/**T4 v3 `t4_broker_measure`**(v1.3.0,分點×日最小量測表 + manifest)
> - CLI:`uv run python -m ws_branch build|verify --table <t> [--year Y]`;
>   個股讀本 `uv run python scripts/observatory/v3_readbook.py 3450 2026-09-14`;
>   分點 profile `uv run python scripts/observatory/v3_broker_profile.py 8440 2026-09-14`
> - 對外契約:`docs/DATA_CONTRACT.md`;`uv run python -m ws_branch contract` 查 schema/
>   單位/缺值/可用時間，`export` 產 Parquet + receipt，供 ws-quant 以檔案介面取用
> - 架構:`src/ws_branch/` 三色分層(純轉換/IO 殼/宣告);測試金字塔
>   (單元+e2e 合成小宇宙+真資料對帳),`uv run pytest`(231 tests;含 20 條 e2e 與分層執法 test_architecture)
> - **現行版 v1.4.2(2026-09-22)**:v3 Phase 1-4 + 2025 + Step E 產品(T4 v3 / 讀本 v2 / profile)+ Step F 盤點;
>   未做 = 排程/`--incr`、A9 上游、ws-desk 接讀本。以下為版本史:
> - **v1.1.0(2026-09-21)行動者層 v3 Phase 1-2**:universe gate、會計硬界限、
>   trait/日效應/席位異常分解;規格在 ws-quant `docs/actor_layer_v3_*_2026-09-18.md`,
>   結果在 `experiments/flow_lab/findings/v3_phase{1,2}_*.md`
> - **v1.2.0(2026-09-21)Phase 3-4 + 納入 2025**:stock salience、actor calibration
>   (外資桶殘差 AUC 年內 0.83 / 跨年 0.75-0.82;fund/prop 無區分力)、cohort v3
>   時變有效期(9A81 永豐金-匯立不入 cohort,讀本標「外資客戶*」)、T1 逐日建表
>   (記憶體事故 ④);findings `v3_phase{3,4}_*.md`、`v3_crossyear_2025.md`
> - **v1.3.0(2026-09-21)Step E**:`t4_broker_measure` 物化(§4.2 欄位、`available_at`、
>   manifest 含 source_snapshot/逐欄認識論標籤;官方 cosine 在 T3 觀測支撐上算,
>   `official_missing_share`)、as-of 契約測試(追加未來日既有列凍結)、讀本改讀
>   T4 v3 並補席位性格/市況/狀態(`measure/state.py`);audit A9 = TEJ shareholding
>   168 檔 2025 整年缺列(上游待查)
> - **v1.3.1(2026-09-21)外部審查四點修正**:t3b 缺 T3/行情的股票日改進表並標
>   不可發布(母體口徑可發布率 2025 91.2% / 2026 96.8%,verify 加母體覆蓋檢查);
>   state 加規模/廣度條件化(讀本「規模」欄);校準撤回「下界」措辭、加 refit
>   leave-one-out(逐席位 AUC:大五家 0.93-0.98、大和國泰 0.27)、Phase 4 以 m2
>   物化表重跑(年內 0.823/0.789、跨年 0.737/0.814);manifest 標 available_at 依據
> - **v1.4.0(2026-09-21)分點 profile**:席位頁四區塊(raw / 性格·規模·市況·特有 /
>   官方配置關聯 + 校準卡 + 殘差百分位 / 本子裡最重的股票),每欄標認識論等級;
>   唯一顯示 cosine 的產品,永遠綁校準卡與「不是身份機率」。§12 完成標準 3
>   (讀本 + profile)到位;剩 Step F、`--incr`、A9 上游
> - **以下舊 README 描述的 PNL/FIFO 系統已整包封存於 `pnl/`**,主線不維護;
>   復用前置條件見 audit_ledger。
> - **v1.4.1(2026-09-21)外部審查第二輪五點**:讀本缺值崩潰與歷史身分變更中止修復;
>   rank_align/robustness 改讀物化表(全部校準路徑同一份公式);校準卡買賣分側、
>   fund/prop 改「無席位真值、無法校準」;manifest 產物補 available_at_basis
> - **v1.4.2(2026-09-22)Step F 盤點**:T1–T4 在 ws-quant/ws-mcp/ws-desk/ws-admin/ws-core
>   **零程式引用**,沒有消費端要遷(`docs/STEP_F_INVENTORY_2026-09-21.md`);T4 v2
>   `t4_broker_features` 標 frozen(可重建供重現、不更新);待決:ws-admin 登記 dataset、
>   ws-desk 接讀本、排程 + `--incr`、A9 上游
> - **v1.4.3(2026-09-22)SRP 分層 + e2e 補強**:cohort/verify/windows/_fmt/loaders 切出、runner 去表
>   內部、CLI 薄殼、`test_architecture.py` 五條分層執法;e2e +8(CLI 正常/缺值/缺 T3 頁、profile、
>   t3b 母體/T4 v3 不變量/重算恆等三種警報、frozen v2 重現)
> - **v1.4.4(2026-09-22)A9 結案**:ws-admin 補拉 168 檔 2016→2026-03-19;T3/T3b/T4 v3 重建,
>   t3b 可發布率 2025 91.2% → **99.99%**;Phase 4 重跑 年內 0.824/0.794、跨年 0.748/0.823;
>   校準卡獨立成 `measure/calibration_card.py`(profile 與 T4 manifest 同一份數字)

---

# (封存)ws-branch PNL 系統(原 README)

全市場券商分點交易資料的 PNL 回測系統。回答五個問題：

1. **哪些券商在賺錢？** — 依 FIFO 計算每家券商的已實現 + 未實現損益，全市場排名
2. **聰明錢在買什麼？** — 給定一支股票，看「在這支股票上歷史績效最好的券商」現在站哪邊
3. ~~**大單有預測力嗎？**~~ — ⚠️ 已封存：T+1 intraday alpha 因時區修正後不存在
4. **聰明錢的累積行為能預測報酬嗎？** — 事件研究：PNL top-K 券商的買賣異常 → 多期 forward return 統計檢定
5. **各種券商行為假說是否成立？** — 可組合五步假說檢定框架，10 種策略（反差券商、加碼信號、集體撤退、逆勢操作等）

## 快速開始

```bash
uv sync

# Pipeline（全市場 2,839 股，約 15 分鐘）
uv run python etl.py                        # → data/daily_summary/
uv run python pnl_engine.py                 # → data/pnl_daily_merged/ + data/pnl_merged/ + data/derived/

# 增量更新（每日只處理新資料）
uv run python etl.py --incr                 # 只處理新 broker_tx → append to daily_summary
uv run python pnl_engine.py --incr          # 從 fifo_state checkpoint 恢復，只算新日期

# 券商合併對照表（首次需要）
uv run python generate_merge_map.py         # → data/derived/broker_merge_map.json

# 查詢
uv run python -m broker_analytics ranking                  # 全市場券商排名
uv run python -m broker_analytics query 1440               # 單一券商績效
uv run python -m broker_analytics query 1440 --breakdown   # 含個股明細
uv run python -m broker_analytics symbol 2330               # 個股 smart money signal（淨買超 × 個股PNL排名）
uv run python -m broker_analytics symbol 2330 --detail 5    # 近 5 日淨買超明細（daily_summary）
uv run python -m broker_analytics symbol 2330 --years 3     # 用 3 年滾動排名計算 signal
uv run python -m broker_analytics rolling                   # 全市場滾動 PNL 排名（pnl_daily 聚合）
uv run python -m broker_analytics rolling --years 2         # 指定窗口（預設 3 年）
uv run python -m broker_analytics rolling --xlsx            # 匯出 Excel
uv run python -m broker_analytics verify                    # 資料完整性驗證

# 事件研究（smart money accumulation → forward returns）
uv run python -m broker_analytics event-study 6285                          # 預設 top-20, 5d, 2σ
uv run python -m broker_analytics event-study 6285 --top-k 10 --window 10  # 調參數
uv run python -m broker_analytics event-study 6285 --threshold 1.5         # 降低門檻（更多事件）
uv run python -m broker_analytics event-study 6285 --no-robustness         # 跳過穩健性檢查

# 非合併版查詢（預設使用合併版，加 --no-merge 切回原始版）
uv run python -m broker_analytics ranking --no-merge       # 非合併版排名
uv run python -m broker_analytics query 1650 --no-merge    # 不含券商合併

# 個股信號分析
uv run python -m broker_analytics signal 2345              # 大單信號回測報告
uv run python -m broker_analytics signal 2345 --train-start 2023-01-01 --train-end 2024-06-30

# 全市場掃描（~7 分鐘）
uv run python -m broker_analytics scan                     # 預設：2億成交額、0.50%成本、1% FDR
uv run python -m broker_analytics scan --min-turnover 200000000 --cost 0.005 --fdr 0.01

# 信號匯出
uv run python -m broker_analytics export                   # 匯出信號 CSV
uv run python -m broker_analytics export --symbols 3665,2345

# 假說檢定框架（10 策略 × 可組合五步流水線）
uv run python -m broker_analytics hypothesis --list                      # 列出 10 策略
uv run python -m broker_analytics hypothesis 2330 -s contrarian_broker   # 單一策略
uv run python -m broker_analytics hypothesis 6285 --all                  # 全部 10 策略
uv run python -m broker_analytics hypothesis 2330 -s conviction --params top_k=30
uv run python -m broker_analytics hypothesis --batch 2330,2454 -s exodus --workers 4
uv run python -m broker_analytics hypothesis --scan -s conviction        # 全市場掃描 + FDR
uv run python -m broker_analytics hypothesis --scan --cv -s conviction   # 5-fold 滾動窗口 CV（推薦）
uv run python -m broker_analytics hypothesis --export -s conviction                    # 匯出 Signal Contract CSV
uv run python -m broker_analytics hypothesis --export -s "conviction,concentration"    # 多策略匯出
uv run python -m broker_analytics hypothesis --export -s conviction --hold-days 10     # 去重疊持倉
```

## Pipeline

```
~/r20/data/fugle/broker_tx/ (9 GB, per-day parquet, managed by ws-admin)
    │ etl.py
    ▼
data/daily_summary/{symbol}.parquet (4.2 GB, 2839 檔)
    │ pnl_engine.py (prices via ws-core → ~/r20/data/tej/prices.parquet)
    ▼
data/pnl_daily/{symbol}.parquet (12 GB, 2839 檔)  ← Layer 1.5：每日明細
data/fifo_state/{symbol}.parquet (289 MB, 2839 檔) ← FIFO checkpoint
    │ aggregate
    ▼
data/pnl/{symbol}.parquet (79 MB, 2839 檔)  ← 個股維度
data/derived/broker_ranking.parquet (56 KB)  ← 券商維度

    │ pnl_engine.py (default: merged, broker code remap → re-FIFO)
    ▼
data/pnl_daily_merged/  ← 合併版 Layer 1.5（預設）
data/pnl_merged/         ← 合併版個股維度（預設）
data/derived/broker_ranking_merged.parquet  ← 合併版券商維度（預設）
```

### Layer 0：供應商原始資料

`~/r20/data/fugle/broker_tx/` — per-day parquet 檔案（由 ws-admin 每日自動拉取），2021-01 ~ 2026-03。

### Layer 1：daily_summary/{symbol}.parquet

`etl.py` 將原始資料 streaming 聚合為每日摘要，按股票分檔。

| 欄位 | 型態 | 說明 |
|------|------|------|
| `broker` | Categorical | 券商代碼 |
| `date` | Date | 交易日期 |
| `buy_shares` | Int32 | 當日買入股數 |
| `sell_shares` | Int32 | 當日賣出股數 |
| `buy_amount` | Float32 | 當日買入金額 |
| `sell_amount` | Float32 | 當日賣出金額 |

排序：`broker, date`（FIFO 計算最佳化）

### Layer 1.5：pnl_daily/{symbol}.parquet + fifo_state/{symbol}.parquet

`pnl_engine.py` FIFO 逐日計算的中間結果，按股票分檔。支援滾動窗口 PNL 查詢和增量更新。

**pnl_daily/{symbol}.parquet** — 每日 PNL 事件

| 欄位 | 型態 | 說明 |
|------|------|------|
| `broker` | Utf8 | 券商代碼 |
| `date` | Date | 交易日 |
| `realized_pnl` | Float64 | 當日已實現損益 |
| `unrealized_pnl` | Float64 | 當日未實現損益（EOD mark-to-market） |

排序：`broker, date`。只存有交易或有持倉的 (broker, date)。所有日期（含 backtest_start 前）均保存，供滾動窗口回溯。

**fifo_state/{symbol}.parquet** — FIFO 持倉 checkpoint

| 欄位 | 型態 | 說明 |
|------|------|------|
| `broker` | Utf8 | 券商代碼 |
| `side` | Utf8 | "long" / "short" |
| `shares` | Int64 | 股數 |
| `cost_per_share` | Float64 | 成本價 |
| `open_date` | Date | 建倉日 |

Layer 2（pnl/ + broker_ranking）從 Layer 1.5 聚合導出。

### Layer 3a：pnl/{symbol}.parquet

從 Layer 1.5 聚合，輸出**個股維度**的排名。

| 欄位 | 型態 | 說明 |
|------|------|------|
| `rank` | UInt32 | 該股票內的 PNL 排名 |
| `broker` | String | 券商代碼 |
| `total_pnl` | Float64 | 該券商在該股票的總損益 |
| `realized_pnl` | Float64 | 已實現損益 |
| `unrealized_pnl` | Float64 | 未實現損益 |
| `total_buy_amount` | Float64 | 總買入金額 |
| `total_sell_amount` | Float64 | 總賣出金額 |
| `timing_alpha` | Float64 | 該股票的擇時能力 |

排序：`total_pnl DESC`

### Layer 3b：derived/broker_ranking.parquet

同一次 `pnl_engine.py` 執行，將所有個股結果聚合為**券商維度**的全市場排名。

| 欄位 | 型態 | 說明 |
|------|------|------|
| `rank` | UInt32 | 全市場 PNL 排名 |
| `broker` | String | 券商代碼 |
| `total_pnl` | Float64 | 總損益（已實現 + 未實現） |
| `realized_pnl` | Float64 | 已實現損益 |
| `unrealized_pnl` | Float64 | 最終未實現損益 |
| `total_buy_amount` | Float64 | 總買入金額 |
| `total_sell_amount` | Float64 | 總賣出金額 |
| `total_amount` | Float64 | 總成交金額 |
| `timing_alpha` | Float64 | 擇時能力 |

## 指標定義

### PNL（FIFO）

每個 **(股票, 券商)** 視為獨立帳戶，使用 FIFO 追蹤持倉：

- **賣出**：先平多倉（賣最早買的 lot），剩餘開空
- **買入**：先平空倉（回補最早的空單），剩餘開多
- **已實現**：平倉時鎖定的損益
- **未實現**：期末持倉以收盤價估值
- **總損益** = 已實現 + 未實現

邊界情況：無庫存先賣 → 開空倉；多翻空 → 先平多再開空；空翻多 → 先平空再開多。

### 回測窗口

- FIFO 從 **2021-01-01** 開始累積持倉歷史
- PNL 與 Timing Alpha 僅從 **2023-01-01** 起算
- 前兩年只建倉不計分，避免冷啟動偏差

### Timing Alpha

衡量擇時能力：前一天買超多的券商，隔天股價是否漲？

```
timing_alpha = Σ((net_buy[t-1] - avg_net_buy) × return[t]) / std(net_buy)
```

- 除以 `std(net_buy)` 正規化，消除交易量偏差（大量交易不會自動高分）
- **正值**：買超後漲、賣超後跌（擇時正確）
- **負值**：買超後跌、賣超後漲（擇時錯誤）
- 減去平均值排除方向偏好干擾（永遠買超不會自動高分）

存在於兩個維度：個股層級（`pnl/{symbol}.parquet`）和全市場層級（`broker_ranking.parquet`）。

### Smart Money Signal

給定一支股票，看「在這支股票上歷史績效最好的券商」現在站在買方還是賣方。

**計算方式**：

1. 取指定窗口（1/5/10/20/60 交易日）內各券商的淨買超
2. 淨買超 TOP 15 → 查他們在**該股**的 PNL 排名 → 加總 = 買方力道
3. 淨賣超 TOP 15 → 同理 = 賣方力道

**解讀**：

| 力道值 | 意義 |
|--------|------|
| 120（理論最小） | TOP 15 全是該股排名 1~15 的高手 |
| ~4,000 | 混合 |
| 13,650（理論最大） | TOP 15 全是排名最差的 |

買方力道遠低於賣方 → 在這支股票上賺過錢的人正在買入。

注意：使用的是**個股 PNL 排名**（`pnl/{symbol}.parquet`），不是全市場排名。在台積電排名第 1 的券商和在智邦排名第 1 的券商是不同的。

**`--detail N`** 顯示近 N 日各券商的淨買超明細，資料來自 `daily_summary`（交易量），不是 N-day rolling PNL（績效）。

**`--years N`** 改用 N 年滾動窗口計算個股 PNL 排名（從 `pnl_daily/{symbol}.parquet`），而非全期間排名。

### Rolling PNL Ranking（滾動 PNL 排名）

`broker_analytics rolling` 對全市場所有股票的 `pnl_daily` 做滾動窗口聚合，輸出券商全市場排名。

**計算方式**：

1. 設定窗口（預設 3 年 = 756 交易日）
2. 讀取所有 `pnl_daily/{symbol}.parquet`，過濾到 `[window_start, window_end]`
3. 每個券商：realized_pnl.sum()（窗口內流量）+ unrealized_pnl 變動（窗口末 − 窗口前）
4. window_pnl = realized + unrealized_change，按此排名

注意：`unrealized_pnl` 是存量快照（全部未平倉部位的 mark-to-market），不是流量。必須取窗口前後差值，否則排名被累計持倉灌水。

**與 Smart Money Signal 的差異**：

| | Rolling PNL (`rolling`) | Smart Money (`symbol`) |
|---|---|---|
| 資料來源 | `pnl_daily/`（全市場聚合） | `daily_summary`（淨買超）+ `pnl/`（個股排名） |
| 範圍 | 跨所有股票 | 單一股票 |
| 問什麼 | 券商最近 N 年全市場賺多少？ | 這支股票上的贏家現在在買還是賣？ |

`rolling --years 3` 和 `symbol 2330 --years 3` 都對 `pnl_daily` 做滾動窗口，邏輯相同，但範圍不同：前者聚合全市場，後者只看單一股票。

### Signal Report（大單信號分析）⚠️ 已封存

> **⚠️ 已封存 (2026-03-11)** — T+1 intraday alpha 在時區修正後不存在。
> 詳見 `docs/information_fragmentation_alpha.md`。

`broker_analytics signal` 對個股執行 4 步分析，輸出 `data/reports/{symbol}.md` + `.json`：

1. **大單偵測**：每個券商的 `|net_buy - mean| > 2σ` 為大單日
2. **統計驗證**：大單日 vs 非大單日的 return spread + t-test（< 5% 顯著 → early exit）
3. **TA 加權信號**：`signal[t] = Σ(TA_b × dev_b[t] / σ_b)`，TA 僅用 train period 計算（test |t| < 2 → early exit）
4. **回測**：open→close return，扣除 0.435% 交易成本，計算 Sharpe / MaxDD / Calmar

預設 train 2023-01~2024-06，test 2024-07~2025-12。開盤價從 ws-core 讀取。

### Market Scan（全市場信號掃描）⚠️ 已封存

> **⚠️ 已封存 (2026-03-11)** — 同上，T+1 intraday alpha 不存在。

`broker_analytics scan` 對全市場 ~2,400 支股票執行 6 層篩選 + 回測，使用 BH-FDR 控制多重檢定：

| 階段 | 說明 |
|------|------|
| F0a | 排除 ETF/ETN（代碼以 "00" 開頭） |
| F0b | 排除股票拆分/減資 |
| F0c | 排除資料不足（train < 30 天、test < 250 天） |
| F1 | 日均成交額 > 門檻（預設 2億 NTD，train period） |
| F2 | 顯著正向券商 > 5% |
| F3 | Benjamini-Hochberg FDR < 1% |

通過 F3 的股票執行完整回測（0.50% 保守成本），輸出 `data/derived/market_scan.json` + `.md`，以及每支個股的 `data/reports/{symbol}.json`。

兩階段平行架構：Phase 1 篩選（12 workers），FDR 校正後 Phase 2 批次拉取 OHLC + 回測。

### Event Study（聰明錢事件研究）

`event-study` 子命令對個股執行事件研究，檢驗 PNL top-K 券商的個別大單（per-broker 2σ）是否預測中期報酬：

1. **事件偵測**：使用 rolling PNL ranking（每天只用過去 3 年的 PNL，避免 2021-2022 持倉建置期噪音），偵測 top-K 券商的個別異常大單（per-broker |net_buy - mean| > 2σ），累積計數超過門檻 → accumulation / distribution 事件
2. **門檻校準**：分析 per-broker z-score 分佈形狀（偏態、峰態），確認 2σ 門檻的實際觸發百分位
3. **方向分拆**：accumulation（大單買超）和 distribution（大單賣超）獨立分析，避免方向對沖稀釋信號
4. **統計檢定**：Permutation test（10,000 次）取代 Bonferroni，搭配 Cohen's d 效果量
5. **SCAR 跨股票池化**：標準化後跨股票合併，解決 per-stock 樣本不足問題
6. **衰減曲線**：逐日 direction-adjusted CAR，判斷 alpha 消化速度與最佳持倉期
7. **穩健性**：Placebo test（隨機券商取代 top-K）

### Hypothesis Testing（可組合假說檢定框架）

`broker_analytics hypothesis` 提供可組合的五步流水線，驗證各種券商行為假說：

```
Selector → Filter → Outcome → Baseline → StatTest
(選券商)   (篩事件)   (量報酬)   (做基準)   (跑統計)
```

每步是純函數，策略只是「五個函數的組合 + 參數」，新增策略零改框架。

**11 策略**（5-fold CV + bias fix + beta 分離 + dedup + stocks-only）：

| # | 策略 | CV | 10d Excess Sharpe | Trades | 說明 |
|---|------|----|-------------------|--------|------|
| 10 | `momentum_conviction` | **4/5** | **6.22** ✅ | 195 | 排名躍升 broker × conviction（⚠ 樣本小） |
| 3 | `conviction` | **4/5** | **3.99** ✅ | 1,658 | 績優券商浮盈 >20% 仍加碼 |
| 8 | `concentration` | **4/5** | **3.08** ✅ | 414 | 持倉高度集中的券商加碼 |
| 1 | `contrarian_broker` | **5/5** | — | — | 全市場差但個股強（*共用 conviction filter） |
| 2 | `dual_window` | **4/5** | — | — | 1yr ∩ 3yr PNL 交集（*共用 conviction filter） |
| 7 | ~~`contrarian_smart`~~ | 5/5 | -0.20 ❌ | — | CV 通過但去重後 alpha 消失 |
| 4 | ~~`exodus`~~ | 3/5 | -0.15 ❌ | — | 波動率信號，非方向性 |
| 9 | ~~`herding`~~ | ❌ 1/5 | — | — | selector bias 修正後崩掉 |
| 0 | ~~`large_trade_scar`~~ | ❌ | — | — | 假說不成立 |
| 6 | ~~`ta_regime`~~ | ❌ | — | — | 事件太稀疏 |
| 5 | `cross_stock` | ⏭ | — | — | 需 cluster 定義 |

Excess Sharpe = stocks-only（排除 ETF/warrant/REIT）+ 扣大盤 + dedup 10d。
獨立 alpha 來源：momentum_conviction + conviction + concentration（3 個獨立）。
詳見 `docs/harshreview.md` 和 `CLAUDE.md`。

```bash
# 策略分析（post-hoc）
uv run python -m broker_analytics analyze                              # 全策略 beta 分析
uv run python -m broker_analytics analyze -s conviction                # 單策略 beta 分析
uv run python -m broker_analytics analyze --tag deduped                # 去重版 beta 分析
uv run python -m broker_analytics hypothesis --strength -s conviction  # 信號強度 quintile 分析
```

## 效能

M3 Pro 12 核，全市場 2,839 股 × 917 券商 × 1,209 交易日：

| 階段 | 時間 | 記憶體 | Big-O |
|------|------|--------|-------|
| ETL | ~10 min | ~2 GB (streaming) | O(N)，N=20.8億 |
| PNL 計算 + Layer 1.5 | ~6 min | ~10 MB/核 | O(S×B×T)，12核並行 |
| 查詢 | 0.01s | 1 MB | O(B log B)，預聚合表 |
| 全市場掃描 | ~7 min | ~100 MB | O(S×B×D)，12核並行 |

全程記憶體峰值 ~2 GB，遠低於 36 GB 可用記憶體。

## 設計決策

### FIFO vs 加權平均成本

| 方法 | 做法 | 結果 |
|------|------|------|
| 加權平均 | 所有股票成本相同 | 已實現損益較平滑 |
| **FIFO** | 先買先賣 | 符合實際交易邏輯 |

範例（買 100@$10，再買 100@$20，賣 100@$25）：
- 加權平均：100 × ($25 - $15) = **$1,000**
- FIFO：100 × ($25 - $10) = **$1,500**（賣最早買的）

產品需求選擇 FIFO。

### By Symbol Parquet vs Dense Tensor

| 方案 | 存儲 | 記憶體 | 查詢 |
|------|------|--------|------|
| Dense Tensor | 26 GB | 26 GB (OOM) | O(S×T + B log B) |
| **By Symbol + 預聚合** | ~4.2 GB | ~5 MB | **O(B log B)** |

選擇 By Symbol：Tensor 放不進記憶體，且預聚合後查詢更快、支援增量更新。

### Pre-partition 並行策略

`price_lookup` 有 260 萬筆，直接傳給 12 個 worker 需 pickle 序列化 12 次，IPC 開銷遠大於計算。解法：按 symbol 預分割，每個 worker 只收 ~1,200 筆。

## 外部依賴

### ws-core

價格資料統一透過 `ws-core` 讀取 `~/r20/data/tej/prices.parquet`（由 ws-admin 每日自動更新）。
