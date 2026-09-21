# Step F 消費端盤點(2026-09-21)

規格:ws-quant `docs/actor_layer_v3_architecture_alignment_2026-09-18.md` §11 F「消費端
改讀新版本,退役舊欄位的現行產出;保留必要舊檔與實驗引用,不原地覆蓋舊語意」、
§12 完成標準 5「舊產品依賴完成盤點及版本遷移驗證;O2b 不再依賴 multiplicity,
O4 不再承諾未經驗證的身份機率」、§10.2「如 ws-quant 仍消費 broker_labels.parquet,
先盤點實際欄位用途」。

盤點範圍:ws-quant(主 repo + worktree)、ws-mcp、ws-desk、ws-admin、ws-core、
ws-broker-flow、ws-branch 自身(程式碼 / YAML / docs / cron / shell 全查)。

## 1. 結論

**ws-branch 的 T1–T4 與 v2/v3 量測欄位在所有外部 repo 是 0 個程式引用。**
「消費端遷移」沒有真實消費端要遷;§12-5 的「版本遷移驗證」對外部是空集合。
需要動作的只有三類,都不是「改讀」:

| 類 | 內容 | 歸屬 | 動作 |
|---|---|---|---|
| a | ws-quant `src/broker_taxonomy/` 是**平行的舊實作**(讀自家 `experiments/broker_taxonomy/*.parquet`,不讀 ws-branch),CLAUDE.md 稱「凍結至 ws-branch 標籤表上線」 | ws-quant | 拆它是刪碼不是改讀;且 `broker_labels` **ws-branch 自己也還沒實作**(`products/__init__.py` 只有宣告)。**現況:無急迫下游**(dispo-rightarm-v2 / sr 已 retired) |
| b | ws-desk `panel_broker.py` 直讀 raw broker_tx,沒有讀本/profile | ws-desk | 若要接讀本是**新增消費端**,非遷移 |
| c | ws-admin `catalog.yaml:2253` 把 ws-branch 登記為 `kind: consumer, excluded: true`,「T1–T4 產物尚未成為登記 dataset」 | ws-admin | 未登記 = 沒有正式契約;要有外部消費端之前先登記 `t4_broker_measure` / `t3b_accounting_bounds`(schema、manifest 欄位、available_at 規則) |

順手可清:ws-admin `workflows/smoke-test.yaml:12` 指向 2026-04-29 已刪的 Cloud Run
job `ws-branch-smoke`。

## 2. 外部 repo 逐一

| repo | 讀 ws-branch 什麼 | 判定 |
|---|---|---|
| ws-quant | 無。`scripts/daily/*`、dispo 線、`src/broker_taxonomy` 全部直讀 raw(`fugle/broker_tx`、`disposition`、`tej/prices`)或自家 parquet;`ROADMAP.md:53-61` 的「ws-branch conviction CSV → backtest-external-daily」狀態 ○ 未開工;`f9_conviction.parquet` 是同名自產物 | **零依賴** |
| ws-mcp | L2 `broker_flow_daily` 生產者是獨立 repo **ws-broker-flow**(自 raw 算),ws-mcp 經 ws-core 讀該目錄;registry 無 branch source | **零依賴** |
| ws-desk | `views/panel_broker.py` 直讀 raw broker_tx 分片;無 readbook/profile 字串 | **零依賴** |
| ws-admin | 反向:上游 dataset 的 `consumers: [ws-branch]`;`migrate_broker_tx.py` / `verify_fugle_api.py` 讀的是 legacy 合併 raw 檔(一次性,已完成) | **零依賴**(T1–T4 未登記) |
| ws-core | `tests/test_flow_readers.py:31` 註解提「身分規則歸 ws-branch 管」 | 註解 |

零引用的產出/欄位(六個 repo 全查):`t1_broker_daily`、`t3_official_daily`、
`t3b_accounting_bounds`、`t4_broker_features`、`t4_broker_measure`、`multiplicity`、
`foreign_sim*`、`fund_sim*`、`sector_hhi`、`daytrade_assoc`、`basket_self_sim`、
`WS_BRANCH_DATA_DIR`、`data/reports/`、`broker_merge_map`、`pnl_daily`、`readbook`、
`broker_profile`、`broker_labels`(未實作)、`broker_ranking`、`smart_money`(無此字串)。

## 3. ws-branch 內部消費端(v2 → v3 的實際遷移面)

| 讀者 | 讀什麼 | 處置 |
|---|---|---|
| `scripts/observatory/v3_readbook.py`、`v3_broker_profile.py` | T1、T3、T3b、**T4 v3** | ✅ 已在 v3 |
| `experiments/flow_lab/v3_*.py`、`v3_common.py` | T1/T3/T3b/**T4 v3**(09-21 起不讀 /tmp 快取) | ✅ |
| `scripts/observatory/o1_stock_readout.py`、`o1_foreign_sim_anchor.py` | **T4 v2**(multiplicity、rank 版 foreign_sim) | **O1 歷史探索腳本,標 legacy 保留**(規格:舊版保留供重現),不遷 |
| `experiments/flow_lab/o1_fingerprint_*.py` | T4 v2 | 同上 |
| `tests/test_e2e.py`、`test_transforms.py` | 五張表 | v2 的 e2e 手算保留(重現契約) |
| 舊產品線 `run.sh` → `pnl/*`(唯一 launchd 排程,22:30) | raw broker_tx + 自家 pnl 目錄 | **不讀 T1–T4**;且 `data/daily_summary`、`data/pnl_daily`、`data/pnl_daily_merged` 磁碟上不存在 → 排程實際上已無產出,屬封存 |

**T1–T4 建表與 observatory 腳本沒有任何排程,全部手動**——這是 Step F 之後的
營運缺口(`--incr` + 排程),不是遷移缺口。

## 4. §12-5 逐條

| 要求 | 狀態 |
|---|---|
| 舊產品依賴完成盤點 | ✅ 本文 |
| 版本遷移驗證 | 外部空集合;內部 v3 產品已改讀 T4 v3,O1 腳本標 legacy |
| O2b 不再依賴 multiplicity | ✅ OBSERVATORY 第 202-203 行已取消該依賴;multiplicity 不在 T4 v3 |
| O4 不再承諾未經驗證的身份機率 | ✅ profile / 讀本只有行政 cohort + 校準卡;`identity.py` 的 `_FOREIGN_EXACT` 與 confidence 常數仍在 measure/ 供舊 O1 腳本,**不進任何 v3 產品** |
| §10.2 `broker_labels.parquet` 欄位用途盤點 | 不適用:該檔從未產出、無人消費 |

## 5. T4 v2 的退役方式(本 commit)

- registry 標 `t4_broker_features` 為 **frozen**:`build` 仍可(重現用)但印警語;不再
  隨年份更新;verify 不變。
- 檔案 `data/t4_broker_features/year=2026.parquet` 保留;不原地覆蓋、不改欄名語意。
- 新消費端一律接 `t4_broker_measure`(+ manifest)。

## 6. 給 user 的決定

1. ws-admin `catalog.yaml` 是否登記 `t4_broker_measure` / `t3b_accounting_bounds` 為
   dataset(需先定 schema 契約與 owner);同時清掉 `smoke-test.yaml` 殘骸。
2. ws-quant `src/broker_taxonomy` 何時拆:它不讀 ws-branch,拆與否只取決於 ws-quant
   是否還要那套 v1 分群(dispo 線已 retired)。
3. ws-desk 要不要接讀本/profile(新增消費端)。
4. T1–T4 排程化 + `--incr`(營運面)。
