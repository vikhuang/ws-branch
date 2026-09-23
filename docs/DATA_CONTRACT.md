

> **2026-09-23 現況**：ws-quant 已是正式消費端，透過 `ws-branch.datasets.v1`
> 匯出八類 dataset（含分價量、universe、state、salience），不互相 import。
> 研究與治理由 quant 發起；探索最近一週用其 `BranchDataClient(purpose="exploration")`。
> 未完成：T4 新鮮度（09-23 測試缺 09-16～18）、增量／排程、export 與讀本/profile
> 的歷史範圍一致性（P2）、完整研究案例驗收。A9 已於 09-22 結案。
> 全局現況見 ws-quant
> `docs/research_platform_status_2026-09-23.md`。


# ws-branch 對外資料契約

ws-branch 是分點資料與通用量測的 provider。研究問題、報酬檢驗與資料 tier
治理留在 ws-quant；兩個 repo 不互相 import，以 Parquet 與 receipt 交換資料。

機器可讀的權威契約是 `ws-branch.datasets.v1`，隨 package 發布於
`src/ws_branch/contracts/datasets.v1.json`。契約逐資料集定義粒度、鍵、欄位單位、
universe、缺值與資料可用時間。新增欄位可以相容發布；刪欄、改單位、改粒度或
改變缺值語意必須升 contract major version。

## 查契約

```bash
uv run python -m ws_branch contract
uv run python -m ws_branch contract --dataset t2_broker_pricelevel
```

公開資料集為：

| dataset | 粒度 | 來源 |
|---|---|---|
| `t1_broker_daily` | 分點×股票×日 | 物化表 |
| `t2_broker_pricelevel` | 分點×股票×日×價位 | ws-core canonical view，按需匯出 |
| `t3_official_daily` | 股票×日 | 物化表 |
| `t3b_accounting_bounds` | 股票×日×買賣側 | 物化表 |
| `t4_broker_measure` | 分點×日 | 物化表與年度 manifest |
| `universe_daily` | 股票×日 | 與 T4 相同的普通股研究母體 |
| `seat_state` | 分點×日 | provider 端用完整 T4 歷史計算 trait/day/state |
| `salience_pair` | 分點×股票×日 | provider 端以完整母體與固定歷史範圍計算的 salience 三問 |

## 匯出

```bash
uv run python -m ws_branch export \
  --dataset t2_broker_pricelevel \
  --start 2025-01-02 --end 2025-01-31 \
  --symbol 3450 \
  --output /tmp/3450_pricelevel.parquet
```

`salience_pair` 是計算型資料集，同樣使用 `export`：必須給至少一個 `--symbol`。
provider 會從已公告的 `history_start` 讀到查詢終日，套用普通股 universe，並以
T4 的完整席位日總額作分母；輸出仍只含查詢區間。這使同一目標日不因查詢起日
不同而改變。consumer 不應在查詢子集內重算。

每次匯出同時產生 `<output>.receipt.json`，記錄：

- contract 版本、內嵌 contract snapshot、contract hash 與 provider commit；
- 所需年度是否齊全，以及可用時的上游年度 manifest、measurement version 與 source snapshot；
- 完整查詢範圍；
- 實際列數與日期範圍；
- Parquet SHA-256；
- universe、缺值及可用時間語意。

輸出檔已存在時預設失敗；明確傳入 `--force` 才會覆寫。`--symbol`、`--broker`
可以重複指定，但資料集沒有該維度時直接報錯。

## 語意邊界

- T1/T2 的量是股；tick/order-book 的量是張。
- T2 排除 `price="-"` 彙總列。完整分點日股數以 T1 為準。
- T1/T2 是原始可觀測代號範圍；研究普通股時必須使用逐日 universe gate。
- T3 缺列不能當零；T3b 只發布 `bounds_publishable=true` 的界限。
- T4 cosine 是配置描述量，不是投資人身分機率。
- 子集查詢不得改變量測母體。需要全市場分母、同日截面或暖機窗的量，由
  ws-branch 計算後輸出，不讓 consumer 用查詢子集重算。
- receipt 描述目前重建所得的資料。若沒有歷史版本快照，不能宣稱它證明資料在
  當時確實已經到達；`availability` 會保留這項限制。

## 目前使用限制（2026-09-23）

- `export` 的 salience/state 固定從 2025-01-02 讀歷史；讀本/profile 仍用 150 日曆日。
  稀疏席位可能有不同 baseline／pair 清單；P2 尚未統一，不能宣稱所有入口結果相同。
- 全歷史依賴照實交由 consumer 治理；quant 的 exploration 允許接觸並記帳，
  validation 仍可能因依賴包含 holdout 被擋。
- 近期查詢須先通過 source coverage；09-23 測試發現 T4 缺 09-16～18，尚未補建。
- 非所有 source_manifests 都有內容 hash；部分資料以 path/size/mtime 識別。
  receipt 不是歷史資料版本已封存或當時實際到達時間的證明。
