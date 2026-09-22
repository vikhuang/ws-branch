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

## 匯出

```bash
uv run python -m ws_branch export \
  --dataset t2_broker_pricelevel \
  --start 2025-01-02 --end 2025-01-31 \
  --symbol 3450 \
  --output /tmp/3450_pricelevel.parquet
```

每次匯出同時產生 `<output>.receipt.json`，記錄：

- contract 版本、contract hash 與 provider commit；
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
