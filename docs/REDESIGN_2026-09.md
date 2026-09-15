# ws-branch 重設計提案（2026-09-15，待 user 審後動工）

> 一句話：ws-branch 從「分點 PNL 回測系統」升格為「**分點資料的量測與歸因中心**」，
> 承接 ws-quant 這一年長出來的分點研究，產出標籤表與訊號檔給下游用。
> 本文件是施工前的藍圖，審過才動工。

## 0. 決策紀錄（user 已拍板，2026-09-15）

1. 分點相關研究歸 ws-branch；ws-quant 只留策略與回測本業
2. 標準資料讀取器放 ws-core（全生態共用一把尺）
3. 分群/身分規則整包從 ws-quant 搬來；ws-quant 改讀 ws-branch 產出的標籤表
4. 先寫本文件，user 審過再施工；全程在 worktree（branch `redesign`）

## 1. 為什麼要重設計（三個事實）

1. **分點研究長錯地方了。** ws-quant 的處置實驗資料夾裡塞了一整套全市場分點
   研究（散戶化剖面、官方對帳、身分規則），跟處置無關，是借住。
2. **「做過」不等於「做對」。** 這一週抓到的三個案子：
   - 身分規則的自營桶用了近一年，實測是空殼（只蓋到 0.06% 的量）
   - conviction 訊號「Sharpe 5-8」缺 IS/OOS 分段與 beta 拆離，數字目前只是傳聞
   - 分群文件說散戶密集群佔市場 89%，實算 53%——矛盾沒人追過
3. **本 repo 休眠中。** 資料凍在 2026-03，重資料層已被清掉，復活 = 全量重跑。

## 2. 邊界（三專案分工）

| 專案 | 管什麼 | 不管什麼 |
|---|---|---|
| **ws-core** | 怎麼「正確地讀」每種原始資料（單位/髒列/陷阱全封在裡面）| 不做任何研究判斷 |
| **ws-branch** | 看懂分點：身分（誰在用這櫃檯）、技巧（歷史賺不賺）、狀態（這批錢走到哪一步）；產標籤表與訊號檔 | 不做回測定生死、不碰策略上線 |
| **ws-quant** | 拿訊號驗證賺不賺（回測、治理、上線裁決）| 不再自己養分點量測邏輯 |

**溝通規矩：不互相 import 程式碼，只交換檔案。**
先例 = ws-quant↔ws-trade（spec YAML + CSV）、ws-branch→ws-quant 7 訊號 CSV 回測（2026 已走通）。

## 3. 新架構（四層）

```
L0 讀資料   ws-core 標準讀取器（本 repo 自己的 etl.py 逐步廢掉）
             broker_tx(2021+) / tej_shareholding(千股!) / tdcc / 主動ETF
L1 量測     三個獨立模組,互不 import:
             identity/  身分:名字規則+行為分群+官方對帳(外資0.97先例)
             skill/     技巧:FIFO PNL、timing alpha、rolling ranking(現有搬入)
             state/     狀態:新進場/連買天數/反手/擁擠度/價格積極度(新建)
L2 產品     每日產出,檔案交付:
             broker_labels.parquet   每分點:身分機率+技巧分位+狀態旗標
             flow_states.parquet     每股每日:五條 latent flow + 生命週期
             signals/*.csv           Signal Contract 格式(既有規格)
L3 研究     experiments/ + 家法(從 ws-quant 移植):
             預期先行 / 獨立紅隊重算 / trial ledger / 體檢先於使用
```

現有的 Clean Architecture（domain/infrastructure/application/interfaces）保留，
L1 三模組落在 domain+application；L0 換成 ws-core 是 infrastructure 的置換。

## 4. 搬遷清單

**從 ws-quant 搬來（連同體檢義務）**
- `src/broker_taxonomy/`（四群分群、簽名 pair、虎尾幫名單）
- `src/data_layer/broker_loader.py` 的身分規則（含已知 bug：自營空殼）
- 本週對帳與量測腳本：u1/u1b/u2/u3 系列（散戶化剖面、官方對帳）
- 對應的 findings 與 frontier 記錄（複本；ws-quant 原檔留存為歷史）

**留在 ws-quant**
- 處置策略線全部（v2、H-20260911、生態圖鑑報告）
- 回測引擎、治理層、experiments 秩序

**過渡期相容**：ws-quant 處置線腳本 import 的 `src.broker_taxonomy` **凍結不動**，
直到 broker_labels.parquet 上線且處置線改讀標籤表，才刪舊模組。不做大爆炸式切換。

## 5. 舊資產體檢表 v0（重驗優先於新挖；★=施工前必辦）

| 資產 | 聲稱 | 已知裂縫 | 體檢動作 |
|---|---|---|---|
| ★ conviction 等 11 策略 | 10-60d Sharpe 5-8 | 無 IS/OOS 分段、60d 未拆 beta | 復活資料後首件事：分段+beta 拆離重跑 |
| ★ 分群 vol_share | C0+C3=89% 市場量 | 與實算 53% 矛盾 | 查口徑（母體/單位/分母），寫結論 |
| 身分規則 | 五類 cohort | 自營桶空殼（U3 已證） | prop 桶標記棄用；HQ 改讀「法人混合通道」 |
| 四群分群 | 806 分點行為分群 | 建於單一窗口，未測時間穩定性 vs 官方錨 | 對 tej_shareholding 錨重驗+標註有效期 |
| FIFO PNL | 每(股,分點)損益 | HQ 席位=混合通道，PNL 非單一主體 | 文件加註;外資席位(0.97)可信,HQ 打折 |
| 簽名 pair 40 組 | 100% persistent | 2026 新制/大型化 regime 未重驗 | 滾動重驗 persistence |
| T+1 日內線 | 已封存(時區 bug) | — | 維持封存,墓碑保留 |

## 6. ws-core 新增讀取器（另開小 PR 到 ws-core）

| 讀取器 | 封進去的陷阱 |
|---|---|
| broker_tx | 單位=股;price 千分位逗號(<2026-05-29 高價股);"-"彙總列 coalesce;2021+ 全史 |
| tej_shareholding | **單位=千股**(U3 實測);三大法人 buy/sell gross 語意 |
| tdcc_distribution | 週頻;級距 schema;epoch 時區 |
| etf_holdings | 主動 ETF 每日 JSON→表;申贖 vs 調倉不可分的但書 |

每個讀取器附**對帳測試**當迴歸防線：broker_tx 總量==TEJ vol（分毫不差先例）、
外資席位 vs qfii corr>0.95（0.97 先例）。

## 7. 施工順序（估 1-2 週,每階段可獨立驗收）

| 階段 | 內容 | 驗收 |
|---|---|---|
| P1 | ws-core 四個讀取器+對帳測試 | 測試綠;兩條對帳先例重現 |
| P2 | 復活資料層:用 ws-core 讀取器重跑 daily_summary+FIFO 到當下 | verify 通過;資料到最新交易日 |
| P3 | 體檢表 ★ 兩案(conviction 重驗、89/53 矛盾) | 各出一頁結論,更新第 5 節 |
| P4 | 搬遷:taxonomy/身分規則/u 系列入 L1;identity 模組成形 | ws-branch 內測試綠;ws-quant 原模組凍結未動 |
| P5 | broker_labels.parquet v1 上線(身分+技巧+基本狀態) | schema 定稿;ws-quant 處置線試讀成功 |
| P6 | L3 研究區開張:家法文件+第一批假說(NNLS 歸因/價格積極度/主動ETF對帳) | 假說各有凍結預期才准跑 |

P1-P2 是地基,P3 是還債,P4-P5 是搬家,P6 才是新研究。**順序不可倒。**

## 8. 風險

- **兩套讀取邏輯並存期**（舊 etl.py vs ws-core）:以對帳測試互驗,P2 完成即廢舊
- **ws-quant 過渡期斷鏈**:靠「凍結舊模組直到標籤表上線」保護
- **範圍蔓延**:P6 的新假說清單已存在誘惑,家規=P1-P5 沒驗收前不開新題
- 資料量:全量重跑 ETL+FIFO 約 15-20 分鐘/次,非風險僅提醒

## 9. 本文件的下一步

user 審閱 → 修訂 → 核准後按 P1 開工。P1 動的是 ws-core(另開 worktree/PR),
本 repo 從 P2 起動工,全程留在 branch `redesign`,驗收後才併 main。
