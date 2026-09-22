# ws-branch 重設計提案 v2（2026-09-15，待 user 審後動工）

> 一句話：ws-branch 的新核心是**把分點資料清洗聚合成一組乾淨、對過帳的表**，
> 供歸因研究（分點流量是誰的錢、什麼狀態）與下游（ws-quant/ws-desk)使用。
> PNL/FIFO 整層降為封存件。本文件是施工前的藍圖,審過才動工。

## 0. 決策紀錄（user 拍板）

- 2026-09-15 上午:①分點研究歸 ws-branch;②標準讀取器放 ws-core;
  ③分群/身分規則從 ws-quant 搬來,ws-quant 改讀產出檔;④先文件後施工,全程 worktree
- 2026-09-15 v2 修訂:**PNL 不是核心**——長期無人使用,招牌成果缺關鍵驗證,
  且 U3 已證總公司席位是混合通道(對它算單席位損益答非所問)。
  ws-branch 先聚焦**清洗並聚合資料**;歸因研究(另一 LLM 提案的可取部分)
  蓋在乾淨資料之上,PNL 降級封存(不刪,復用前須補 IS/OOS+beta 檢查)

## 1. 為什麼重設計（四個事實）

1. 分點研究長錯地方:全市場分點研究借住在 ws-quant 的處置實驗資料夾
2. 「做過」≠「做對」:自營桶空殼用了近一年;conviction「Sharpe 5-8」缺
   IS/OOS 分段與 beta 拆離;分群文件 89% vs 實算 53% 矛盾無人追
3. 本 repo 休眠:資料凍在 2026-03,重資料層已清空
4. 價值實測:PNL 層算了 12GB,user 一年未用過——使用率就是價值的答案

## 2. 邊界（三專案分工）

| 專案 | 管什麼 | 不管什麼 |
|---|---|---|
| **ws-core** | 怎麼「正確地讀」每種原始資料(單位/髒列/陷阱全封在裡面) | 不做研究判斷 |
| **ws-branch** | 分點資料的**清洗、聚合、對帳**,及其上的歸因量測;產標籤表與訊號檔 | 不回測定生死、不碰上線 |
| **ws-quant** | 拿訊號驗證賺不賺(回測、治理、上線裁決) | 不再自己養分點量測 |

**溝通規矩:不互相 import 程式碼,只交換檔案**(先例:ws-quant↔ws-trade;
ws-branch 7 訊號 CSV→ws-quant 回測,2026 已走通)。

## 3. 核心產品:四張表(schema 先行,由第一批假說反推,防清洗失焦)

| 表 | 粒度 | 內容 | 服務誰 |
|---|---|---|---|
| **T1 主表** | 分點×股票×日 | 買/賣股數與金額(canonical 清洗:單位、逗號、"-"列 coalesce) | 所有下游 |
| **T2 價位表** | 分點×股票×日×**價位** | 分價買賣量——**不壓扁**。這是獨有優勢欄位,多數籌碼站直接丟棄 | 價格積極度(BuyLocation)、撮合指紋 join |
| **T3 官方對齊表** | 股票×日 | 外資/投信/自營官方買賣 gross(千股→股)、集保週級距、主動 ETF 持倉變化 | 歸因模型的「答案卷」 |
| **T4 特徵表** | 分點×日 / 分點×股票×日 | 方向比 \|B−S\|/(B+S)、連買/連賣天數、gross、集中度、籃子向量、累積流量 | 分群重做、歸因、狀態量測 |

**每張表都帶自動對帳測試**(迴歸防線,錯了自己叫):
- T1 總量 == TEJ 成交量(分毫不差,已有先例)
- T1 外資席位 vs T3 qfii corr > 0.95(0.97 先例)
- T2 聚合 == T1(內部一致性)

## 4. 架構(四層,PNL 移出主線)

```
L0 讀資料   ws-core 標準讀取器(本 repo 的 etl.py 逐步廢掉)
L1 資料廠   四張表的建造與對帳(新核心;Clean Architecture 的 domain+application)
L2 量測     蓋在四張表上,依序:
             identity/  身分:名字規則+行為分群重做+官方對帳
             state/     狀態:新進場/連買/反手/擁擠/價格積極度
             attribution/ 歸因:NNLS/籃子指紋 → 五條 latent flow(外資型/投信型/
                          自營型/本土集中型/散戶擴散型)——研究題,過家法才進產品
L3 產品     broker_labels.parquet(每分點:身分機率+狀態旗標)
             flow_states.parquet(每股每日:latent flow+生命週期)
             signals/*.csv(Signal Contract,給 ws-quant 回測)

[封存] pnl/(FIFO/timing alpha/聰明錢):archive/ 保留可讀,主線不維護。
       復用前置條件:IS/OOS 分段+beta 拆離重驗,且先說清楚要回答什麼問題。
```

## 5. 搬遷清單

**從 ws-quant 搬來(連同體檢義務)**:`src/broker_taxonomy/`、
`broker_loader.py` 身分規則(含自營空殼 bug 註記)、u1/u1b/u2/u3 系列腳本與
findings(複本,原檔留 ws-quant 作歷史)。
**留在 ws-quant**:處置策略線全部、回測引擎、治理層。
**過渡期相容**:ws-quant 的 `src.broker_taxonomy` 凍結不動,直到
broker_labels.parquet 上線且處置線改讀成功,才刪舊模組。不做大爆炸切換。

## 6. 舊資產體檢表 v0(重驗優先於新挖;★=主線必辦)

| 資產 | 聲稱 | 已知裂縫 | 體檢動作 |
|---|---|---|---|
| ★ 分群 vol_share | C0+C3=89% 市場量 | 與實算 53% 矛盾 | 查口徑,寫一頁結論 |
| ★ 身分規則 | 五類 cohort | 自營桶空殼(U3 已證) | prop 桶棄用;HQ 改「法人混合通道」語意 |
| ★ 四群分群 | 806 分點行為分群 | 單一窗口建成,未驗時間穩定性 | 在 T4 上重做,對 T3 官方錨驗證 |
| 簽名 pair 40 組 | 100% persistent | 2026 新制/大型化未重驗 | 滾動重驗 persistence |
| conviction 等 11 策略 | 10-60d Sharpe 5-8 | 無 IS/OOS、未拆 beta | **隨 PNL 封存**;復用時才補 |
| T+1 日內線 | 已封存(時區 bug) | — | 維持封存 |

## 7. ws-core 新增讀取器(另開 PR 到 ws-core)

| 讀取器 | 封進去的陷阱 |
|---|---|
| broker_tx | 單位=股;price 千分位逗號(<2026-05-29 高價股);"-"彙總列 coalesce;2021+ 全史 |
| tej_shareholding | **單位=千股**(U3 實測);三大法人 buy/sell gross 語意 |
| tdcc_distribution | 週頻;級距 schema;epoch 時區 |
| etf_holdings | 主動 ETF 每日 JSON→表;申贖與調倉不可分的但書 |

## 8. 施工順序(估 1-1.5 週;順序不可倒)

| 階段 | 內容 | 驗收 |
|---|---|---|
| P1 | ws-core 四讀取器+對帳測試 | 測試綠;兩條對帳先例重現 | **✅ 2026-09-15,ws-core v0.10.0 已 ship** |
| P2 | T1/T2 建表(2021+ 全史)+對帳 | 閉環(修正語意:broker ⊆ TEJ,A5) **✅ 2026-09-15;2021-23 需 BQ 補驗** |
| P3 | T3/T4 建表;還債:★三案體檢 | 特徵表就緒;三案各一頁結論 | **✅ 2026-09-15(帳本 A1-A5 結案)** |
| P4-P6 | **重心由觀測站取代**:四層維度(行動者/部位/執行/關係)分期 O1-O6,詳 `docs/OBSERVATORY_2026-09.md`(2026-09-16) | 各期驗收見該文件 |
| ~~P4~~ | ~~搬遷+identity 重做~~(併入 O1) | |
| P5 | broker_labels.parquet v1 上線 | **09-22:未實作,且 Step F 盤點證無消費端**(ws-quant 處置線不讀 ws-branch;dispo 線已 retired)——降為待需求 |
| P6 | 歸因研究開張(NNLS/籃子指紋/BuyLocation/主動ETF對帳),家法全套 | 已歸 O6;席位歸因另立 v3.5(未開始)。**v3 Phase 1-4 已完成**(OBSERVATORY §10) |

## 9. 風險

- 清洗失焦 → 已用「schema 先行、假說反推」鎖住(第 3 節)
- 兩套讀取邏輯並存期 → 對帳測試互驗,P2 完成即廢舊 etl.py
- ws-quant 過渡期斷鏈 → 凍結舊模組保護
- 範圍蔓延 → P6 之前不開新假說;PNL 不得悄悄復活

## 10. 下一步

user 審 v2 → 核准後 P1 開工(ws-core 另開 worktree/PR)。
本 repo 從 P2 起動工,全程 branch `redesign`,驗收後併 main。

## 11. 施工補記(2026-09-15)

- 架構全面重排(user 核准):單一套件 `src/ws_branch/` 三色分層
  (transforms/checks/measure=純;io/products=IO;registry/taxonomy=宣告);
  PNL 時代整包物理封存於 `pnl/`(原名 legacy,user 定名)。
- 三次記憶體事故與條文化修法:①並行 OOM→序列執行;②單程序多年建表
  polars 不還記憶體堆 94GB 壓縮頁→一年一子程序;③verify 全史 unique
  30GB→逐年抽日+謂詞下推。教訓:36GB 機器上 RSS 是假象,**壓縮區才是
  真水位**,任何億級查詢先按壓縮區預算設計。
  **④(2026-09-21)T1 整年 group_by**:group key 含 date = raw 分片,聚合從不
  跨檔,但整年 scan 成一條鏈再 group_by,polars 要維持整年 1.5 億 group 的
  雜湊表——1 月 8.2GB、1 年 7.6GB RSS + 7GB swap(2025 建表中止)。修法:
  逐 raw 日 collect → 逐月 part → 串流串接;2025 全年 **34 秒、峰值 4.5GB**,
  2026-07 逐列比對與舊法 exact equal。「一年一子程序」只是把爆點推遲,
  **key 含分片鍵的聚合一律逐分片做**。
- T4 建表逐月切塊(group 鍵含 date,月切無損)。
- 四張表落地:T1 8.5GB/6.8 億列、T3 91MB/470 萬列(2016+)、
  T4 61MB/120 萬分點日;T2=ws-core lazy 視圖。

## 12. 待辦清單快照(2026-09-15 晚,v0.38.0/v1.0.0 ship 後;**09-22 逐條標註**)

1. **T1 `--incr` 增量更新**:仍未做,但痛點變了——T1 逐日建表後整年重建 34 秒(事故 ④),
   `--incr` 的價值在修訂資料的下游範圍與排程,不在時間
2. ~~P4:在 T4 重做分群~~ → 由 v3 Phase 1-4 取代(t4_broker_measure + 校準卡;T4 v2 frozen)
3. ~~P5:broker_labels → ws-quant 處置線改讀~~ → Step F 盤點:無消費端、無依賴(`docs/STEP_F_INVENTORY_2026-09-21.md`)
4. P6:歸因研究(NNLS/籃子指紋/BuyLocation/主動ETF×投信對帳),家法全套
5. T1 2021-2023 閉環 BQ 補驗(帳本 A5)
6. ws-quant 側等 user 拍板:《生態圖鑑》要不要補 U1 散戶化章節;
   rejected.yaml pull_rate retry 標記已消耗
7. 持續盤:H-20260911 週更(23/60)、3450 高盛殘餘出清/JPM 十日窗

## 13. P6 歸因研究 backlog(2026-09-15 蒸餾;**2026-09-16 起整節歸入
`docs/OBSERVATORY_2026-09.md` 的 O6 期**,籃子指紋提前至 O1)

> 來源 = user 徵詢的外部 LLM 匿名資金流估計器提案(兩篇)。收其可取、
> 標其不知、丟其重複。**任何一題開跑前:凍結預期、鎖鄰近變體、入 trial
> ledger——治理不隨提案來,隨家法來。**

### 採用的框架語言

- **兩層構念**:Investor Identity(外資/投信/自營/其他)vs **Flow Character**
  (informed/index/避險/集中大戶/散戶 churn)。後者較可能載 alpha——與
  ws-quant facts #8 八連殺教訓(「人」無資訊、「狀態」有)理論-實證互證。
- **會計恆等式監督框架**:Other = 總量 − 四官方桶;前四桶有 T3 地面真值
  可監督,Other 為無監督分解對象(RetailDiffuse/LocalConcentrated/
  CorporateLike/Unknown)。
- **修正(A5,提案不知)**:分點日報 ⊆ TEJ vol(鉅額/特殊交易不在分點資料),
  恆等式必須聲明在「普通盤宇宙」;Other 殘差含鉅額,須另行對帳或揭露。
- **錨的可信度分級(U3 實證,提案不知)**:外資錨 corr 0.97(歸因可做準)/
  投信錨 0.58(先天低精度,輸出須帶寬信賴帶)/自營名字錨=空殼(僅能靠
  官方 T3 數字,無分點監督)。五條 latent flow 不得一視同仁標精度。

### 實驗排序(便宜且能判別者先)

1. **籃子指紋 probe**(全篇最佳招,T1×T3 現成):分點日持股向量 vs 官方
   外資/投信日向量的 cos/rank-corr——市場整體當指紋,非逐股比對。
   先當診斷:哪些分點日與外資向量高相似?時間穩定嗎?(≈半天)
2. **恆等式一致性 QA**:T3 的 tot_* 欄 vs 四桶加總;Other 殘差與分點
   宇宙的鉅額缺口對帳(A5 的延伸,1 小時級)。
3. **NNLS 歸因 v1**:w≥0+稀疏+時間平滑;**權重設為慢變量(週/月頻)**,
   每日重估必不穩;階層式收縮(sector/size 分層+先驗)防對 stock×branch
   硬估;驗收=外資權重須重現名字席位名單(0.97 錨)。
4. **價格積極度 BuyLocation**(T2 現成):VWAP_buy 相對日內區間位置,
   身分/性格分類的特徵,也是獨立描述量。
5. **生命週期狀態**:FlowAge/NewEntry/CumFlow/反手——ws-branch 原
   11 策略部分重疊,引用其封存結論,不重蓋。
6. **markout 技巧曲面**(封存 PNL 軸的重生形態):VWAP markout 取代 FIFO、
   Skill(b,horizon,side,sector) 曲面取代單一分數;**復活前置=補 IS/OOS
   分段+beta 拆離(帳本欠條)**;fade-the-crowded-skill(SSRN preprint
   假說,當輸入不當事實)。
7. **X 光交叉驗證**:TDCC 級距(**本地僅 2025-03+ 週頻**,歷史短)、
   當沖佔比(T3 已有)、融資融券(shareholding 有,T3 未納,需時再加欄)。
8. **主動 ETF fingerprint**(在庫 2026-04+):holdings 變化 × T3 投信流 ×
   分點籃子三方疊合;申贖污染但書照 ws-core docstring。

### 明確不做/待拍板

- **不做**:每日重估的 w_{b,k,t,s} 全參數版(辨識性不足);GBM 交互作用
  自由挖掘(HARKing;若做,特徵與交互須預先宣告+trial ledger);
  把 SSRN 數字當已驗證事實。
- **待 user 拍板**:被動 ETF PCF 申贖籃資料採購(本地無;liquidity_shock
  評估卡過的同一缺口)。
- **設計權在本 repo**:schema/feature 對著真實 T1-T4(單位/覆蓋/怪癖)設計,
  外部提案只當點子產生器。
