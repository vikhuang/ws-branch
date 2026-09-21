# Broker Flow Observatory 架構提案（2026-09-16，v1 待 user 審）

> 一句話：在 T1-T4 之上蓋一座**給人讀的觀測站**——每個分點每天一張臉、
> 每檔股票每天一份讀本。四層維度全部接在已驗證的機制上,描述先行,
> 預測歸法院。此文件取代 REDESIGN §8 的 P4-P6 重心排序(法理不變,重心翻轉)。

## 0. 為什麼(缺口診斷,user 2026-09-15 定調)

一年來我們蓋了法院(假說→紅隊→判決,八墓碑戰績),沒蓋觀測站——原始資料
到判決之間沒有一層「看」的工具,每次讀盤面都靠即棄的手工腳本(f16 系列)。
外部提案兩篇的真正價值 = 把「看」制度化;我們的歷史教訓 = 維度必須接在
被驗證過的機制上。本提案是兩者的合成。

**可讀性警語**:可讀生敘事,敘事生事後 alpha(breadth 之死)。觀測站 =
exploration 工具,看與讀完全自由;任何「它能預測」的聲明進 ws-quant 治理。

## 1. 四層維度(每層附機制錨與查證依據)

### L-A 行動者狀態——這櫃檯今天是一個腦袋還是一千個腦袋
- **決策者多重性**(合併方向比+集中度+購物籃自相似度為單一潛變量):
  低=單一意志(機構台/專營戶/大戶),高=人群(散戶櫃檯)。
  這是 taxonomy 真正在量的東西,合併後分群變成可解釋連續量。
- **身分相似度**(籃子指紋):分點日購物籃向量 vs T3 官方外資/投信向量的
  cos/rank-corr——市場整體當指紋。錨:外資 0.97(U3)。投信錨僅 0.58,
  輸出帶寬信賴帶;自營無分點錨(只有官方數字)。
- 機制錨:U1(處置期散戶化/外資作業性退場)即此維度粗版讀出的真東西。

### L-B 部位狀態——這個持有者現在套牢還是獲利 ★首推
- **部位軌跡**:多窗淨額/連買賣天數/FlowAge/反手——一條時間序列的四種摘要。
- **持倉與成本狀態**:自 2021 全史 FIFO 累積持倉、成本均價、未實現損益%、
  部位年齡。**引擎現成**:pnl/ 封存件中 `domain/fifo.py` 為純函數且帶
  `cost_per_share`+`open_date`(已查證)——復用的只是這段純域邏輯,
  不復活其未驗證的 PNL 排名聲明(帳本欠條不變)。
- 機制錨(我們唯一反覆驗證的機制之直接操作化):套牢 chaser=壓抑窗主力
  賣壓(P2 t=4.31,n=603);機構收貨→59.4% 解禁後倒貨(bootstrap);
  3450 活例:JPM 成本 ~569 於 500-509 放量倒貨=停損,無成本狀態讀不出。
- **適用界線**:成本狀態只對低多重性席位有意義(客戶穩定);高多重性
  櫃檯只算軌跡。以 L-A 的多重性 gate 之。

### L-C 執行風格——怎麼買的
- **價格位置** BuyLocation=(VWAP_buy−Low)/(High−Low)(T2×日 OHLC)。
- **切單細碎度**:當日成交價位數/分佈形狀(大單直插 vs 演算法切單)。
- **時序推斷**(我們獨有,提案沒有):分點成交價位集合 × tick 價格軌跡
  → 推交易時段(開盤追/尾盤掃)。精度=時段機率非時點;tick 自 2024-07-23
  起 527 交易日(已查證)。分盤股為精確版先例(F19 撮合指紋 42/42 吻合、
  93% 量可歸因,已查證)。

### L-D 關係——跟誰交易、跟誰一起動(提案空白層)
- **對手盤矩陣**:同 (stock,day,price) 的買方群×賣方群按比例歸因
  「誰從誰手上接貨」(機率)。骨架=F19 的價位級 join;分盤股可精確驗證,
  驗過再推連續股。機制錨:3450 手工做過(機構自套牢盤收貨)。
- **共動群組**:分點×分點跨日跨股購物籃同步 → 合併多通道為隱形主體。
  用途:外資 24% 暗流嫌疑人名單(U3 覆蓋缺口)、專營戶跨券商分身。
  簽名 pair 40 組=此維度單點版。消歧:用 L-A 多重性+身分相似度。

(markout=結果端標籤非狀態維度,歸研究;regime=環境欄位非原語。)

## 2. 架構(映射到既有三色分層,零新顏色)

```
tables/(宣告+IO)                       measure/(全純函數)
  registry 新增:                          actor.py      多重性/自相似度/指紋
    t4 v2   分點×日   行動者狀態擴欄        position.py   軌跡+FIFO 持倉狀態
    t5a_trajectory    分點×股×日(全體,活躍門檻)
    t5b_cost_state    分點×股×日(白名單,FIFO) (_fifo.py = 自 pnl/ 提取的純域邏輯,
    t6_exec_style     分點×股×日(活躍門檻)     附出處與復用聲明)
    t7_counterparty_watch 股×日(watchlist)  execution.py  BuyLocation/細碎度/時序
  io 擴充:stateful checkpoint(T5 用)      relation.py   對手盤歸因/共動(純計算)

products/(IO,給人與下游)
  profile.py       每分點每日一張臉(四層欄位齊)→ broker_profiles parquet
  stock_reader.py  每股每日一份讀本(誰在買/新老手/像誰的錢/買位/對手盤/
                   **擁擠度**=同方向分點數與增速——描述量,頁面必附註腳
                   「breadth 型預測已判死 R-dispo-breadth-crowding」)
                   → stock_reader parquet + 單股一頁渲染(ws-desk context 餵料)
  labels.py        broker_labels parquet(ws-quant 處置線契約,原 P5 承諾)

experiments/flow_lab/  共動群組批次、markout 曲面、一切預測性研究(家法全套)
```

**表 schema 草案**(建表時定稿,單位一律股/元,日期=台北 Date):
- t3 補欄(2026-09-16):融資融券/借券(`buy_l/sell_l/long_t/...`,shareholding
  本有,建表時未選)——讀本環境描述用;作預測子已判死(R-counter-cyclical),
  不重測。
- t4 v2 新欄:`multiplicity`(0-1 複合)、`basket_self_sim`、`foreign_sim`、
  `fund_sim`(+信賴帶旗標;買/賣向量各算一次)、**`sector_hhi`、`top_sector`**
  (產業碼自 ws-core tickers;TEJ 產業粗,主題籃子辨識力受限——2026-09-16 補)、
  **`daytrade_assoc`**(所持股票 T3 vol_dtp 以 gross 加權;區分當沖/隔日沖幫 vs
  現貨散戶——補)、`oddlot_share`(股數 mod 1000 佔比;**可緩**,與多重性重複)、
  滾動窗 20/60/120/250D 版身分相似度;沿用 v1 全欄。
- t5a_trajectory(**全體**分點×股票×日,活躍門檻:近 60 日 gross≥N 張,控規模;
  讀本「新面孔/老手」須對當日所有買方成立,故不限白名單——2026-09-16 補):
  `broker, symbol_id, date, net_1d/3d/5d/10d/20d/60d, consec_buy/sell,
  flow_age_days, prior60d_exposure, flipped, cum_flow_20`。
- t5b_cost_state(**白名單宇宙**:無 dash HQ 席位+簽名 pair+名單分點,可設定
  擴充;≈120 分點;客戶穩定才有「成本」語意):`broker, symbol_id, date,
  pos_sh, cost_avg, unreal_pnl_pct, pos_age_days`(FIFO,stateful)。
- t6_exec_style(活躍門檻 gross≥10 張):`broker, symbol_id, date, vwap_buy,
  vwap_sell, buy_location, sell_location, n_price_levels, frag_score,
  session_guess(nullable,tick 期間才有)`。
- t7_counterparty_watch(watchlist 股票,如處置母體+持股名單):
  `symbol_id, date, buyer_broker, seller_broker, est_lots, method(exact|prob)`。
  全市場版不物化,relation.py 隨叫隨算。

## 3. 建置紀律(記憶體條文延伸)

- 全部走 registry/runner:序列、一年一子程序、月切塊;壓縮區預算先估。
- T5 為**有狀態表**(FIFO 跨年累積):registry Table 加 `stateful` 旗標,
  io 提供 per-year checkpoint(仿 pnl_engine 的 fifo_state 模式,但宇宙=
  白名單,規模砍到 T1 的一成內)。重建=從 2021 重放;增量=從 checkpoint 續。
- t6 讀 T2(lazy 視圖)聚合:月切塊硬性。
- 時序推斷(tick)為隨叫隨算 lens,不入每日批次(重活,案例導向)。

## 4. 驗證錨(每層的地面真值,先於任何敘事)

| 層 | 錨 | 判準 |
|---|---|---|
| L-A 指紋 | U3 外資 0.97 | 外資名字席位的 foreign_sim 分佈須顯著高於母體(重現錨) |
| L-B 持倉 | TDCC 週級距(2025-03+) | 白名單分點累積持倉變化 vs 大戶級距變化同向;解禁倒貨率隨未實現虧損深度單調(描述性) |
| L-C 執行 | 分盤股撮合指紋 | 分盤股上 BuyLocation/時序推斷與精確重播一致(F19 先例) |
| L-D 對手盤 | 分盤股精確配對 | exact 法與 prob 法在分盤股上的歸因一致率報告 |
| 全部 | 合成 e2e | 觀測站表全部納入既有 e2e 小宇宙(手算臉與讀本) |

## 5. 分期(取代 REDESIGN P4-P6 重心;每期都先出「3450 一頁讀本」驗讀感)

| 期 | 內容 | 驗收 |
|---|---|---|
| O1 ✅(2026-09-16) | t4 v2(actor:多重性+指紋)+ 3450 讀本原型 v1 | 指紋錨重現(2026 全年,institutional cohort vs 母體:foreign_sim_buy Welch t=4.77 p=1.8e-6 d=0.30;foreign_sim_sell t=7.98 p=1.5e-15 d=0.50;fund_sim 更強 d=1.65-1.79——四項皆顯著,方向正確);3450 讀本(2026-09-14 解禁 day-1)獨立重現手工結論:摩根大通/美商高盛/美林三席外資賣壓、元大等 HQ 席位人群化(multiplicity 0.6-0.8)。**解讀後但書(同日,`experiments/flow_lab/findings/o1_fingerprint_read.md`)**:錨通過的來源是熱門度——官方外資向量與成交金額 Spearman 0.866,扣掉熱門度後 foreign_sim 不分外資席位(AUC 0.53);現行 rank-corr 指紋應讀成「掃市場的廣度」。金額 cosine 殘差版探針 AUC 0.92(單月),為 v3 候選,改定義待 user 決策 |
| O2 | t5a 軌跡(全體)+ t5b 成本狀態(FIFO 白名單)+ TDCC 對向檢查 | 3450 的 JPM 成本線自動重現手工結論;倒貨率×虧損深度描述表 |
| O3 | t6 執行風格(T2 聚合+BuyLocation) | 分盤股 vs 撮合指紋一致;讀本 v2 加執行欄 |
| O4 | products 三件套上線(profile/讀本/labels)→ ws-quant 處置線改讀 labels → 拆凍結舊模組;ws-desk 接讀本 | 原 P5 驗收條款;每日批次入 run 排程 |
| O5 | 關係層(對手盤 watch 表+共動群組首輪批次於 flow_lab) | 分盤股 exact 一致率;共動群組產出外資暗流嫌疑名單 v1 |
| O6 | 預測性研究(原 P6 backlog §13 全部歸此,含 markout 曲面/fade 假說) | 家法全套,逐題凍結預期 |

原 P4(重分群)併入 O1:在 t4 v2 特徵上重做,對 T3 錨驗,標有效期。
**未完成(O1 這輪未做,順延)**:實際的重分群重做——本輪 O1 施工只完成
user 指定的 5 項任務(schema/驗證錨/3450 讀本/測試),沒有動 P4 的分群
演算法;`measure/identity.py` 的 CLUSTER_VOLUME_SHARE 等仍是舊窗口
(2021-01~2025-08)版本,尚未在 t4 v2 特徵上重跑。

## 6. 治理邊界(一條線,寫死)

觀測站的一切輸出=**描述**,自由看自由讀,不需 pre-register;
任何欄位要說「能預測報酬」→ 立假說走 ws-quant 治理(或 flow_lab 家法),
引用 rejected.yaml 相關墓碑(行動者層八墓碑尤其)。讀本頁面上加固定註腳:
「本頁為描述性觀測,非交易訊號」。

## 7. 風險

- 敘事污染(最大風險):讀本太好讀 → 事後故事 → 直跳交易。防線=治理邊界
  +讀本註腳+任何升級走法院。
- 白名單偏誤:t5 只蓋低多重性席位,讀本要明示「散戶櫃檯無成本狀態」。
- 對手盤/共動為機率構念,禁止當事實引用;輸出一律帶 method/confidence。
- 規模:t6 若門檻設太低會逼近 T1 規模;建表前先估列數與壓縮區預算。
- 投信指紋先天糊(0.58 錨),讀本相關欄位降權呈現。

## 8. 與既有文件的關係

- REDESIGN §8:P4-P6 重心由本文件 O1-O6 取代(其 P1-P3 已完成不動)。
- REDESIGN §13(P6 backlog):整節歸入 O6;籃子指紋提前至 O1(升為 t4 v2
  欄位);恆等式 QA 維持 O1 前置;PCF 採購決策不變(待 user)。
- pnl/ 封存狀態不變;唯一提取物=`domain/fifo.py` 純域邏輯(附出處註記)。

## 9. 提案覆蓋審計(2026-09-16,對照外部提案兩篇逐指標)

做完 O1-O6 覆蓋約九成。五處原設計漏洞已於本版補入(上方標「補」):
①軌跡對全體(t5a)②產業維度③融資欄+零股比(可緩)④當沖關聯度⑤讀本擁擠度
(帶墓碑)。**刻意不做**:猜 beneficial owner(制度上不可能)、每日全參數
w_{b,k,t,s}(辨識性)、技巧曲面當每日產品(先研究)、GBM 自由挖掘(閘住)。
**資料缺**:被動 ETF PCF(待 user 拍板採購)。**延後**:指數調整流
(index/rebalance character,需指數事件日曆,stock_attr 成分旗標可推,等案例)。

---

## 10. v3 骨架重整與研究暫停(2026-09-21)

**O1 的「改定義待 user 決策」已決**(見 §5 的 O1 但書尾句)。user 2026-09-18
拍板的方向不是「把 foreign_sim 換成 amount cosine」這種欄位級修補,而是整層重定位:

> **行動者層 v3 = Broker-flow Measurement Layer**。首要目標不是分類「誰是誰」,
> 而是先建立券商席位資金流的最小、可解釋、可校準的行為座標系。
> 外資/投信/自營官方桶降為 **calibration anchor**,不是研究主題;
> 「從公開分點反推外資實際下在哪些席位」拆成獨立的 **v3.5 inverse-attribution**。

**現行規格(兩份,都在 ws-quant)**:

- 研究骨架:`~/r20/wp/ws-quant/docs/actor_layer_v3_reframed_2026-09-18.md`
  ——五 phase 順序(geometry → trait/state → salience → actor calibration →
  accounting bounds),Phase 1 明令不碰 alpha;T4 v3 收薄至八個 primitive,
  multiplicity / rolling / rank 版 foreign_sim / sector_hhi 退場,residual 讀時算;
  新增 stock salience 取代 presence;buy/sell 不預設對稱;結論須標
  observed / identified / bounded / inferred / unidentifiable 五級。
- 工程銜接與資料契約:`~/r20/wp/ws-quant/docs/actor_layer_v3_architecture_alignment_2026-09-18.md`
  ——三色分層落位、T4 v3 最小 schema、硬會計界限**雙側區間**(不只 HiddenForeign_min)、
  對 O1-O6 的逐期影響(**§10 對本檔**)、A-F 遷移順序。
  另含兩處對 09-17 原提案的語意補正:①`Other = total − 官方四桶` 不是
  「外資券商 cohort 以外的席位」②T3 尚未納入自營兩桶的**金額**欄,
  故原提案的八個 actor amount cosine 尚未具備完整輸入(須標 `unanchored`,
  不得以收盤價 × 股數假裝 amount)。

**對本檔分期的直接影響**(細節見銜接文 §10):
O2 拆成 O2a 流量軌跡(可獨立推進)與 O2b 模型成本狀態(不作 v3 前置——
舊設計「低 multiplicity → 客戶穩定 → 可讀 FIFO 成本」這條依賴已取消);
O4 先交行為 profile 與讀本,labels 另做相容遷移,**不承諾身份機率**;
O3 可沿 T2 推進,不等 actor 身份辨識。

**狀態:規格已凍結,Phase 1 尚未開工**(user 2026-09-21 轉去新專案)。
09-17 原提案已加「已被取代」橫幅,保留為討論紀錄。施工時正式契約與架構文件
回歸本 repo 維護(銜接文 §3.1),屆時該兩份文件在 ws-quant 側只保留引用與
消費端遷移紀錄。

**ws-quant 側同期發生的事**(影響本層的取用端):處置線的 `dispo-rightarm-v2`
與 `sr-v2-10d` 已於 2026-09-21 retired(非證偽,user 決定暫停),
slots 4/6 → 2/6;`src/broker_taxonomy` 維持凍結。因此 O4 的
「ws-quant 處置線改讀 labels」在重啟前沒有急迫的下游需求,
可按 v3 的 measurement-first 順序慢慢來。
