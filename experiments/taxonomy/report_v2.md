# 台股分點行為分類學 v2 — Broker Taxonomy Report

**Descriptive/Exploratory research**(見 [[feedback_exploration_vs_confirmation]])
**Supersedes**:`report.md`(v1)
**Author**:autonomous session,2026-07-06
**新增內容 vs v1**:
- Q4 broker × symbol pair archetype 偵測(「做的股票」)
- prop desk 特徵(aggregate_share)補 v1 遺漏
- Q3 時間穩定性(2021-2022 / 2023-2024H1 / 2024H2-2025H1 三期)

---

## TL;DR(v2 新發現)

1. **「做的股票」pattern 分成四類**:
   - **institutional_daily**(機構日常持倉,~37 pairs):外資投行 → 2330/2454
   - **dedicated_making**(專營做股,11 pairs):**京城-嘉義 → 康那香 28.6% 市佔**、
     **元大 → 00637L 反向 ETF 26.8% 市佔**
   - **occasional_whale**(偶發大戶單,3361 pairs):100% market share 但只 3-5 天
   - **day_trade_regular**(頻繁進出小額,11029 pairs):-經紀 分點主導
2. **虎尾幫 C1 不是「stick to specific stock」而是「跳股輪換」**:
   - C1 只 1 個 dedicated_making pair(永豐金-潮州 → 00637L 反向 ETF)
   - 61 個 occasional_whale,7 個 day_trade_regular — 分散型 pattern
   - 印證 user 提供的市場 intel:虎尾幫是隔日沖策略,不是傳統做股主力
3. **prop desk 完美識別**:`aggregate_share_median = 1.0` 對 26 個 `-自營` 分點
   perfect signal(v1 遺漏,現補上)
4. **虎尾幫時間動態揭示「被量化打爆」證據**:
   - 3 個 hardcore(2021-2022、2023-2024H1、2024H2-2025H1 三期皆 C1-like):
     **兆豐-虎尾、凱基-總公司、永豐金-潮州**
   - 4-5 個 fadeout(2021 是但後來不是):**凱基-斗六、台灣企銀-三民、凱基-虎尾、
     國票-南京** — 對應 user intel「被外資量化打爆」
   - 5 個新加入(2023 之後才 C1-like):元大-忠孝鼎富、兆豐-內湖、台中銀-高雄、
     台灣企銀-建成、致和-台北

---

## 1. Motivation

v1 report 已產出 4 個 branch-level cluster,C1 (25 brokers) 驗證「後側冷門分點大單」
直覺。v2 探索 v1 § Limitations 中列的 4 個 gap:
- **Q4** broker × symbol interaction("做的股票")— **本次核心新增**
- **Q3** 時間動態
- **Q6.2** prop desk 遺漏
- 部分做:GMM/hierarchical clustering(未做,k-means 已足)

新的市場 intel(user 2026-07-06 push-back 後):
- 虎尾/嘉義是知名 隔日沖/當沖 gang(電腦室、開班授課)
- 大戶 pattern:「進出量都是其他分點數倍、甚至數十倍,而且集中在特定股票」
- 「他們近年被外資量化基金打爆」

v2 三個新分析都設計為回應這些具體市場現象。

---

## 2. Q4 Broker × Symbol Pair Archetype

### 2.1 方法

對 114 sample days,每 (broker, symbol) pair 跨天聚合:
- `pair_total_dollar_vol`:總金額
- `pair_days`:該 pair 有交易的天數
- `persistence` = pair_days / broker_active_days
- `median_day_concentration` = symbol / broker_total per day 中位數
- `median_day_market_share` = broker's share of symbol's total per day 中位數

按 pre-declared 規則歸類為四個 archetype:

| Archetype | persistence | concentration | market_share | 意義 |
|-----------|-------------|---------------|--------------|------|
| institutional_daily | ≥ 85% | ≥ 5% | (中) | 每天在做 + 集中投入 = 機構日常持倉 |
| dedicated_making | ≥ 40% | ≥ 1.5% | ≥ 5% | 高頻高市佔 + 中集中 = 專營做股 |
| occasional_whale | < 20% | (低) | ≥ 15% | 偶爾大量參與單一股票 = 藏單大戶 |
| day_trade_regular | ≥ 25% | < 2% | ≥ 2% | 頻繁進出但金額不大 = 短線客/當沖 |

### 2.2 各 archetype 的原型案例

**institutional_daily(機構每日持倉)**:
| broker | symbol | market_share | concentration |
|--------|--------|--------------|---------------|
| 摩根大通 | 2330(TSMC) | 8.4% | 12% |
| 台灣摩根士丹利 | 2330 | 7.0% | 12% |
| 新加坡商瑞銀 | 2330 | 6.0% | 7% |
| 美林 | 2330 | 5.5% | 9% |
| 香港上海匯豐 | 2330 | 1.1% | **17%** |
| **永豐金-潮州** | **00637L**(反 1 ETF) | 1.8% | 12% |

外資投行是 2330 常客(百分之七以上市佔),但**永豐金-潮州(C1 虎尾幫)**
出現在此類別上是**反向 ETF 天天做**的隔日沖 pattern — 「天天做」不代表持倉。

**dedicated_making(專營做股,11 pairs)**:
| broker | symbol | market_share | concentration | persistence |
|--------|--------|--------------|---------------|-------------|
| **京城-嘉義** | **9919**(康那香) | **28.6%** | 7.9% | 60% |
| 元大 | 00637L | 26.8% | 1.9% | 99% |
| 統一-彰化 | 6219(富致) | 14.6% | 1.7% | 75% |
| 國票-新莊 | 3444(利機) | 12.6% | 2.0% | 81% |

**京城-嘉義 → 康那香** = 這份 taxonomy 目前最強烈的「做股」訊號。一家分行連續
114 天樣本中有 60% 天在做,佔康那香總成交量 28.6%,並且 concentration 對這家
broker 也算高(8%)— 明顯是**用這家分行做這隻股票**。

**occasional_whale(偶發大戶,3361 pairs)**:
100% market share 但只 3-5 天。這是「特定日大單擊中冷門小股」的典型 pattern。
分布上 C0 大都會分行 1850 個、C3 小地方 613 個、C1 虎尾幫只 61 個。這意味著:
「偶發大戶下大單藏於分行」是**普遍現象**,不只 C1。

**day_trade_regular(頻繁進出小額,11029 pairs)**:
7102 個屬 null cluster(機構等)、C0 有 3438 個。這是散戶密集分行的日常「跟盤」
pattern,金額對分行來說小但天天有。

### 2.3 各 cluster × archetype cross-tab

```
                     institutional  dedicated  occasional  day_trade  minor
                     _daily         _making    _whale      _regular
Cluster null         33             8          591         7102       其餘
Cluster 0(都會散戶) 0              0          1850        3438       其餘
Cluster 1(虎尾幫)  1              1          61          7          其餘
Cluster 2(中型主力)0              3          246         其餘        其餘
Cluster 3(小地方散戶)0            0          613         其餘        其餘
```

**關鍵**:C1 虎尾幫**只有 1 個 dedicated_making pair**。這推翻我原本以為「虎
尾幫 = 做特定股票」的假設。實際上:
- 虎尾幫是**跳股輪換**的隔日沖策略,不 stick to specific stock
- 他們的訊號多在 occasional_whale(當天大量交易某股,隔幾天換另一支)
- 這與「隔日沖」策略邏輯一致:每天挑當日熱門股拉抬,不會固守

C2 中型主力有 3 個 dedicated_making,是**傳統做股主力**的簽名(京城-嘉義、
統一-彰化、國票-新莊)— 這些才是「做特定股票」的分點。

### 2.4 給 v2 一個機制修正

**修正 user 直覺**:「做的股票」pattern **不在 C1 虎尾幫**(隔日沖跳股),
**在 C2 中型主力**(如京城-嘉義-康那香)。C1 是隔日沖跳股輪換型,C2 是專營
特定股票操縱型 — 兩個不同的市場現象。

**兩個都是有價值的偵測目標**:
- C1 → 隔日沖客戶行為 → user intel「被量化打爆」→ 可測驗其當前狀態
- C2 → 專營做股主力 → 可能仍有 alpha(操縱模式較難被 arb)

---

## 3. Prop Desk 特徵(v1 遺漏補正)

### 3.1 修正
v1 filter `price != "-"` 排除 aggregate rows → prop desks(全用 aggregate)
完全不見。v2 加 `aggregate_share = aggregate_rows / total_rows` 為第 9 維。

### 3.2 效果
| Anchor | aggregate_share_median | n |
|--------|------------------------|---|
| **prop_desk** | **1.0** | 26 |
| foreign_institutional | 0.0 | 13 |
| aggregator_institutional | 0.0 | 5 |
| futures | 0.0 | 2 |
| branch_dash | 0.0 | 878 |
| hq_or_other | 0.0 | 60 |

**perfect identifier**:aggregate_share = 1.0 = prop desk(僅走券商自營總計)。

v2 的完整 anchor 分類 = 6 類(v1 是 5 類),涵蓋所有已知市場結構角色。

---

## 4. 時間穩定性 — 虎尾幫的動態演變

### 4.1 方法

三個時期各自 rebuild profile + rebuild cluster:
- 2021-2022(49 sample days)
- 2023-2024H1(36 days)
- 2024H2-2025H1(29 days)

每期識別「當期 C1-like」= max_row_concentration_median 最高的 cluster
brokers。追蹤 v1 全期 C1 的 25 個 brokers 在三期是否穩定屬 C1-like。

### 4.2 三類演變 pattern

**Hardcore(三期皆 C1-like,4 年鐵桿藏單/隔日沖)**:
- 兆豐-虎尾
- 凱基-總公司
- 永豐金-潮州

**Fadeout(2021-2022 是但後來不是,可能對應 user intel「被量化打爆」)**:
- **凱基-斗六**(2021-2022 ✓,之後全 ✗)
- **台灣企銀-三民**(2021-2022 ✓,之後全 ✗)
- **國票-南京**(2021-2022 ✓,2023 ✓,2024-2025 ✗)
- **凱基-虎尾**(2021-2022 ✓,2023 ✓,2024-2025 ✗)

**New(2023 之後才 C1-like,新加入)**:
- 元大-忠孝鼎富(2023 起)
- 兆豐-內湖(2023 起)
- 台中銀-高雄(2023 起)
- 台灣企銀-建成(2023 起)
- 致和-台北(2023 起)

**Never period-C1(v1 全期分類為 C1 但每期單獨看都不是,邊界案例)**:
- 台灣企銀-北高雄, 日茂-埔里, 盈溢-籬子內, 統一-永和, 美好-高雄

### 4.3 每期 C1-like 總數變化
- 2021-2022: 29 brokers
- 2023-2024H1: 21 brokers
- 2024H2-2025H1: 18 brokers

**每期 C1-like 數量下降**(29 → 21 → 18)。這是**強證據**支持 user intel:
藏單/隔日沖型分行**近年減少**(可能因外資量化 arb-ed 掉大部分 alpha)。

### 4.4 意義

「虎尾幫」不是**靜態一群人**,而是**動態的市場現象**:
- 有一些鐵桿(3 個)持續運作
- 有一些逐漸淡出(4-5 個,可能對應量化基金 arb 掉的部分)
- 有一些新加入(5 個,可能是資金/客戶流動的接手)

**對後續研究的暗示**:若要交易化「fade 虎尾幫」策略,不能用 v1 static cluster
(2021-2025 平均),需要 **rolling window taxonomy**(每季重算)。這是 v3
的方向。

---

## 5. 對現有 alpha 研究的啟發

### 5.1 京城-嘉義 → 康那香(9919)
持續 4 年,單一分行控制 28.6% 成交量。這是**待驗證的 alpha 源頭**:
- 假說:當京城-嘉義大額進 9919(當日 > median × 2),T+1~T+5 forward return 有訊號
- Path A 事件研究可 quickly 驗
- 若 alpha 存在 → 「跟隨京城-嘉義」策略;若反向 → 「fade 京城-嘉義」

### 5.2 反向 ETF 專營者
`元大 → 00637L` (元大台灣50反 1) 26.8% 市佔;永豐金-潮州(C1)也在此 pair
上高活躍。反向 ETF 有內建每日衰減,適合做隔日沖 → 這是「機構做莊 vs 散戶隔日沖」的公開池,值得單獨研究是否有可交易 pattern。

### 5.3 hardcore vs fadeout 的對比
若能 tag brokers 為 "hardcore" / "fadeout" / "new" / "never-in-period-C1",
可設計不同策略:
- fade **hardcore** — 若他們仍在做但 alpha 已消失,反向或許有小 edge
- avoid **new** — 新崛起的可能是新玩家有短期 edge
- ignore **fadeout** — 已被 arb

---

## 6. 給後續 (v3 or spinoff)

### 6.1 尚未做的
- Rolling window taxonomy(每季重算,追蹤 broker migration)
- Broker × symbol × day 動態 archetype(某分點在 A 股是 dedicated_making,
  在 B 股是 occasional_whale — 這種混合角色未被本次捕捉)
- 主動 fade/follow 特定 broker 的 alpha 探索
- Full data(取代 114 sample days;對於 dedicated_making 這種穩定 pattern
  的 median 統計可能更 robust)

### 6.2 直接可行的 spinoff hypothesis 候選
1. **H:京城-嘉義大單 → 康那香 forward return** — 具體、單一,快速驗證
2. **H:「dedicated_making brokers」大單 → forward return** — 更廣的一批
3. **H:虎尾幫 hardcore 每日出貨 → T+1 fade** — 用 3 個 hardcore 分點做 T+1
   反向策略
4. **H:反向 ETF 隔日沖 pattern 是否仍有 alpha**

以上都是 taxonomy 產出的 tradeable 線索,若要走 confirmation phase 需各自
pre-register 走原 governance flow。

---

## Appendix A. 檔案索引(v2)

- Q1(基礎特徵)script:`scripts/broker_taxonomy_build_features.py`(v2 加 aggregate_share)
- Q1.5(profile)+ Q5(anchor):`scripts/broker_taxonomy_analyze.py`(v2 加 aggregate_share metric)
- Q2(cluster)script:`scripts/broker_taxonomy_cluster.py`(unchanged from v1)
- Q4a(pair features)script:`scripts/broker_taxonomy_v2_broker_symbol.py`(v2 新)
- Q4b(archetype 分類)script:`scripts/broker_taxonomy_v2_pair_archetypes.py`(v2 新)
- Q3(時間穩定)script:`scripts/broker_taxonomy_v2_time_stability.py`(v2 新)
- Data:
  - `broker_day_features.parquet`(v2 加 aggregate_share)
  - `broker_profiles.parquet`(v2 rebuild)
  - `broker_clusters.parquet`(v1,unchanged)
  - `broker_symbol_pairs.parquet`(v2 新)
  - `broker_symbol_archetypes.parquet`(v2 新)
- Analysis MD:
  - `anchor_validation.md`(v2 加 aggregate_share row)
  - `pair_archetypes_analysis.md`(v2 新)
  - `time_stability_analysis.md`(v2 新)
  - `report_v2.md`(本檔,主要 deliverable)
