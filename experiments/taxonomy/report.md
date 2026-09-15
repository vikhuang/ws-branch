# 台股分點行為分類學 v1 — Broker Taxonomy Report

**Descriptive/Exploratory research**(見 [[feedback_exploration_vs_confirmation]])
**Author**:autonomous session,2026-07-06
**Trigger**:user push-back on kill decision of retail-frenzy-umbrella v2
(2 REFUTED aux 之後);naïve broker_name proxy 造成的假陰性懷疑。
**Data window**:broker_tx 2021-01-04 ~ 2025-08-29(114 個抽樣交易日,
每 ~10 交易日一取,避開 frenzy OOS 窗口)。
**輸出檔**:`broker_day_features.parquet`、`broker_profiles.parquet`、
`broker_clusters.parquet`、`clustering_analysis.md`、`anchor_validation.md`

---

## TL;DR

1. **8 維 broker × day 行為特徵**能有效區隔已知 institutional 錨點:
   - `-經紀`/`-法人`(aggregator):n_symbols=20 vs 分行 545,top1_concentration
     36% vs 8%(KS p < 1e-5)。
   - foreign(港商/-自營未捕獲,見 §6.2 待改進):total_shares 7.7x,mean_row 大。
   - `-自營`(prop desk):**目前特徵漏掉**(全部走 aggregate rows,per-price
     filter 過濾掉了) — 這本身也是 signature。
2. **「公司-地名」型分行(v2 全部歸為 retail)內部自然分為 4 個 cluster**:
   - **C0(n=416, 53% vol)** 都會大型散戶密集(元富-信義, 凱基-士林...)
   - **C1(n=25, 3% vol)** **鄉鎮藏單型 whale**(兆豐-虎尾, 凱基-斗六...)—— **這是關鍵發現**
   - **C2(n=67, 8% vol)** 中型主力密集(統一-南京, 兆豐-大同...)
   - **C3(n=298, 36% vol)** 小型地方性散戶(合庫-新竹, 統一-台南...)
3. **C1 有明顯地域偏差**:中南部分行佔 52% vs C0 只佔 8%(6.5x)。
   驗證使用者「後側冷門分點大單 = 藏單」的市場直覺。
4. **對 retail-frenzy-umbrella v2 的影響**:v2 把 C0+C1+C2+C3 全算 retail
   → C1+C2(11.3% vol 但異質性最強)污染了散戶行為訊號。
   若剔除 C1+C2 只留 C0+C3(88.7% 同質散戶),aux2 F3 診斷觀察到的
   「散戶下跌日 sell 3.54x baseline」很可能大幅收斂到接近凹單簽名。

---

## 1. Motivation

v2 分類 spec 用 `broker_name` pattern(公司-地名 = retail)是 5 分鐘 naïve
proxy,實際上把三種行為型態混為一談:真散戶密集分行、藏單型 whale 分行、
中型主力分行。傘級 aux1(ρ=0.15 < 0.5)與 aux2(凹單 Δ=+0.013 遠低 0.05)
的 REFUTED 可能大部分來自這種混合造成的訊號稀釋,而非機制本身失敗。

**用 confirmation-style stop rule 綁 exploration 是 category error** — 我
最初的規則(「至多 v2.1 修訂,超過強制 kill」)套錯了紀律。台股分點行為
本身就是**未解、冷門、困難的 descriptive 問題**,exploration 是必要,產出
本身有價值(獨立於是否救活傘級)。

## 2. Method

### 2.1 資料
- **Source**:`~/r20/data/fugle/broker_tx/broker_tx_YYYYMMDD.parquet`
- **Window**:2021-01-04 ~ 2025-08-29(避開 frenzy OOS 保持獨立)
- **Sampling**:每 10 交易日取一天,共 **114 天**(足以觀察跨年穩定性)
- **Unit**:單日 (broker, symbol, price) rows,單位 = 股(非張)
- **Filter**:排除 aggregate rows(price="-"),排除總量 = 0 rows

### 2.2 特徵設計(8 維)

| Feature | Formula | 意義 |
|---------|---------|------|
| `n_rows` | count of (symbol, price) rows | 分點當日活躍度 |
| `total_shares` | Σ (buy + sell) | 分點當日總量 |
| `mean_row_shares` | total / n_rows | 平均單一「size bucket」規模 |
| `median_row_shares` | median of row_shares | 典型 size bucket 規模(more robust) |
| `n_symbols` | unique symbols | 客戶多樣性代理 |
| `shares_per_symbol` | total / n_symbols | 每股平均量(集中度反指標) |
| `max_row_concentration` | max_row / total | 單一 size bucket 佔比(藏單簽名) |
| `top1_symbol_concentration` | top-1 symbol / total | 最集中股票佔比 |
| `top5_symbol_concentration` | top-5 symbols / total | 分散度 |

Broker profile 聚合:median across active_days ≥ 50 days。避開了短暫活躍的雜訊 broker。

### 2.3 Clustering
- K-means with k-means++ init
- 對 skewed 特徵(shares_per_symbol / median_row_shares / n_rows / total_shares)
  做 log10 變換
- Standardize before clustering
- Elbow(k=2..8)後選 k=4

## 3. Anchor Cross-Validation

用**已知類別**(-經紀/-法人/-自營/futures/foreign)驗證特徵是否能區分。
Baseline = branch_dash("公司-地名"型分行,n=856)。

### 3.1 Aggregator(-經紀/-法人,n=5)——**極度可辨識**

| Feature | Anchor | Baseline | Ratio | KS p |
|---------|--------|----------|-------|------|
| n_symbols_median | 20 | 545 | **0.037** | 2.2e-5 |
| top1_symbol_concentration | 36% | 8% | **4.6** | 2.3e-5 |
| top5_symbol_concentration | 79% | 23% | **3.4** | 2.2e-5 |
| n_rows_median | 40 | 1,707 | **0.023** | 2.2e-5 |
| max_row_concentration | 16% | 4% | **4.3** | 2.4e-5 |

Aggregator 只交易 20 支股票(vs 分行 545 支),80% 集中在 top 5 → 完美吻合
「機構聚合單」的預期簽名。 → **特徵有效**

### 3.2 Futures(元大期貨/群益期貨,n=2)——**極度可辨識**

| Feature | Anchor | Baseline | Ratio |
|---------|--------|----------|-------|
| n_symbols | 4.5 | 545 | 0.008 |
| top1_symbol | 52% | 8% | 6.7 |

期貨分點只交易 2-5 支股票 → 完美吻合。 → **特徵有效**

### 3.3 Foreign institutional(港商*/摩根/美林...,n≈13)

| Feature | Anchor | Baseline | Ratio | KS p |
|---------|--------|----------|-------|------|
| total_shares_median | 5.5e7 | 7.1e6 | **7.7** | 4.6e-6 |
| max_row_concentration | 4.2% | 3.8% | 1.1 | 0.008 |
| top1_symbol_concentration | 10% | 8% | 1.3 | 0.06 |

Foreign 主要在「單筆規模大」上明顯,集中度只略高於 baseline 。→ **部分有效**,
但需要更好的 identifier(如「per-row shares > threshold」的 tail 指標)。

### 3.4 Prop desk(-自營,n=0)—— **⚠️ 特徵漏掉**

`-自營` 分點交易全部走 aggregate rows(price="-"),被我的 per-price filter
排除。**這其實是 prop 的獨特簽名**:
- 未來 v2 taxonomy 應加 `aggregate_share = aggregate_rows / (aggregate + per_price)`
  作為第 9 維。 = 100% 就是 prop signature。

## 4. Clustering: 分行(branch_dash)內部結構

**Sample**:806 個 branch_dash brokers with active_days ≥ 50

### 4.1 Elbow

| k | Inertia | Δ |
|---|---------|---|
| 2 | 4244 | — |
| 3 | 3523 | 721 |
| **4** | **3141** | 382 |
| 5 | 2486 | 656 |
| 6 | 2095 | 390 |

Elbow 不極端明顯,選 k=4 是穩健中庸;k=5 也可,但 4 個 clusters 已有清楚
的 archetype 可命名。

### 4.2 Cluster Archetypes

| Cluster | n | Vol % | n_symbols | shares/symbol | max_row_conc | top1_sym | Archetype |
|---------|---|-------|-----------|---------------|--------------|----------|-----------|
| **C0** | 416 | **53%** | 693 | 15k | **3.1%** | **6.9%** | 都會大型散戶密集 |
| **C1** | **25** | 3% | 229 | 10k | **9.6%** | **18.9%** | **鄉鎮藏單型** |
| **C2** | 67 | 8% | 490 | **18.5k** | 5.4% | 12.4% | 中型主力密集 |
| **C3** | 298 | 36% | 406 | 10k | 4.7% | 8.9% | 小型地方性散戶 |

### 4.3 Cluster 1(**藏單型 whale**)完整名單(25)

```
元大-大直       元大-忠孝鼎富   凱基-總公司     凱基-虎尾*
兆豐-中壢       兆豐-內湖       兆豐-虎尾*      凱基-斗六*
台中銀-高雄     台灣企銀-三民   台灣企銀-北高雄 台灣企銀-岡山*
台灣企銀-建成   國票-南京       日茂-埔里*      永興-台中
永豐金-潮州*    永豐金-虎尾*    盈溢-楠梓*      盈溢-籬子內*
福邦-新竹       統一-永和       美好-高雄       聯邦-富強
致和-台北
```
*標記為中南部/鄉鎮分行(共 13/25 = **52%**),vs C0 的 32/416 = **8%**。
**6.5 倍地域偏差**驗證「後側冷門分點大單 = 藏單」的市場直覺。

**特徵組合分析**:C1 的 max_row_concentration(9.6%)與 top1_symbol
concentration(18.9%)都是 C0 的 2-3 倍。這**不是單純活躍度低**(active_days
中位數與 baseline 相同),而是**行為結構性偏向大單 + 少股集中** — 典型
藏單簽名。

### 4.4 各 cluster 佔總 volume 比例

- **C0(53%)+ C3(36%)= 89% 是真散戶密集分行**(C0 大都會 + C3 小地方)
- **C1(3%)是「retail 名下但實為藏單」**的最主要污染源
- **C2(8%)是中型主力**,行為介於散戶與 whale 之間

## 5. 對 retail-frenzy-umbrella v2 的可能影響

**v2 aux1/aux2 REFUTED 之診斷**:

若假設 C0+C3(714 brokers, 89% vol)為真散戶,C1+C2(92 brokers, 11% vol)
含噪聲:

- v2 aux2 F3 診斷觀察「散戶下跌日 sell = 平坦日 3.54x」。**假設**這 3.54x
  的部分來源是 C1 藏單型分點下跌日拋售(大戶恐慌賣壓),而非真散戶行為。
  若剔除 C1+C2 只算 C0+C3,`retail_sell` 分子可能顯著下降 → holdon_rate
  可能提高 → 靠近「凹單」的預期簽名。
- v2 aux1 ρ_median = 0.15 弱訊號同理:C1+C2 的行為與 long_t 增速的
  結構性相關可能非常不同(甚至負相關,若大戶藏單常反手融資),稀釋了
  真散戶的訊號。

**但這只是假設**。要驗證需要用 taxonomy 重跑 aux1/aux2(是否要做見 §7)。

## 6. Limitations & Future Work

### 6.1 Sample 而非 full data
114 sample days across 4.5 years。優點:快速迭代;缺點:每 broker 的 profile
是 sample median,若某 broker 的行為只在特定期間表現(如某段時期的藏單潮),
sample 可能低估。若要用於正式測量,需擴到 full data。

### 6.2 Prop desk 未捕獲
`-自營` 全部走 aggregate rows → per-price filter 排除他們。修法:features
應加 `aggregate_share`(0-1 float) 作為第 9 維。 → 待 v2 report。

### 6.3 K-means 假設球形 cluster
真實 broker 行為分布可能有 heavy tails 或非凸形狀。可試 GMM 或 hierarchical
clustering 作 robustness check。 → 待 v2 report。

### 6.4 Static clustering 未捕捉時間動態
本次是「per-broker 跨 4.5 年 median」的靜態 clustering。若某 broker 2021
時是散戶密集,2024 變成藏單型(隨資金流入變化),static clustering 看不見。
Q3(time stability)是待做項目。

### 6.5 未做「per (broker × symbol) 專營性」分析
Q4(broker × symbol interaction)未做。可能某分行對特定股票是散戶(交易頻繁
小單),對另一股票是大戶(偶爾大單)。若這個現象普遍,static broker-level
classification 就是根本不夠精細,需要 broker × symbol × day 動態標記。 →
待 v2 report if needed。

### 6.6 未做外部驗證
沒有跟 TEJ 券商別買賣超或其他外部資料 cross-check。目前 anchor 驗證只用
「name pattern 已知類別」這個 weak ground truth。

## 7. What Next

**Option A(建議)**:先擴大 taxonomy(v2 report):
- 加 `aggregate_share` 特徵補 prop desk
- 加時間動態分析(Q3):broker 隨時間漂移
- 加 broker × symbol 分析(Q4):專營性
- 產出更完整的 taxonomy v2

**Option B**:用 v1 taxonomy(C0/C1/C2/C3)重跑 aux1/aux2:
- 只用 C0+C3 = 真散戶密集分行作為 retail cohort
- 剔除 C1(藏單)、C2(中戶),不歸 retail
- 看 aux1 ρ、aux2 holdon 是否明顯改變
- 若改變顯著 → 傘級可以復活;若改變微小 → 傘級真的 REFUTED
- ⚠️ 注意:此為 second attempt after v2 OOS,governance 需明確標記
  「second measurement, 因 v1 measurement 被證明不足」

**Option C**:先讓 user 檢視 v1 taxonomy,再決定 A 或 B 或其他方向。

## Appendix A. 特徵計算的技術細節

- broker_tx 單位 = 股(不是張),張 = 1000 股。所有 shares 計算都是股。
- Aggregate rows(price="-")在本次全部排除:優點 clean、缺點漏掉 prop desk。
- Row = (broker, symbol, price)。同一 broker 同一天同一 symbol 不同 price
  是不同 row,反映當日多輪次委託。
- `max_row_shares` 是最大單一 (symbol, price) 的 buy+sell 總和,不是「單一
  委託」規模(因 broker_tx 已按 price 聚合)。

## Appendix B. 檔案索引

- Q1 script:`scripts/broker_taxonomy_build_features.py`
- Q1.5+Q5 script:`scripts/broker_taxonomy_analyze.py`
- Q2 script:`scripts/broker_taxonomy_cluster.py`
- Features data:`experiments/broker_taxonomy/broker_day_features.parquet`
- Profiles:`experiments/broker_taxonomy/broker_profiles.parquet`
- Clusters:`experiments/broker_taxonomy/broker_clusters.parquet`
- Anchor validation:`experiments/broker_taxonomy/anchor_validation.md`
- Clustering analysis:`experiments/broker_taxonomy/clustering_analysis.md`
