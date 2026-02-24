# Information Fragmentation Alpha：分散式私有資訊的聚合與 Alpha 萃取

## 核心命題

在台股分點資料中，我們觀察到一個反直覺的現象：

> **一支股票的可交易 alpha 強度，與「具統計顯著預測力的券商數量佔比」高度正相關。**

直覺上，我們預期 alpha 來自「少數特別準的人」。但實證顯示，alpha 更多來自「大量各自持有碎片資訊的人」的聚合。

| 股票 | 顯著券商佔比 | Out-of-sample t | 策略報酬 | 結論 |
|------|-------------|-----------------|---------|------|
| 2330（台積電） | 1.9% | — | — | 無信號，early exit |
| 2345（智邦） | 7.3% | 11.8 | +622% | 強信號 |
| 8996（高力） | 18.3% | 10.0 | +806% | 強信號 |

本文探討為何如此，並整理相關理論框架。

---

## 1. 觀察：顯著券商佔比作為信號強度的代理變數

### 1.1 零假設基準

我們使用 Welch's t-test（t > 1.96，單尾 α ≈ 2.5%）檢驗每家券商的「大單日隔日報酬」是否顯著高於「非大單日」。在零假設（所有券商無預測力）下，897 家券商中預期有約 22 家（2.5%）通過門檻。

- 台積電 17 家 < 22 家：**低於隨機預期**，代表零信號
- 智邦 65 家 ≈ 3× 隨機預期：扣除假陽性後約 43 家真顯著
- 高力 164 家 ≈ 7× 隨機預期：扣除假陽性後約 142 家真顯著

### 1.2 為什麼佔比比絕對數更重要

佔比本質上是一個 **signal prevalence** 指標——它衡量的不是「最準的人有多準」，而是「這個市場中有多少人掌握了尚未被價格反映的資訊」。

---

## 2. 理論框架

### 2.1 Grossman-Stiglitz Paradox（資訊效率悖論）

**Grossman & Stiglitz (1980)** 指出：如果市場完全有效（價格反映所有資訊），那麼沒有人有動機花成本去蒐集資訊。因此，均衡狀態下市場必然是「部分有效」的——價格反映大部分但非全部資訊，留下足夠的 alpha 補償資訊蒐集者。

**與我們的觀察的連結**：台積電接近完全有效（1.9% ≈ 噪音），高力遠離有效（18.3%），智邦居中。顯著券商佔比可以視為 **Grossman-Stiglitz 均衡中「未被反映的資訊量」的實證測量**。

> **關鍵字**：Grossman-Stiglitz paradox, information acquisition cost, partially revealing equilibrium

### 2.2 Kyle's Lambda（資訊衝擊係數）

**Kyle (1985)** 的經典模型中，informed trader 的交易對價格的衝擊由 λ（lambda）決定。λ 越大，每單位 order flow 對價格的影響越大，代表市場對資訊交易越敏感。

我們的系統本質上在做的事情是：**從分點 order flow 中辨識 informed component，然後預測隔日價格變動**。這與 Kyle 模型的 market maker 問題是對偶的——market maker 試圖從 order flow 推斷資訊，我們也是。

差異在於：Kyle 假設一個 informed trader，我們的實證發現是**多個 heterogeneous informed traders**。

> **關鍵字**：Kyle (1985), lambda, price impact, informed order flow, strategic trading

### 2.3 PIN — Probability of Informed Trading

**Easley, Kiefer, O'Hara & Paperman (1996)** 提出 PIN 模型，將每日交易拆解為：

```
每日交易 = 無資訊交易（noise） + 有資訊交易（informed）
```

PIN 值越高，代表 informed trading 佔比越大。

我們的「顯著券商佔比」和 PIN 在概念上是近親——兩者都在衡量「有多少交易活動來自 informed participants」。差異在於：

| | PIN | 我們的方法 |
|--|-----|----------|
| 資料 | 買賣 tick 數 | 分點買賣超 |
| 粒度 | 全市場 aggregate | 逐券商分解 |
| 結構假設 | Poisson arrival | 無參數 |
| 輸出 | 單一數字 | 每家券商的顯著性 |

我們的方法更 granular——不只知道「有 informed trading」，還知道「哪些券商 informed」以及「他們各自的擇時能力（TA）有多強」。

> **關鍵字**：PIN model, Easley-O'Hara, probability of informed trading, VPIN (Volume-Synchronized PIN), order flow toxicity

### 2.4 Mosaic Theory（馬賽克理論）

在證券分析實務中，**Mosaic Theory** 指的是：分析師合法地從多個公開和非重大非公開來源蒐集碎片資訊，拼湊出對公司的完整判斷。

這正是我們在高力上觀察到的現象：

- 沒有任何單一券商知道全貌
- 但不同券商各自持有不同的碎片（客戶訂單、供應鏈、產能利用率...）
- 他們各自的交易行為（大單買超/賣超）反映了這些碎片
- 我們的信號聚合，本質上就是在做「自動化的 Mosaic 分析」

> **關鍵字**：mosaic theory, information mosaic, CFA Institute ethics, material nonpublic information (MNPI)

### 2.5 Wisdom of Crowds 與信號聚合

**Surowiecki (2004)** 歸納了群體智慧的四個條件：

| 條件 | 在我們系統中的對應 |
|------|-------------------|
| Diversity of opinion（觀點多樣性） | 不同券商有不同資訊來源 |
| Independence（獨立性） | 各分點的交易決策獨立 |
| Decentralization（去中心化） | 無統一指令，各自判斷 |
| Aggregation mechanism（聚合機制） | TA 加權信號 `Σ(TA_b × dev_b / σ_b)` |

當四個條件都滿足時，群體判斷優於任何個體。高力 142 家真顯著券商滿足了所有條件，所以聚合信號（test t=10）遠強於任何單一券商。

**統計上的解釋**：如果 N 個獨立信號各有相關係數 ρ，聚合後的信噪比按 √N 增長（在 ρ 小時近似成立）。142 家 → 信噪比放大約 12 倍。

> **關鍵字**：wisdom of crowds, Surowiecki, ensemble methods, signal aggregation, diversity prediction theorem, Condorcet jury theorem

### 2.6 Heterogeneous Agents（異質代理人模型）

傳統金融理論假設 representative agent（代表性投資人），但實證觀察（如我們的分點資料）明確顯示投資人是異質的。

**Diether, Malloy & Scherbina (2002)** 發現 analyst forecast dispersion（分析師預測分歧度）與後續報酬有關。我們的「顯著券商佔比」可以視為一種 **revealed disagreement measure**——不是看分析師怎麼說，而是看交易者怎麼做。

**Hong & Stein (1999)** 的 gradual information diffusion 模型指出：資訊在不同投資者之間緩慢擴散，導致初期 underreaction 和後續 momentum。我們的系統可能正在捕捉這個擴散過程的早期階段——部分券商已經行動，但價格尚未完全反映。

> **關鍵字**：heterogeneous agents, analyst dispersion, Hong-Stein model, gradual information diffusion, underreaction, momentum

### 2.7 Market Microstructure 與 Order Flow

**Hasbrouck (1991)** 將價格變動拆解為 permanent component（資訊）和 transitory component（噪音），並用 order flow 來推斷。我們的方法本質上是 Hasbrouck 分解的分點版本。

**Chordia, Roll & Subrahmanyam (2002)** 研究 order imbalance（買賣失衡）對報酬的預測力，發現在小型股上更顯著。這與我們的觀察一致——高力（中小型）的信號強於台積電（大型）。

> **關鍵字**：market microstructure, Hasbrouck (1991), permanent price impact, order imbalance, Chordia-Roll-Subrahmanyam

---

## 3. 為什麼「少數天才」模型在這裡不適用

直覺上的「找幾個最準的券商」策略有三個根本問題：

### 3.1 無法可靠識別

假設真的只有 3 家超準的券商，但還有 22 家假陽性。你面對 25 家「顯著」券商，無法事前區分哪 3 家是真的。這是 **multiple testing** 的經典難題——當真信號稀疏時，假陽性率遠高於真陽性率（精確率極低）。

### 3.2 信號太稀疏

3 家券商，每家一年可能只有 20~30 個大單日。合計 60~90 個信號日 / 365 天 ≈ 每週一次。太稀疏的信號無法建構穩定的交易策略。

### 3.3 不穩定

少數券商的行為可能因人事異動、策略改變、客戶流失而突然消失。142 家券商的聚合信號則對個別券商的變化有很強的 robustness。

---

## 4. 延伸思考

### 4.1 這個指標能否用於選股？

如果「顯著券商佔比」是 alpha 強度的代理變數，是否可以：

1. 對全市場 2839 支股票計算此指標
2. 篩選出佔比 > 某閾值的股票
3. 對這些股票建構 TA 加權信號
4. 形成一個多股票的投資組合

這本質上是把 single-stock alpha 延伸為 **cross-sectional alpha**。

### 4.2 佔比的時間穩定性

一個關鍵問題：高力的 18.3% 是否穩定？還是隨時間漂移？如果某支股票在 train period 佔比高但 test period 降低，信號會衰減。這需要 rolling window 分析。

### 4.3 與公司特徵的關係

直覺上，以下公司特徵可能與高顯著佔比相關：

- 較小的市值（analyst coverage 少）
- 較高的波動度（資訊衝擊大）
- 供應鏈複雜度高（資訊碎片化）
- 法人持股比例低（散戶/中實戶主導）
- 產業主題性強（AI、電動車等，吸引 informed speculation）

驗證這些假說需要 cross-sectional regression。

### 4.4 與 PIN 的實證比較

如果能取得逐筆成交資料（tick data），可以同時計算 PIN 和我們的顯著券商佔比，檢驗兩者的相關性和各自的增量預測力。

---

## 5. 參考文獻

| 文獻 | 核心概念 |
|------|---------|
| Grossman & Stiglitz (1980). "On the Impossibility of Informationally Efficient Markets." *AER* | 完全有效市場不可能存在；均衡下必有未反映資訊 |
| Kyle (1985). "Continuous Auctions and Insider Trading." *Econometrica* | Informed trading 的經典模型；lambda、price impact |
| Easley, Kiefer, O'Hara & Paperman (1996). "Liquidity, Information, and Infrequently Traded Stocks." *JF* | PIN 模型；estimated probability of informed trading |
| Easley, de Prado & O'Hara (2012). "Flow Toxicity and Liquidity in a High-frequency World." *RFS* | VPIN；高頻環境下的 informed trading 測量 |
| Hasbrouck (1991). "Measuring the Information Content of Stock Trades." *JF* | Order flow 的資訊含量分解 |
| Hong & Stein (1999). "A Unified Theory of Underreaction, Momentum Trading, and Overreaction." *JF* | 資訊在異質投資者間的漸進擴散 |
| Diether, Malloy & Scherbina (2002). "Differences of Opinion and the Cross Section of Stock Returns." *JF* | 分析師預測分歧度與報酬的關係 |
| Chordia, Roll & Subrahmanyam (2002). "Order Imbalance, Liquidity, and Market Returns." *JFE* | 買賣失衡對報酬的預測力 |
| Surowiecki (2004). *The Wisdom of Crowds*. Anchor Books | 群體智慧的四個條件與聚合機制 |
| Barber, Odean & Zhu (2009). "Do Retail Trades Move Markets?" *RFS* | 散戶交易的資訊含量 |
| Boehmer, Jones & Zhang (2020). "Potential Pilot Problems." *RFS* | 機構 vs 散戶 order flow 的資訊含量差異 |

---

## 6. 關鍵字索引

供後續研究使用：

**市場微結構**：market microstructure, order flow, price impact, bid-ask spread, information asymmetry, adverse selection

**Informed Trading 測量**：PIN, VPIN, order flow toxicity, Kyle's lambda, Amihud illiquidity, information share

**信號聚合**：ensemble methods, wisdom of crowds, Condorcet jury theorem, diversity prediction theorem, boosting, bagging, signal-to-noise ratio

**異質代理人**：heterogeneous agents, heterogeneous beliefs, disagreement, analyst dispersion, gradual information diffusion

**台股特有**：broker-level data (分點資料), proprietary trading desk (自營商), retail flow, day trading

**統計方法**：multiple testing, false discovery rate (FDR), Benjamini-Hochberg, Bonferroni correction, permutation test, Welch's t-test
