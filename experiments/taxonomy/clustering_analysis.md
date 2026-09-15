# Branch_dash Clustering Analysis

樣本:806 個 branch_dash brokers(active_days ≥ 50)
維度(8):n_symbols_median, shares_per_symbol_median, median_row_shares_median, max_row_concentration_median, top1_symbol_concentration_median, top5_symbol_concentration_median, n_rows_median, total_shares_median

## Elbow analysis

| k | Inertia | Δ vs prev |
|---|---------|-----------|
| 2 | 4243.9 | - |
| 3 | 3523.1 | 720.8 |
| 4 | 3141.4 | 381.7 |
| 5 | 2485.5 | 655.9 |
| 6 | 2095.4 | 390.1 |
| 7 | 1934.6 | 160.8 |
| 8 | 1668.4 | 266.2 |

## Chosen k = 4

### Cluster centroids(unstandardized)

| Cluster | n | n_symbols_median | shares_per_symbol_median | median_row_shares_median | max_row_concentration_median | top1_symbol_concentration_median | top5_symbol_concentration_median | n_rows_median | total_shares_median |
|---|---|---|---|---|---|---|---|---|---|
| C0 | 416 | 693.125 | 15,118 | 1,102 | 0.031 | 0.069 | 0.211 | 2,383 | 10,332,599 |
| C1 | 25 | 228.920 | 9,997 | 1,136 | 0.096 | 0.189 | 0.436 | 425.303 | 1,885,183 |
| C2 | 67 | 490.000 | 18,519 | 1,413 | 0.054 | 0.124 | 0.330 | 1,509 | 8,978,567 |
| C3 | 298 | 405.544 | 10,027 | 1,005 | 0.047 | 0.089 | 0.260 | 1,051 | 3,960,769 |

### 各 Cluster 代表 broker(前 10 名)

**Cluster 0(n = 416)**
  元富-信義, 凱基-士林, 富邦-員林, 永豐金-內湖, 元大-台中, 元大-景美, 永豐金-台南, 永豐金-嘉義, 國票-台南, 台新-高雄

**Cluster 1(n = 25)**
  兆豐-虎尾, 盈溢-籬子內, 凱基-虎尾, 台灣企銀-三民, 凱基-斗六, 台灣企銀-岡山, 聯邦-富強, 兆豐-內湖, 永豐金-虎尾, 元大-忠孝鼎富

**Cluster 2(n = 67)**
  兆豐-大同, 元富-虎尾, 兆豐-南京, 群益金鼎-大安, 康和-內湖, 凱基-員林, 華南永昌-竹北, 統一-南京, 中國信託-松江, 兆豐-大安

**Cluster 3(n = 298)**
  台灣企銀-九如, 合庫-新竹, 兆豐-民生, 第一金-台中, 兆豐-新莊, 元富-屏東, 高橋-中壢, 永豐金-埔里, 群益金鼎-潭子, 統一-台南
