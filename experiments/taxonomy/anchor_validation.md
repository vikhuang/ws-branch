# Anchor Cross-Validation Report

目的:驗證行為維度是否能區隔已知 institutional / prop vs 一般分行(branch_dash)。
若已知 anchor 在特徵上與 branch_dash 差異微弱,說明維度定義有問題。

**Baseline(branch_dash「公司-地名」):n = 878**

### foreign_institutional(n = 13)

| Feature | Anchor median | Branch median | Anchor/Branch ratio | KS D | KS approx p |
|---------|---------------|---------------|---------------------|------|-------------|
| n_symbols_median | 500.5 | 544.8 | 0.919 | 0.424 | 0.01346 |
| shares_per_symbol_median | 2.093e+05 | 1.288e+04 | 16.250 | 0.972 | 7.862e-12 |
| median_row_shares_median | 5,000 | 1,000 | 5.000 | 0.922 | 1.072e-10 |
| mean_row_shares_median | 2.835e+04 | 4,067 | 6.971 | 0.997 | 1.994e-12 |
| max_row_concentration_median | 0.04223 | 0.03751 | 1.126 | 0.444 | 0.008203 |
| top1_symbol_concentration_median | 0.1024 | 0.07748 | 1.321 | 0.356 | 0.05856 |
| top5_symbol_concentration_median | 0.3157 | 0.2333 | 1.353 | 0.430 | 0.01159 |
| n_rows_median | 2,002 | 1,707 | 1.173 | 0.441 | 0.008924 |
| total_shares_median | 5.469e+07 | 7.078e+06 | 7.726 | 0.683 | 4.564e-06 |
| active_days | 114 | 114 | 1.000 | 0.207 | 0.6051 |
| aggregate_share_median | 0 | 0 | nan | 0.000 | 1 |

### prop_desk(n = 26)

| Feature | Anchor median | Branch median | Anchor/Branch ratio | KS D | KS approx p |
|---------|---------------|---------------|---------------------|------|-------------|
| n_symbols_median | nan | 544.8 | nan | nan | nan |
| shares_per_symbol_median | nan | 1.288e+04 | nan | nan | nan |
| median_row_shares_median | nan | 1,000 | nan | nan | nan |
| mean_row_shares_median | nan | 4,067 | nan | nan | nan |
| max_row_concentration_median | nan | 0.03751 | nan | nan | nan |
| top1_symbol_concentration_median | nan | 0.07748 | nan | nan | nan |
| top5_symbol_concentration_median | nan | 0.2333 | nan | nan | nan |
| n_rows_median | nan | 1,707 | nan | nan | nan |
| total_shares_median | nan | 7.078e+06 | nan | nan | nan |
| active_days | 114 | 114 | 1.000 | 0.106 | 1 |
| aggregate_share_median | 1 | 0 | nan | 1.000 | 1.292e-23 |

### aggregator_institutional(n = 5)

| Feature | Anchor median | Branch median | Anchor/Branch ratio | KS D | KS approx p |
|---------|---------------|---------------|---------------------|------|-------------|
| n_symbols_median | 20 | 544.8 | 0.037 | 0.997 | 2.168e-05 |
| shares_per_symbol_median | 6,105 | 1.288e+04 | 0.474 | 0.815 | 0.0009473 |
| median_row_shares_median | 1,062 | 1,000 | 1.062 | 0.444 | 0.2069 |
| mean_row_shares_median | 2,896 | 4,067 | 0.712 | 0.614 | 0.02612 |
| max_row_concentration_median | 0.1628 | 0.03751 | 4.341 | 0.992 | 2.406e-05 |
| top1_symbol_concentration_median | 0.3573 | 0.07748 | 4.611 | 0.993 | 2.344e-05 |
| top5_symbol_concentration_median | 0.7894 | 0.2333 | 3.383 | 0.997 | 2.168e-05 |
| n_rows_median | 39.5 | 1,707 | 0.023 | 0.995 | 2.225e-05 |
| total_shares_median | 1.206e+05 | 7.078e+06 | 0.017 | 0.995 | 2.225e-05 |
| active_days | 114 | 114 | 1.000 | 0.212 | 1 |
| aggregate_share_median | 0 | 0 | nan | 0.000 | 1 |

### futures(n = 2)

| Feature | Anchor median | Branch median | Anchor/Branch ratio | KS D | KS approx p |
|---------|---------------|---------------|---------------------|------|-------------|
| n_symbols_median | 4.5 | 544.8 | 0.008 | 1.000 | 0.01117 |
| shares_per_symbol_median | 1.072e+04 | 1.288e+04 | 0.832 | 0.448 | 0.7074 |
| median_row_shares_median | 3,000 | 1,000 | 3.000 | 0.883 | 0.03514 |
| mean_row_shares_median | 4,211 | 4,067 | 1.035 | 0.440 | 0.7339 |
| max_row_concentration_median | 0.255 | 0.03751 | 6.799 | 0.999 | 0.01131 |
| top1_symbol_concentration_median | 0.5163 | 0.07748 | 6.664 | 1.000 | 0.01117 |
| top5_symbol_concentration_median | 1 | 0.2333 | 4.286 | 1.000 | 0.01117 |
| n_rows_median | 10.5 | 1,707 | 0.006 | 1.000 | 0.01117 |
| total_shares_median | 4.825e+04 | 7.078e+06 | 0.007 | 0.999 | 0.01131 |
| active_days | 107 | 114 | 0.939 | 0.794 | 0.07609 |
| aggregate_share_median | 0 | 0 | nan | 0.000 | 1 |

### hq_or_other(n = 60)

| Feature | Anchor median | Branch median | Anchor/Branch ratio | KS D | KS approx p |
|---------|---------------|---------------|---------------------|------|-------------|
| n_symbols_median | 477.2 | 544.8 | 0.876 | 0.166 | 0.08026 |
| shares_per_symbol_median | 1.724e+04 | 1.288e+04 | 1.339 | 0.361 | 4.968e-07 |
| median_row_shares_median | 2,000 | 1,000 | 2.000 | 0.488 | 1.927e-12 |
| mean_row_shares_median | 5,623 | 4,067 | 1.383 | 0.446 | 1.751e-10 |
| max_row_concentration_median | 0.04319 | 0.03751 | 1.151 | 0.195 | 0.02385 |
| top1_symbol_concentration_median | 0.09557 | 0.07748 | 1.233 | 0.274 | 0.000313 |
| top5_symbol_concentration_median | 0.2782 | 0.2333 | 1.192 | 0.320 | 1.309e-05 |
| n_rows_median | 1,490 | 1,707 | 0.873 | 0.187 | 0.03486 |
| total_shares_median | 8.668e+06 | 7.078e+06 | 1.225 | 0.326 | 8.705e-06 |
| active_days | 114 | 114 | 1.000 | 0.035 | 1 |
| aggregate_share_median | 0 | 0 | nan | 0.000 | 1 |

## 判讀

**理想結果**:foreign_institutional / prop_desk 至少在 shares_per_symbol、
max_row_concentration、n_symbols 上有明顯差異(KS p < 0.01, ratio 遠離 1)。
aggregator_institutional 應該在 n_rows / total_shares 上偏低或偏高(-經紀通常聚合單少)。
若差異微弱 → 目前維度不足以識別 anchor → 需要加更多維度(時序、跨股集中度、日間變異)。