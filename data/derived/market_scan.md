# Market Scan Report
Generated: 2026-03-11 15:21:05

## Config

- Train: 2023-01-01 ~ 2024-06-30
- Test: 2024-07-01 ~ 2025-12-31
- Min turnover: 0億 NTD
- Cost: 0.50%
- FDR: 5%

## Filter Funnel

| Stage | Count | Description |
|-------|-------|-------------|
| Universe | 2413 | Stocks with price data |
| F0a: No ETF | 2052 | Exclude 361 ETFs/ETNs |
| F0b: No split | 2022 | Exclude 30 splits/reverse-splits |
| F0c: Data | 1842 | Train ≥ 30 days, Test ≥ 250 days |
| F1: Liquidity | 1842 | Avg turnover > 0億 |
| F2: Signal | 100 | >5% significant brokers |
| F3: FDR | 5 | BH-FDR < 5% |

## Results (sorted by Sharpe)

| Rank | Symbol | Sharpe | Return | MaxDD | Calmar | Test t |
|------|--------|--------|--------|-------|--------|--------|
| 1 | 3443 | -2.13 | -76.2% | -81.6% | -0.9 | +3.5 |
| 2 | 6139 | -2.29 | -78.5% | -83.0% | -0.9 | +2.8 |
| 3 | 2408 | -4.12 | -96.7% | -97.1% | -1.0 | -3.6 |
| 4 | 2344 | -4.18 | -94.4% | -94.5% | -1.0 | -3.6 |
| 5 | 3312 | -4.27 | -94.0% | -95.3% | -1.0 | +3.1 |
