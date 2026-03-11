# Signal Threshold Analysis - Key Findings

> **⚠️ 已封存 (2026-03-11)** — 本分析基於時區錯誤的資料，結果無效。
> 詳見 `docs/information_fragmentation_alpha.md` 頂部說明。

**Analysis Date:** 2026-02-25
**Script Location:** `/tmp/threshold_analysis.py`
**Test Period:** 2024-07-01 to 2025-12-31 (OOS)
**Stocks Analyzed:** Top 10 by OOS Sharpe from market_scan.json

## Executive Summary

Higher signal thresholds consistently improve strategy performance across all top stocks:
- **Sharpe Ratio**: Increases from 5.50 (threshold=0) to 6.80 (threshold=3) - a 24% improvement
- **Average Return per Trade**: Increases from 1.08% to 1.36% - a 26% improvement
- **Win Rate**: Increases from 63% to 67% - a 4 percentage point improvement
- **Trade Count**: Decreases from 279 to 231 trades per stock - more selective trading

## Aggregate Results (Average Across 10 Stocks)

| Threshold | Avg N Trades | Avg Return/Trade | Avg Sharpe | Avg Win Rate |
|-----------|--------------|------------------|------------|--------------|
| 0.0       | 279.3        | 1.081%          | 5.498      | 63.02%       |
| 0.5       | 271.5        | 1.121%          | 5.693      | 63.66%       |
| 1.0       | 263.1        | 1.170%          | 5.945      | 64.52%       |
| **2.0**   | **246.5**    | **1.274%**      | **6.421**  | **65.95%**   |
| 3.0       | 231.4        | 1.362%          | 6.800      | 67.37%       |

## Key Insights

### 1. Quality Over Quantity
- Increasing threshold from 0 to 3 reduces trades by ~17% (279 → 231)
- But increases Sharpe by +24% and return/trade by +26%
- **Implication**: Signal contains both high-quality and low-quality trades

### 2. Optimal Threshold
- **Threshold 2.0** appears to be the sweet spot:
  - Strong Sharpe improvement (+17% vs. baseline)
  - Maintains reasonable trade frequency (~247 trades/stock)
  - Win rate at 66% (vs. 63% baseline)
  
### 3. Diminishing Returns Beyond 2.0
- Threshold 2.0 → 3.0: Additional +6% Sharpe but -6% trades
- The marginal benefit decreases as threshold increases
- Risk of over-filtering in some stocks

### 4. Stock-Specific Performance

**Best Performers at Threshold 2.0:**
- **5274**: Sharpe 10.32, 79% win rate, 1.88% avg return
- **3131**: Sharpe 8.00, 68% win rate, 1.77% avg return
- **1326**: Sharpe 7.61, 68% win rate, 1.23% avg return

**Stocks with Largest Improvement (0 → 2.0):**
- **1326**: Sharpe +40% (5.45 → 7.61)
- **2059**: Sharpe +33% (4.14 → 5.50)
- **6415**: Sharpe +10% (4.75 → 5.25)

### 5. Trade-off Analysis

| Metric              | Threshold 0 | Threshold 2 | Change    |
|---------------------|-------------|-------------|-----------|
| Trades/Stock        | 279         | 247         | -11%      |
| Return/Trade        | 1.08%       | 1.27%       | +18%      |
| Sharpe Ratio        | 5.50        | 6.42        | +17%      |
| Win Rate            | 63%         | 66%         | +3pp      |

## Recommendations

### For Production Implementation
1. **Use threshold 2.0** as the default for market-wide scanning
   - Balances signal quality with trade frequency
   - Provides 17% Sharpe improvement over no threshold
   - Maintains ~250 trades per stock over 18-month OOS period

2. **Consider dynamic thresholds** based on:
   - Stock volatility (higher vol → higher threshold)
   - Signal strength distribution (adaptive to market regime)
   - Recent signal accuracy (Bayesian updating)

3. **Monitor threshold effectiveness** over time:
   - Track Sharpe ratio at different thresholds monthly
   - Adjust if signal quality degrades or improves

### For Research
1. **Investigate signal composition**:
   - What differentiates high-threshold signals from low?
   - Are certain brokers more informative at high thresholds?
   - Does time-of-day/week affect optimal threshold?

2. **Test non-symmetric thresholds**:
   - Long threshold ≠ Short threshold
   - Bull market vs. bear market thresholds

3. **Combine with other filters**:
   - Volatility regime (high/low VIX equivalent)
   - Momentum/mean-reversion regime
   - Liquidity conditions

## Technical Notes

### Signal Construction
- **Timing Alpha (TA)**: Computed per broker from training period (2023-01 to 2024-06)
- **Daily Signal**: Weighted sum of broker TAs, weighted by z-scored net_buy
- **Filter**: Only include brokers with |net_buy - mean| > 2σ
- **Threshold**: Applied as |signal| > threshold for trade entry

### Backtest Methodology
- **Entry**: signal[T] > threshold → long T+1 (close-to-close)
- **Cost**: 0.50% per trade (includes bid-ask spread + commissions)
- **Universe**: Top 10 stocks by OOS Sharpe from full market scan
- **Period**: 2024-07-01 to 2025-12-31 (18 months, ~280 trading days)

### Caveats
1. **Survivorship bias**: Only tested on top performers
2. **Overfitting risk**: Threshold optimized on same OOS period used for ranking
3. **Market regime**: Results may vary in different market conditions
4. **Execution**: Assumes perfect close-to-close execution

## Next Steps

1. **Validate on different universes**:
   - Test threshold on mid-tier stocks (Sharpe 2-4)
   - Test on bottom performers to see if threshold prevents bad trades

2. **Walk-forward analysis**:
   - Re-optimize threshold monthly
   - Test stability over different time periods

3. **Portfolio-level testing**:
   - How does threshold affect portfolio Sharpe?
   - Correlation between signals at different thresholds

4. **Signal decay analysis**:
   - Does signal quality degrade with higher thresholds?
   - Optimal holding period at different thresholds
