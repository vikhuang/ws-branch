# Statistical Methods

## Timing Alpha

```
timing_alpha = Σ((net_buy[t-1] - avg_net_buy) × return[t]) / std(net_buy)
```

- Positive = bought before rally, sold before drop (good timing)
- Negative = bought before drop, sold before rally (bad timing)
- Subtract mean to remove directional bias
- Divide by std(net_buy) to normalize for trade volume

Implemented in: `pnl_analytics/domain/metrics/timing_alpha.py`

## Hypothesis Testing

### No scipy dependency

All statistical functions are hand-implemented using `math.erfc` for normal CDF approximation (valid for n>30).

Implemented in: `pnl_analytics/domain/statistics.py`

### Dual significance threshold

A result is significant only when BOTH conditions are met:

- `p_value_corrected < 0.05`
- `|Cohen's d| >= 0.2` (small effect size minimum)

This prevents declaring significance on trivially small effects.

### Permutation test

- 10,000 permutations in `domain/statistics.py` (general purpose)
- 200 permutations in `domain/metrics/statistical.py` (per-broker timing alpha)
- p-value for broker ranking is computed at query time, not pre-stored (too slow to precompute)

### Event study significance

- Bonferroni correction: `p < 0.05 / n_horizons`
- Plus Cohen's d >= 0.2
- Conclusion: 2+ significant horizons = "significant", 1 = "marginal", 0 = "no_effect"

### Signal validation (signal_report.py)

- Welch's t-test with early exit: skip if <5% brokers significant OR |test t-stat| < 2
- Market scan uses Benjamini-Hochberg FDR correction across ~2400 stocks
