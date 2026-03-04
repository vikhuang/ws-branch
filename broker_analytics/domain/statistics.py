"""General-purpose statistical tests.

Provides Welch's t-test, Cohen's d, distribution comparison,
distribution shape analysis, and permutation tests.
No scipy dependency. Uses math.erfc for p-value approximation
(normal approximation, valid for n > 30).

These are pure functions — no project-specific logic.
"""

import math
from dataclasses import dataclass

import numpy as np


# =============================================================================
# Data Classes
# =============================================================================

@dataclass(frozen=True, slots=True)
class DistributionSummary:
    """Summary statistics for a distribution.

    Attributes:
        mean: Arithmetic mean
        median: Median value
        std: Standard deviation
        p5: 5th percentile
        p25: 25th percentile
        p75: 75th percentile
        p95: 95th percentile
        n: Sample size
    """
    mean: float
    median: float
    std: float
    p5: float
    p25: float
    p75: float
    p95: float
    n: int


@dataclass(frozen=True, slots=True)
class DistributionShape:
    """Shape analysis for threshold calibration.

    Answers: given a ±sigma threshold, what fraction of observations
    actually falls beyond it? Useful for validating normality assumptions.

    Attributes:
        skewness: Sample skewness (0 = symmetric).
        excess_kurtosis: Excess kurtosis (0 = normal, >0 = fat tails).
        pct_beyond: Actual % of observations beyond ±threshold.
        pct_expected: Theoretical % under normal distribution.
        threshold_percentile: What percentile the +threshold corresponds to.
        n: Sample size.
    """
    skewness: float
    excess_kurtosis: float
    pct_beyond: float
    pct_expected: float
    threshold_percentile: float
    n: int


@dataclass(frozen=True, slots=True)
class HypothesisTestResult:
    """Result of a two-sample hypothesis test.

    Significance requires BOTH p_value_corrected < 0.05 AND |cohens_d| >= 0.2.
    This dual criterion guards against large-n false positives.

    Attributes:
        t_stat: Welch's t statistic
        p_value: Raw two-tailed p-value
        p_value_corrected: Bonferroni-corrected p-value
        cohens_d: Cohen's d effect size
        significant: True if both p and effect size criteria met
    """
    t_stat: float
    p_value: float
    p_value_corrected: float
    cohens_d: float
    significant: bool


# =============================================================================
# Core Functions
# =============================================================================

def summarize(values: np.ndarray) -> DistributionSummary:
    """Compute summary statistics for a 1-D array.

    Args:
        values: 1-D numpy array (NaN values are dropped).

    Returns:
        DistributionSummary with all percentiles.
    """
    values = values[~np.isnan(values)]
    if len(values) == 0:
        return DistributionSummary(
            mean=0.0, median=0.0, std=0.0,
            p5=0.0, p25=0.0, p75=0.0, p95=0.0, n=0,
        )
    return DistributionSummary(
        mean=float(np.mean(values)),
        median=float(np.median(values)),
        std=float(np.std(values, ddof=1)) if len(values) > 1 else 0.0,
        p5=float(np.percentile(values, 5)),
        p25=float(np.percentile(values, 25)),
        p75=float(np.percentile(values, 75)),
        p95=float(np.percentile(values, 95)),
        n=len(values),
    )


def welch_t_test(a: np.ndarray, b: np.ndarray) -> tuple[float, float]:
    """Welch's t-test for unequal variances.

    Uses normal approximation for p-value via math.erfc.
    Accurate for n > 30 (our typical n is 50-200).

    Args:
        a: Sample A (1-D, NaN dropped).
        b: Sample B (1-D, NaN dropped).

    Returns:
        (t_stat, p_value) tuple. Two-tailed p-value.
    """
    a = a[~np.isnan(a)]
    b = b[~np.isnan(b)]

    n1, n2 = len(a), len(b)
    if n1 < 2 or n2 < 2:
        return 0.0, 1.0

    m1, m2 = float(np.mean(a)), float(np.mean(b))
    s1 = float(np.var(a, ddof=1))
    s2 = float(np.var(b, ddof=1))

    denom = math.sqrt(s1 / n1 + s2 / n2)
    if denom == 0:
        return 0.0, 1.0

    t = (m1 - m2) / denom
    # Normal approximation: p = erfc(|t| / sqrt(2))
    p = math.erfc(abs(t) / math.sqrt(2))
    return t, p


def cohens_d(a: np.ndarray, b: np.ndarray) -> float:
    """Compute Cohen's d effect size (pooled std).

    Args:
        a: Sample A (1-D, NaN dropped).
        b: Sample B (1-D, NaN dropped).

    Returns:
        Cohen's d. Positive means a > b.
    """
    a = a[~np.isnan(a)]
    b = b[~np.isnan(b)]

    n1, n2 = len(a), len(b)
    if n1 < 2 or n2 < 2:
        return 0.0

    m1, m2 = float(np.mean(a)), float(np.mean(b))
    s1 = float(np.var(a, ddof=1))
    s2 = float(np.var(b, ddof=1))

    pooled_std = math.sqrt(((n1 - 1) * s1 + (n2 - 1) * s2) / (n1 + n2 - 2))
    if pooled_std == 0:
        return 0.0

    return (m1 - m2) / pooled_std


def compare_distributions(
    conditional: np.ndarray,
    unconditional: np.ndarray,
    n_tests: int = 1,
) -> HypothesisTestResult:
    """Compare two return distributions with Bonferroni correction.

    Tests H_a: conditional mean != unconditional mean.
    Significance requires BOTH corrected p < 0.05 AND |d| >= 0.2.

    Args:
        conditional: Returns following events (1-D).
        unconditional: Baseline returns (1-D).
        n_tests: Number of horizons tested (for Bonferroni).

    Returns:
        HypothesisTestResult with all statistics.
    """
    t, p = welch_t_test(conditional, unconditional)
    d = cohens_d(conditional, unconditional)
    p_corr = min(p * n_tests, 1.0)
    sig = (p_corr < 0.05) and (abs(d) >= 0.2)

    return HypothesisTestResult(
        t_stat=t,
        p_value=p,
        p_value_corrected=p_corr,
        cohens_d=d,
        significant=sig,
    )


# =============================================================================
# Distribution Shape Analysis
# =============================================================================

def describe_shape(
    values: np.ndarray,
    threshold_sigma: float = 2.0,
) -> DistributionShape:
    """Analyze distribution shape and validate threshold calibration.

    Computes skewness, excess kurtosis, and the actual fraction of
    observations beyond ±threshold_sigma standard deviations.

    Args:
        values: 1-D numpy array (NaN values are dropped).
        threshold_sigma: Threshold in standard deviations to check.

    Returns:
        DistributionShape with shape metrics and threshold analysis.
    """
    values = values[~np.isnan(values)]
    n = len(values)
    if n < 3:
        return DistributionShape(
            skewness=0.0, excess_kurtosis=0.0,
            pct_beyond=0.0, pct_expected=0.0,
            threshold_percentile=100.0, n=n,
        )

    mean = float(np.mean(values))
    std = float(np.std(values, ddof=1))

    if std == 0:
        return DistributionShape(
            skewness=0.0, excess_kurtosis=0.0,
            pct_beyond=0.0, pct_expected=0.0,
            threshold_percentile=100.0, n=n,
        )

    z = (values - mean) / std

    # Skewness: E[(z)^3]
    skew = float(np.mean(z ** 3))
    # Excess kurtosis: E[(z)^4] - 3
    kurt = float(np.mean(z ** 4) - 3.0)

    # Actual % beyond ±threshold
    beyond = float(np.mean(np.abs(z) > threshold_sigma) * 100)

    # Theoretical % under normal: 2 * Φ(-threshold) = erfc(threshold / sqrt(2))
    expected = float(math.erfc(threshold_sigma / math.sqrt(2)) * 100)

    # What percentile does +threshold correspond to?
    threshold_val = mean + threshold_sigma * std
    pctl = float(np.mean(values <= threshold_val) * 100)

    return DistributionShape(
        skewness=round(skew, 3),
        excess_kurtosis=round(kurt, 3),
        pct_beyond=round(beyond, 2),
        pct_expected=round(expected, 2),
        threshold_percentile=round(pctl, 2),
        n=n,
    )


# =============================================================================
# Permutation Test
# =============================================================================

def permutation_test(
    event_values: np.ndarray,
    population_values: np.ndarray,
    n_perms: int = 10000,
    seed: int = 42,
) -> float:
    """Exact p-value via permutation test.

    Tests whether the mean of event_values is significantly different
    from what you'd get by randomly sampling from population_values.
    No distributional assumptions.

    Args:
        event_values: Observed returns after events (1-D).
        population_values: All available returns to permute from (1-D).
        n_perms: Number of permutation iterations.
        seed: Random seed.

    Returns:
        Two-tailed p-value (fraction of permutations with |mean| >= |observed|).
    """
    event_values = event_values[~np.isnan(event_values)]
    population_values = population_values[~np.isnan(population_values)]

    n_events = len(event_values)
    n_pop = len(population_values)
    if n_events == 0 or n_pop < n_events:
        return 1.0

    observed_mean = float(np.mean(event_values))
    rng = np.random.default_rng(seed)

    count_extreme = 0
    for _ in range(n_perms):
        perm_sample = rng.choice(population_values, size=n_events, replace=False)
        if abs(float(np.mean(perm_sample))) >= abs(observed_mean):
            count_extreme += 1

    return count_extreme / n_perms
