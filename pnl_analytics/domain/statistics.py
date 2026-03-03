"""General-purpose statistical tests.

Provides Welch's t-test, Cohen's d, and distribution comparison
without scipy dependency. Uses math.erfc for p-value approximation
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
