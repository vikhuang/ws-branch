"""Statistical tests for trading metrics.

Provides hypothesis testing for timing alpha and other metrics:
- Permutation test: Non-parametric test for timing ability
- Significance interpretation

Key insight from analysis:
- ~5% of brokers show p < 0.05, which is exactly what we'd expect by chance
- Most "significant" timing ability is likely noise
- True market timers are rare
"""

from dataclasses import dataclass
import random
from typing import Sequence, Callable

from pnl_analytics.domain.metrics.timing_alpha import calculate_timing_alpha


# =============================================================================
# Data Classes
# =============================================================================

@dataclass(frozen=True, slots=True)
class PermutationTestResult:
    """Result of a permutation test.

    Attributes:
        observed: The observed test statistic
        p_value: Two-tailed p-value
        n_permutations: Number of permutations performed
        n_extreme: Number of permutations with |stat| >= |observed|
    """
    observed: float
    p_value: float
    n_permutations: int
    n_extreme: int

    @property
    def is_significant(self) -> bool:
        """Check if result is significant at 5% level."""
        return self.p_value < 0.05

    @property
    def significance_label(self) -> str:
        """Get significance label for display."""
        if self.p_value < 0.01:
            return "**"
        elif self.p_value < 0.05:
            return "*"
        elif self.p_value < 0.10:
            return "†"
        return ""


# =============================================================================
# Permutation Test
# =============================================================================

def permutation_test(
    net_buys: Sequence[float | int],
    daily_returns: Sequence[float],
    n_permutations: int = 200,
    seed: int | None = None,
) -> float:
    """Run permutation test for timing alpha, return p-value.

    Tests H0: No relationship between net_buys and future returns.
    By shuffling net_buys, we break any predictive relationship.

    Args:
        net_buys: Daily net buy amounts
        daily_returns: Daily stock returns (aligned with net_buys)
        n_permutations: Number of random shuffles (default 200)
        seed: Random seed for reproducibility (optional)

    Returns:
        Two-tailed p-value. Low p-value suggests timing ability.

    Example:
        >>> net_buys = [100, -50, 200, -100, 50, 75, -25, 150, -80, 30]
        >>> returns = [0.01, -0.02, 0.03, -0.01, 0.02, -0.01, 0.01, -0.02, 0.01, 0.0]
        >>> p = permutation_test(net_buys, returns, n_permutations=1000)
    """
    if seed is not None:
        random.seed(seed)

    # Calculate observed timing alpha
    observed_alpha = calculate_timing_alpha(net_buys, daily_returns)

    # Count how many permutations have |alpha| >= |observed|
    n_extreme = 0
    net_buys_list = list(net_buys)  # Make mutable copy

    for _ in range(n_permutations):
        shuffled = net_buys_list.copy()
        random.shuffle(shuffled)
        simulated_alpha = calculate_timing_alpha(shuffled, daily_returns)

        if abs(simulated_alpha) >= abs(observed_alpha):
            n_extreme += 1

    return n_extreme / n_permutations


def permutation_test_detailed(
    net_buys: Sequence[float | int],
    daily_returns: Sequence[float],
    n_permutations: int = 200,
    seed: int | None = None,
) -> PermutationTestResult:
    """Run permutation test with detailed results.

    Args:
        net_buys: Daily net buy amounts
        daily_returns: Daily stock returns
        n_permutations: Number of random shuffles
        seed: Random seed for reproducibility

    Returns:
        PermutationTestResult with all statistics
    """
    if seed is not None:
        random.seed(seed)

    observed_alpha = calculate_timing_alpha(net_buys, daily_returns)

    n_extreme = 0
    net_buys_list = list(net_buys)

    for _ in range(n_permutations):
        shuffled = net_buys_list.copy()
        random.shuffle(shuffled)
        simulated_alpha = calculate_timing_alpha(shuffled, daily_returns)

        if abs(simulated_alpha) >= abs(observed_alpha):
            n_extreme += 1

    p_value = n_extreme / n_permutations

    return PermutationTestResult(
        observed=observed_alpha,
        p_value=p_value,
        n_permutations=n_permutations,
        n_extreme=n_extreme,
    )


# =============================================================================
# Generic Permutation Test
# =============================================================================

def generic_permutation_test(
    data: Sequence,
    statistic_fn: Callable[[Sequence], float],
    n_permutations: int = 1000,
    seed: int | None = None,
) -> PermutationTestResult:
    """Generic permutation test for any statistic.

    Args:
        data: Data to permute
        statistic_fn: Function that computes test statistic from data
        n_permutations: Number of permutations
        seed: Random seed

    Returns:
        PermutationTestResult

    Example:
        >>> def mean_diff(x):
        ...     mid = len(x) // 2
        ...     return sum(x[:mid]) / mid - sum(x[mid:]) / (len(x) - mid)
        >>> result = generic_permutation_test([1,2,3,4,5,6], mean_diff)
    """
    if seed is not None:
        random.seed(seed)

    observed = statistic_fn(data)

    n_extreme = 0
    data_list = list(data)

    for _ in range(n_permutations):
        shuffled = data_list.copy()
        random.shuffle(shuffled)
        simulated = statistic_fn(shuffled)

        if abs(simulated) >= abs(observed):
            n_extreme += 1

    p_value = n_extreme / n_permutations

    return PermutationTestResult(
        observed=observed,
        p_value=p_value,
        n_permutations=n_permutations,
        n_extreme=n_extreme,
    )


# =============================================================================
# Interpretation Helpers
# =============================================================================

def interpret_significance(p_value: float) -> str:
    """Interpret p-value in plain language.

    Args:
        p_value: P-value from statistical test

    Returns:
        Human-readable interpretation
    """
    if p_value < 0.01:
        return "Highly significant (p < 0.01)"
    elif p_value < 0.05:
        return "Significant (p < 0.05)"
    elif p_value < 0.10:
        return "Marginally significant (p < 0.10)"
    else:
        return "Not significant"


def expected_false_positives(n_tests: int, alpha: float = 0.05) -> float:
    """Calculate expected number of false positives.

    When running many tests, some will be significant by chance.

    Args:
        n_tests: Number of tests performed
        alpha: Significance level (default 0.05)

    Returns:
        Expected number of false positives

    Example:
        >>> expected_false_positives(940, 0.05)
        47.0  # ~47 brokers significant by chance alone
    """
    return n_tests * alpha
