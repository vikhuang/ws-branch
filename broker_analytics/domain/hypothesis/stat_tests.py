"""Step 5: Statistical test functions.

Wraps existing domain/statistics.py -- no new statistics code.

Signature: (HorizonReturns, HorizonReturns, params: dict)
           -> dict[int, HypothesisTestResult]
"""

import numpy as np

from broker_analytics.domain.statistics import (
    compare_distributions,
    HypothesisTestResult,
    permutation_test,
    welch_t_test,
    cohens_d,
)
from broker_analytics.domain.hypothesis.types import HorizonReturns


_EMPTY_RESULT = HypothesisTestResult(
    t_stat=0.0, p_value=1.0, p_value_corrected=1.0,
    cohens_d=0.0, significant=False,
)


def stat_test_parametric(
    event_returns: HorizonReturns,
    baseline_returns: HorizonReturns,
    params: dict,
) -> dict[int, HypothesisTestResult]:
    """Standard parametric test: Welch t-test + Cohen's d + Bonferroni.

    Directly delegates to compare_distributions() from domain/statistics.py.
    """
    n_tests = len(event_returns)
    results = {}
    for h, cond in event_returns.items():
        uncond = baseline_returns.get(h, np.array([]))
        if len(cond) < 3 or len(uncond) < 3:
            results[h] = _EMPTY_RESULT
            continue
        results[h] = compare_distributions(cond, uncond, n_tests=n_tests)
    return results


def stat_test_permutation(
    event_returns: HorizonReturns,
    baseline_returns: HorizonReturns,
    params: dict,
) -> dict[int, HypothesisTestResult]:
    """Permutation test + Cohen's d (like event_study service).

    params: n_perms (int, default 10000)
    """
    n_perms = params.get("n_perms", 10000)
    n_tests = len(event_returns)
    alpha = 0.05 / n_tests if n_tests > 0 else 0.05  # Bonferroni

    results = {}
    for h, cond in event_returns.items():
        uncond = baseline_returns.get(h, np.array([]))
        if len(cond) < 3 or len(uncond) < 3:
            results[h] = _EMPTY_RESULT
            continue

        t, _ = welch_t_test(cond, uncond)
        d = cohens_d(cond, uncond)
        perm_p = permutation_test(cond, uncond, n_perms=n_perms)
        sig = perm_p < alpha and abs(d) >= 0.2

        results[h] = HypothesisTestResult(
            t_stat=t,
            p_value=perm_p,
            p_value_corrected=min(perm_p * n_tests, 1.0),
            cohens_d=d,
            significant=sig,
        )

    return results
