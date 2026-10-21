import math
import random
import statistics

import pytest

from velocia.config.models import Direction
from velocia.stats.summary import percentile, ratio_ci, summarize


def test_percentile_matches_linear_interpolation() -> None:
    assert percentile([1, 2, 3, 4], 0.5) == 2.5
    assert percentile([1, 2, 3, 4, 5], 0.95) == pytest.approx(4.8)
    assert percentile([7], 0.95) == 7


def test_summary_and_cv_flag() -> None:
    s = summarize([10, 10, 10, 10, 10], cv_threshold=0.1)
    assert (s.n, s.median, s.cv, s.cv_flag) == (5, 10, 0, False)
    noisy = summarize([5, 10, 15, 10, 10], cv_threshold=0.1)
    assert noisy.cv_flag and noisy.cv == pytest.approx(statistics.pstdev([5, 10, 15, 10, 10]) / 10)
    assert summarize([], 0.1).median is None


def test_ratio_orientation() -> None:
    fast, slow = [1.0] * 5, [2.0] * 5
    # Lower is better: reference / candidate, so a faster candidate gives > 1.
    assert ratio_ci(fast, slow, Direction.LOWER, 1000, 1).ratio == 2.0
    # Higher is better: candidate / reference.
    assert ratio_ci(fast, slow, Direction.HIGHER, 1000, 1).ratio == 0.5


def test_ratio_is_reproducible_from_the_seed_and_flags_small_n() -> None:
    a, b = [1.0, 1.1, 0.9, 1.05, 0.95], [2.0, 2.2, 1.9, 2.1, 1.8]
    first = ratio_ci(a, b, Direction.LOWER, 2000, seed=42)
    assert first == ratio_ci(a, b, Direction.LOWER, 2000, seed=42)
    assert first.ci_low is not None and first.ci_high is not None
    assert first.ci_low <= first.ratio <= first.ci_high  # type: ignore[operator]
    assert not first.small_n
    assert ratio_ci(a[:3], b, Direction.LOWER, 100, 1).small_n
    assert ratio_ci([], b, Direction.LOWER, 100, 1).ratio is None


def test_bootstrap_coverage_on_log_normal_data() -> None:
    """The 95% CI contains the true ratio of medians in roughly 95% of experiments.

    Reduced size so it runs on every PR (200 trials of n = 20, 1,000 resamples); the band
    is wide enough for that trial count.
    """
    rng = random.Random(7)
    true_ratio = math.exp(0.0) / math.exp(-0.3)  # medians of the two log-normals
    hits = 0
    trials = 200
    for trial in range(trials):
        cand = [rng.lognormvariate(-0.3, 0.2) for _ in range(20)]
        ref = [rng.lognormvariate(0.0, 0.2) for _ in range(20)]
        r = ratio_ci(cand, ref, Direction.LOWER, 1000, seed=trial)
        assert r.ci_low is not None and r.ci_high is not None
        hits += r.ci_low <= true_ratio <= r.ci_high
    assert 0.88 <= hits / trials <= 0.99
