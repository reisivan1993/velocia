"""Median, p95, CV and the oriented ratio of medians with a seeded percentile bootstrap CI.

Orientation (03 §5): > 1 always means "better than the reference" — reference / candidate for
lower-is-better measures, candidate / reference for higher-is-better ones. For wall time the
ratio is the speedup.

Pure Python on purpose: inputs are a handful of repetitions per cell, and results must be
reproducible bit for bit from the seed.
"""

import math
import random
import statistics
from dataclasses import dataclass

from velocia.config.models import Direction

MIN_RECOMMENDED_N = 5


@dataclass(frozen=True)
class Summary:
    n: int
    median: float | None
    p95: float | None
    cv: float | None  # population stdev / mean
    cv_flag: bool


@dataclass(frozen=True)
class Ratio:
    ratio: float | None
    ci_low: float | None
    ci_high: float | None
    small_n: bool  # either side has fewer than MIN_RECOMMENDED_N values


def percentile(values: list[float], q: float) -> float:
    """Linear interpolation between closest ranks (numpy's default method)."""
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    pos = (len(ordered) - 1) * q
    lo, hi = math.floor(pos), math.ceil(pos)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (pos - lo)


def summarize(values: list[float], cv_threshold: float) -> Summary:
    if not values:
        return Summary(0, None, None, None, False)
    mean = statistics.fmean(values)
    cv = statistics.pstdev(values) / mean if len(values) > 1 and mean != 0 else None
    return Summary(
        n=len(values),
        median=statistics.median(values),
        p95=percentile(values, 0.95),
        cv=cv,
        cv_flag=cv is not None and cv > cv_threshold,
    )


def _oriented(candidate: float, reference: float, direction: Direction) -> float | None:
    if direction is Direction.LOWER:
        return reference / candidate if candidate != 0 else None
    return candidate / reference if reference != 0 else None


def ratio_ci(
    candidate: list[float],
    reference: list[float],
    direction: Direction,
    resamples: int,
    seed: int,
    confidence: float = 0.95,
) -> Ratio:
    """Ratio of medians with a percentile bootstrap CI.

    Candidate and reference repetitions are resampled independently (design §5.5).
    """
    if not candidate or not reference:
        return Ratio(None, None, None, True)
    point = _oriented(statistics.median(candidate), statistics.median(reference), direction)
    small_n = min(len(candidate), len(reference)) < MIN_RECOMMENDED_N
    if point is None:
        return Ratio(None, None, None, small_n)
    rng = random.Random(seed)
    boots = []
    for _ in range(resamples):
        c = statistics.median(rng.choices(candidate, k=len(candidate)))
        r = statistics.median(rng.choices(reference, k=len(reference)))
        value = _oriented(c, r, direction)
        if value is not None:
            boots.append(value)
    if not boots:
        return Ratio(point, None, None, small_n)
    alpha = (1 - confidence) / 2
    return Ratio(point, percentile(boots, alpha), percentile(boots, 1 - alpha), small_n)
