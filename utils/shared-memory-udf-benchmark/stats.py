#!/usr/bin/env python3
"""Summary statistics for the benchmark runners.

Usage: stats.py summary FILE           -> median q1 q3  (seconds, of the samples in FILE)
       stats.py ratio BASE_FILE FILE   -> median q1 q3 of FILE, then speedup = median(BASE)/median(FILE)
                                          with a 95% bootstrap confidence interval

The location estimate is the median: run-to-run noise on a shared machine is one-sided (a sample
can only be slowed down, never sped up), so the mean would follow the outliers. The spread is the
interquartile range for the same reason. The interval on the speedup is a percentile bootstrap of
the ratio of medians (10000 resamples of each side); a speedup whose interval contains 1.0 is
reported as parity, whatever its point estimate says.
"""

import random
import statistics
import sys


def read_samples(path):
    with open(path) as f:
        samples = [float(line) for line in f if line.strip()]
    if len(samples) < 3:
        sys.exit(f"{path}: need at least 3 samples, got {len(samples)}")
    return samples


def quartiles(samples):
    q1, _, q3 = statistics.quantiles(samples, n=4, method="inclusive")
    return statistics.median(samples), q1, q3


def bootstrap_ratio(base, other, resamples=10000, seed=1):
    rng = random.Random(seed)
    ratios = []
    for _ in range(resamples):
        b = statistics.median(rng.choices(base, k=len(base)))
        o = statistics.median(rng.choices(other, k=len(other)))
        ratios.append(b / o)
    ratios.sort()
    return ratios[int(0.025 * resamples)], ratios[int(0.975 * resamples) - 1]


def main():
    mode = sys.argv[1]
    if mode == "summary":
        med, q1, q3 = quartiles(read_samples(sys.argv[2]))
        print(f"{med:.3f} {q1:.3f} {q3:.3f}")
    elif mode == "ratio":
        base = read_samples(sys.argv[2])
        other = read_samples(sys.argv[3])
        med, q1, q3 = quartiles(other)
        lo, hi = bootstrap_ratio(base, other)
        print(f"{med:.3f} {q1:.3f} {q3:.3f} {statistics.median(base) / med:.2f} {lo:.2f} {hi:.2f}")
    else:
        sys.exit(f"unknown mode {mode}")


if __name__ == "__main__":
    main()
