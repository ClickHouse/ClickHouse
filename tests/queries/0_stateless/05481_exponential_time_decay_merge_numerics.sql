SET allow_experimental_time_decay_aggregate_functions = 1;
SET max_threads = 1;

CREATE TABLE time_decay_merge_numerics (value Float64, time Float64) ENGINE = Memory;
INSERT INTO time_decay_merge_numerics VALUES (1e100, 0), (0, 400), (1e-300, 750);

-- A representable contribution must survive both incremental and single-step decay.
SELECT abs(exponentialTimeDecayedSum(1)(value, time) / 1.9016849634750064e-226 - 1) < 1e-12
FROM (SELECT * FROM time_decay_merge_numerics ORDER BY time);

SELECT abs(exponentialTimeDecayedSum(1)(value, time) / 1.9016849634750064e-226 - 1) < 1e-12
FROM (SELECT * FROM time_decay_merge_numerics ORDER BY indexOf([0., 750., 400.], time));

-- Independently aggregate the earliest row and the later rows, then merge in both orders.
SELECT abs(exponentialTimeDecayedSumMerge(1)(state) / 1.9016849634750064e-226 - 1) < 1e-12
FROM
(
    SELECT exponentialTimeDecayedSumState(1)(value, time) AS state
    FROM time_decay_merge_numerics
    GROUP BY time = 0
    ORDER BY max(time)
);

SELECT abs(exponentialTimeDecayedSumMerge(1)(state) / 1.9016849634750064e-226 - 1) < 1e-12
FROM
(
    SELECT exponentialTimeDecayedSumState(1)(value, time) AS state
    FROM time_decay_merge_numerics
    GROUP BY time = 0
    ORDER BY max(time) DESC
);

TRUNCATE TABLE time_decay_merge_numerics;
INSERT INTO time_decay_merge_numerics VALUES (1, -1e308), (0, 1e308);

-- The timestamp difference overflows, but the normalized distance is just two.
SELECT
    abs(exponentialTimeDecayedSum(1e308)(value, time) / exp(-2) - 1) < 1e-12,
    abs(exponentialTimeDecayedCount(1e308)(time) / (1 + exp(-2)) - 1) < 1e-12,
    abs(exponentialTimeDecayedAvg(1e308)(value, time) / (exp(-2) / (1 + exp(-2))) - 1) < 1e-12
FROM (SELECT * FROM time_decay_merge_numerics ORDER BY time);

SELECT
    abs(exponentialTimeDecayedSum(1e308)(value, time) / exp(-2) - 1) < 1e-12,
    abs(exponentialTimeDecayedCount(1e308)(time) / (1 + exp(-2)) - 1) < 1e-12,
    abs(exponentialTimeDecayedAvg(1e308)(value, time) / (exp(-2) / (1 + exp(-2))) - 1) < 1e-12
FROM (SELECT * FROM time_decay_merge_numerics ORDER BY time DESC);

SELECT
    abs(exponentialTimeDecayedSumMerge(1e308)(sum_state) / exp(-2) - 1) < 1e-12,
    abs(exponentialTimeDecayedCountMerge(1e308)(count_state) / (1 + exp(-2)) - 1) < 1e-12,
    abs(exponentialTimeDecayedAvgMerge(1e308)(avg_state) / (exp(-2) / (1 + exp(-2))) - 1) < 1e-12
FROM
(
    SELECT
        exponentialTimeDecayedSumState(1e308)(value, time) AS sum_state,
        exponentialTimeDecayedCountState(1e308)(time) AS count_state,
        exponentialTimeDecayedAvgState(1e308)(value, time) AS avg_state
    FROM time_decay_merge_numerics
    GROUP BY time
    ORDER BY time
);

SELECT
    abs(exponentialTimeDecayedSumMerge(1e308)(sum_state) / exp(-2) - 1) < 1e-12,
    abs(exponentialTimeDecayedCountMerge(1e308)(count_state) / (1 + exp(-2)) - 1) < 1e-12,
    abs(exponentialTimeDecayedAvgMerge(1e308)(avg_state) / (exp(-2) / (1 + exp(-2))) - 1) < 1e-12
FROM
(
    SELECT
        exponentialTimeDecayedSumState(1e308)(value, time) AS sum_state,
        exponentialTimeDecayedCountState(1e308)(time) AS count_state,
        exponentialTimeDecayedAvgState(1e308)(value, time) AS avg_state
    FROM time_decay_merge_numerics
    GROUP BY time
    ORDER BY time DESC
);

DROP TABLE time_decay_merge_numerics;
