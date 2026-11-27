-- Tests the `timeSeries*ToGrid` aggregate functions when many series with the same timestamps go into one state,
-- so the unsorted tail covers most of the bucket and the sorted prefix is short (case 1) or long (case 2).
-- Every case puts all samples into ONE bucket and must equal the same functions over explicitly sorted input
-- with unique timestamps (the largest real value wins at a duplicate timestamp).

SET allow_experimental_time_series_aggregate_functions = 1;
SET optimize_aggregation_in_order = 0;

DROP TABLE IF EXISTS ts_equal;
DROP TABLE IF EXISTS ts_equal_sorted;
DROP VIEW IF EXISTS ts_equal_expected;
DROP TABLE IF EXISTS ts_equal_states;

-- `n` is the arrival order of the samples within the case `c`.
CREATE TABLE ts_equal (c UInt8, n UInt32, ts DateTime64(3, 'UTC'), v Float64) ENGINE = MergeTree ORDER BY (c, n) SETTINGS index_granularity = 64;
CREATE TABLE ts_equal_sorted (c UInt8, ts DateTime64(3, 'UTC'), v Float64) ENGINE = MergeTree ORDER BY (c, ts);

-- Case 1: 50 series of 200 samples with the same timestamps 1..200 and different values, one series after another.
INSERT INTO ts_equal SELECT 1, number, toDateTime64(number % 200 + 1, 3, 'UTC'), number % 200 % 29 + intDiv(number, 200) % 3 * 0.5
FROM numbers(10000);
-- Case 2: one long series of 3000 samples (a long sorted prefix) followed by 40 shorter series that overlap it.
INSERT INTO ts_equal SELECT 2, number,
    toDateTime64(if(number < 3000, number + 1, ((intDiv(number - 3000, 50) * 17 + (number - 3000) % 50 * 61) % 3000) + 1), 3, 'UTC'),
    if(number < 3000, number % 13, (number * 5) % 19)
FROM numbers(5000);
-- Case 3: as case 1, but the series go in descending timestamp order, so the sorted prefix is a single sample.
INSERT INTO ts_equal SELECT 3, number, toDateTime64(200 - number % 200, 3, 'UTC'), number % 200 % 29 + intDiv(number, 200) % 3 * 0.5
FROM numbers(10000);

INSERT INTO ts_equal_sorted SELECT c, ts, max(v) FROM ts_equal GROUP BY c, ts;

CREATE VIEW ts_equal_expected AS
SELECT
    c,
    (timeSeriesRateToGrid(4000, 4000, 1, 4000)(ts, v),
     timeSeriesChangesToGrid(4000, 4000, 1, 4000)(ts, v),
     timeSeriesLastToGrid(4000, 4000, 1, 4000)(ts, v),
     timeSeriesResetsToGrid(4000, 4000, 1, 4000)(ts, v)) AS r
FROM ts_equal_sorted
GROUP BY c
SETTINGS max_threads = 1;

SELECT 'reference is non-trivial (3 3 1):';
SELECT count(), uniqExact(r.1), min(r.2[1] > 0) FROM ts_equal_expected;

SELECT 'scalar add (all 1):';
SELECT c, a.r = e.r
FROM
(
    SELECT
        c,
        (timeSeriesRateToGrid(4000, 4000, 1, 4000)(ts, v),
         timeSeriesChangesToGrid(4000, 4000, 1, 4000)(ts, v),
         timeSeriesLastToGrid(4000, 4000, 1, 4000)(ts, v),
         timeSeriesResetsToGrid(4000, 4000, 1, 4000)(ts, v)) AS r
    FROM ts_equal
    GROUP BY c
    SETTINGS max_threads = 1
) AS a
INNER JOIN ts_equal_expected AS e USING (c)
ORDER BY c;

SELECT 'parallel GROUP BY (all 1):';
SELECT c, a.r = e.r
FROM
(
    SELECT
        c,
        (timeSeriesRateToGrid(4000, 4000, 1, 4000)(ts, v),
         timeSeriesChangesToGrid(4000, 4000, 1, 4000)(ts, v),
         timeSeriesLastToGrid(4000, 4000, 1, 4000)(ts, v),
         timeSeriesResetsToGrid(4000, 4000, 1, 4000)(ts, v)) AS r
    FROM ts_equal
    GROUP BY c
    SETTINGS max_threads = 4, max_block_size = 100, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1
) AS a
INNER JOIN ts_equal_expected AS e USING (c)
ORDER BY c;

-- Partial states, one per 1000 samples, merged in ascending and descending order.
SELECT 'merge of partial states, ascending (all 1):';
SELECT c, a.r = e.r
FROM
(
    SELECT
        c,
        (timeSeriesRateToGridMerge(4000, 4000, 1, 4000)(s1),
         timeSeriesChangesToGridMerge(4000, 4000, 1, 4000)(s2),
         timeSeriesLastToGridMerge(4000, 4000, 1, 4000)(s3),
         timeSeriesResetsToGridMerge(4000, 4000, 1, 4000)(s4)) AS r
    FROM
    (
        SELECT
            c,
            intDiv(n, 1000) AS part,
            timeSeriesRateToGridState(4000, 4000, 1, 4000)(ts, v) AS s1,
            timeSeriesChangesToGridState(4000, 4000, 1, 4000)(ts, v) AS s2,
            timeSeriesLastToGridState(4000, 4000, 1, 4000)(ts, v) AS s3,
            timeSeriesResetsToGridState(4000, 4000, 1, 4000)(ts, v) AS s4
        FROM ts_equal
        GROUP BY c, part
        ORDER BY c, part
        SETTINGS max_threads = 1
    )
    GROUP BY c
    SETTINGS max_threads = 1
) AS a
INNER JOIN ts_equal_expected AS e USING (c)
ORDER BY c;

SELECT 'merge of partial states, descending (all 1):';
SELECT c, a.r = e.r
FROM
(
    SELECT
        c,
        (timeSeriesRateToGridMerge(4000, 4000, 1, 4000)(s1),
         timeSeriesChangesToGridMerge(4000, 4000, 1, 4000)(s2),
         timeSeriesLastToGridMerge(4000, 4000, 1, 4000)(s3),
         timeSeriesResetsToGridMerge(4000, 4000, 1, 4000)(s4)) AS r
    FROM
    (
        SELECT
            c,
            intDiv(n, 1000) AS part,
            timeSeriesRateToGridState(4000, 4000, 1, 4000)(ts, v) AS s1,
            timeSeriesChangesToGridState(4000, 4000, 1, 4000)(ts, v) AS s2,
            timeSeriesLastToGridState(4000, 4000, 1, 4000)(ts, v) AS s3,
            timeSeriesResetsToGridState(4000, 4000, 1, 4000)(ts, v) AS s4
        FROM ts_equal
        GROUP BY c, part
        ORDER BY c, part DESC
        SETTINGS max_threads = 1
    )
    GROUP BY c
    SETTINGS max_threads = 1
) AS a
INNER JOIN ts_equal_expected AS e USING (c)
ORDER BY c;

-- The states are stored while still unsorted in memory, so `serialize` writes a sorted copy.
CREATE TABLE ts_equal_states
(
    c UInt8,
    st AggregateFunction(timeSeriesRateToGrid(4000, 4000, 1, 4000), DateTime64(3, 'UTC'), Float64),
    sl AggregateFunction(timeSeriesLastToGrid(4000, 4000, 1, 4000), DateTime64(3, 'UTC'), Float64)
)
ENGINE = AggregatingMergeTree ORDER BY c;

INSERT INTO ts_equal_states
SELECT c, timeSeriesRateToGridState(4000, 4000, 1, 4000)(ts, v), timeSeriesLastToGridState(4000, 4000, 1, 4000)(ts, v)
FROM ts_equal GROUP BY c SETTINGS max_threads = 1;

SELECT 'serialization roundtrip (all 1):';
SELECT c, (timeSeriesRateToGridMerge(4000, 4000, 1, 4000)(st), timeSeriesLastToGridMerge(4000, 4000, 1, 4000)(sl)) = (e.r.1, e.r.3)
FROM ts_equal_states
INNER JOIN ts_equal_expected AS e USING (c)
GROUP BY c, e.r
ORDER BY c;

DROP TABLE ts_equal_states;
DROP VIEW ts_equal_expected;
DROP TABLE ts_equal_sorted;
DROP TABLE ts_equal;
