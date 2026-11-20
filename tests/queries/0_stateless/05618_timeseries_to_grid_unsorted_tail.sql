-- Tags: distributed

-- Tests the unsorted-tail storage of the `timeSeries*ToGrid` aggregate functions: a bucket keeps a sorted prefix and
-- appends out-of-order samples to an unsorted tail, which is sorted and merged into the prefix when the state is read.
-- Every case puts all samples into ONE bucket (a single grid point whose window covers everything) and must equal the
-- same functions over explicitly sorted input with unique timestamps (the largest real value wins at a duplicate timestamp).

SET allow_experimental_time_series_aggregate_functions = 1;
SET optimize_aggregation_in_order = 0;

DROP TABLE IF EXISTS ts_tail;
DROP TABLE IF EXISTS ts_tail_sorted;
DROP VIEW IF EXISTS ts_tail_expected;
DROP TABLE IF EXISTS ts_tail_states;

-- `n` is the arrival order of the samples within the case `c`.
CREATE TABLE ts_tail (c UInt8, n UInt32, ts DateTime64(3, 'UTC'), v Float64) ENGINE = MergeTree ORDER BY (c, n) SETTINGS index_granularity = 64;
CREATE TABLE ts_tail_sorted (c UInt8, ts DateTime64(3, 'UTC'), v Float64) ENGINE = MergeTree ORDER BY (c, ts);

-- Every case starts with a sorted prefix of 2000 samples (n < 2000): ts = 10, 20, ..., 20000, a sawtooth value.
-- The tails use `(i * 37) % 500`, a permutation of 0..499, so they arrive out of order.
-- Case 1: the tail lies entirely after the prefix.
INSERT INTO ts_tail SELECT 1, number, toDateTime64(if(number < 2000, (number + 1) * 10, 30000 + ((number - 2000) * 37 % 500) * 10), 3, 'UTC'), if(number < 2000, number % 13, (number - 2000) % 11)
FROM numbers(2500);
-- Case 2: the tail interleaves with the prefix (5, 15, 25, ...).
INSERT INTO ts_tail SELECT 2, number, toDateTime64(if(number < 2000, (number + 1) * 10, ((number - 2000) * 37 % 500) * 10 + 5), 3, 'UTC'), if(number < 2000, number % 13, (number - 2000) % 11)
FROM numbers(2500);
-- Case 3: the tail repeats timestamps of the prefix with different values (larger and smaller).
INSERT INTO ts_tail SELECT 3, number, toDateTime64(if(number < 2000, (number + 1) * 10, ((number - 2000) * 37 % 500 + 1) * 10), 3, 'UTC'), if(number < 2000, number % 13, (number - 2000) * 7 % 17)
FROM numbers(2500);
-- Case 4: the tail interleaves with the prefix and repeats its own timestamps with different values (500 + 500 tail samples).
INSERT INTO ts_tail SELECT 4, number,
    toDateTime64(multiIf(number < 2000, (number + 1) * 10, number < 2500, ((number - 2000) * 37 % 500) * 10 + 5, ((number - 2500) * 13 % 500) * 10 + 5), 3, 'UTC'),
    multiIf(number < 2000, number % 13, number < 2500, (number - 2000) % 11, (number - 2500) * 5 % 19)
FROM numbers(3000);
-- Case 5: a descending run, so the sorted prefix is a single sample.
INSERT INTO ts_tail SELECT 5, number, toDateTime64((2000 - number) * 10, 3, 'UTC'), number % 13
FROM numbers(2000);
-- Case 6: the tail lies entirely before the prefix.
INSERT INTO ts_tail SELECT 6, number, toDateTime64(if(number < 2000, 30000 + (number + 1) * 10, ((number - 2000) * 37 % 500 + 1) * 10), 3, 'UTC'), if(number < 2000, number % 13, (number - 2000) % 11)
FROM numbers(2500);
-- Case 7: the tail lies entirely before the prefix and repeats its own timestamps with different values (500 timestamps twice).
INSERT INTO ts_tail SELECT 7, number, toDateTime64(if(number < 2000, 30000 + (number + 1) * 10, ((number - 2000) * 37 % 500 + 1) * 10), 3, 'UTC'), if(number < 2000, number % 13, (number - 2000) * 7 % 17)
FROM numbers(3000);
-- Case 8: the tail starts with the last timestamp of the prefix and goes on in order (20000, 20010, ..., 20990), then interleaves with the prefix.
INSERT INTO ts_tail SELECT 8, number,
    toDateTime64(multiIf(number < 2000, (number + 1) * 10, number < 2100, 20000 + (number - 2000) * 10, ((number - 2100) * 37 % 400) * 10 + 5), 3, 'UTC'),
    if(number < 2000, number % 13, (number - 2000) % 11)
FROM numbers(2500);

-- The reference input: sorted by timestamp, one sample per timestamp.
INSERT INTO ts_tail_sorted SELECT c, ts, max(v) FROM ts_tail GROUP BY c, ts;

CREATE VIEW ts_tail_expected AS
SELECT
    c,
    (timeSeriesRateToGrid(100000, 100000, 1, 100000)(ts, v),
     timeSeriesChangesToGrid(100000, 100000, 1, 100000)(ts, v),
     timeSeriesLastToGrid(100000, 100000, 1, 100000)(ts, v),
     timeSeriesResetsToGrid(100000, 100000, 1, 100000)(ts, v)) AS r
FROM ts_tail_sorted
GROUP BY c
SETTINGS max_threads = 1;

-- The reference must be non-trivial: all cases have a result and the cases differ from each other.
SELECT 'reference is non-trivial (8 7 1):';
SELECT count(), uniqExact(r.1), min(r.2[1] > 0) FROM ts_tail_expected;

-- (a, b, c) Scalar adds, one row at a time in arrival order, in one block and split into small blocks
-- (the unsorted tail starts in the middle of a block, and then continues across block boundaries).
SELECT 'scalar add, one block (all 1):';
SELECT c, a.r = e.r
FROM
(
    SELECT
        c,
        (timeSeriesRateToGrid(100000, 100000, 1, 100000)(ts, v),
         timeSeriesChangesToGrid(100000, 100000, 1, 100000)(ts, v),
         timeSeriesLastToGrid(100000, 100000, 1, 100000)(ts, v),
         timeSeriesResetsToGrid(100000, 100000, 1, 100000)(ts, v)) AS r
    FROM ts_tail
    GROUP BY c
    SETTINGS max_threads = 1
) AS a
INNER JOIN ts_tail_expected AS e USING (c)
ORDER BY c;

SELECT 'scalar add, small blocks (all 1):';
SELECT c, a.r = e.r
FROM
(
    SELECT
        c,
        (timeSeriesRateToGrid(100000, 100000, 1, 100000)(ts, v),
         timeSeriesChangesToGrid(100000, 100000, 1, 100000)(ts, v),
         timeSeriesLastToGrid(100000, 100000, 1, 100000)(ts, v),
         timeSeriesResetsToGrid(100000, 100000, 1, 100000)(ts, v)) AS r
    FROM ts_tail
    GROUP BY c
    SETTINGS max_threads = 1, max_block_size = 333
) AS a
INNER JOIN ts_tail_expected AS e USING (c)
ORDER BY c;

-- (d) `addMany` with array arguments: one array holding the prefix and the tail.
SELECT 'addMany, one array with prefix and tail (all 1):';
SELECT c, a.r = e.r
FROM
(
    SELECT
        c,
        (timeSeriesRateToGrid(100000, 100000, 1, 100000)(tss, vs),
         timeSeriesChangesToGrid(100000, 100000, 1, 100000)(tss, vs),
         timeSeriesLastToGrid(100000, 100000, 1, 100000)(tss, vs),
         timeSeriesResetsToGrid(100000, 100000, 1, 100000)(tss, vs)) AS r
    FROM (SELECT c, groupArray(ts) AS tss, groupArray(v) AS vs FROM ts_tail GROUP BY c SETTINGS max_threads = 1)
    GROUP BY c
) AS a
INNER JOIN ts_tail_expected AS e USING (c)
ORDER BY c;

-- (d) `addMany` called twice per state: the prefix array first and the tail array after it, so the second call
-- appends an out-of-order array to a non-empty sorted bucket; and the same arrays in the opposite order.
SELECT 'addMany, prefix array then tail array (all 1):';
SELECT c, a.r = e.r
FROM
(
    SELECT
        c,
        (timeSeriesRateToGrid(100000, 100000, 1, 100000)(tss, vs),
         timeSeriesChangesToGrid(100000, 100000, 1, 100000)(tss, vs),
         timeSeriesLastToGrid(100000, 100000, 1, 100000)(tss, vs),
         timeSeriesResetsToGrid(100000, 100000, 1, 100000)(tss, vs)) AS r
    FROM (SELECT c, n >= 2000 AS part, groupArray(ts) AS tss, groupArray(v) AS vs FROM ts_tail GROUP BY c, part ORDER BY c, part SETTINGS max_threads = 1)
    GROUP BY c
    SETTINGS max_threads = 1
) AS a
INNER JOIN ts_tail_expected AS e USING (c)
ORDER BY c;

SELECT 'addMany, tail array then prefix array (all 1):';
SELECT c, a.r = e.r
FROM
(
    SELECT
        c,
        (timeSeriesRateToGrid(100000, 100000, 1, 100000)(tss, vs),
         timeSeriesChangesToGrid(100000, 100000, 1, 100000)(tss, vs),
         timeSeriesLastToGrid(100000, 100000, 1, 100000)(tss, vs),
         timeSeriesResetsToGrid(100000, 100000, 1, 100000)(tss, vs)) AS r
    FROM (SELECT c, n >= 2000 AS part, groupArray(ts) AS tss, groupArray(v) AS vs FROM ts_tail GROUP BY c, part ORDER BY c, part DESC SETTINGS max_threads = 1)
    GROUP BY c
    SETTINGS max_threads = 1
) AS a
INNER JOIN ts_tail_expected AS e USING (c)
ORDER BY c;

-- (e) `-State` / `-Merge` of partial states split at three points inside the prefix and the tail, so the states have
-- a prefix only, a prefix with a tail, and a tail only. The merge order is ascending (the first state is merged
-- into an empty accumulator) and descending (an unsorted state is merged into a non-empty accumulator).
SELECT 'merge of partial states, ascending (all 1):';
SELECT c, a.r = e.r
FROM
(
    SELECT
        c,
        (timeSeriesRateToGridMerge(100000, 100000, 1, 100000)(s1),
         timeSeriesChangesToGridMerge(100000, 100000, 1, 100000)(s2),
         timeSeriesLastToGridMerge(100000, 100000, 1, 100000)(s3),
         timeSeriesResetsToGridMerge(100000, 100000, 1, 100000)(s4)) AS r
    FROM
    (
        SELECT
            c,
            multiIf(n < 1000, 0, n < 2300, 1, 2) AS part,
            timeSeriesRateToGridState(100000, 100000, 1, 100000)(ts, v) AS s1,
            timeSeriesChangesToGridState(100000, 100000, 1, 100000)(ts, v) AS s2,
            timeSeriesLastToGridState(100000, 100000, 1, 100000)(ts, v) AS s3,
            timeSeriesResetsToGridState(100000, 100000, 1, 100000)(ts, v) AS s4
        FROM ts_tail
        GROUP BY c, part
        ORDER BY c, part
        SETTINGS max_threads = 1
    )
    GROUP BY c
    SETTINGS max_threads = 1
) AS a
INNER JOIN ts_tail_expected AS e USING (c)
ORDER BY c;

SELECT 'merge of partial states, descending (all 1):';
SELECT c, a.r = e.r
FROM
(
    SELECT
        c,
        (timeSeriesRateToGridMerge(100000, 100000, 1, 100000)(s1),
         timeSeriesChangesToGridMerge(100000, 100000, 1, 100000)(s2),
         timeSeriesLastToGridMerge(100000, 100000, 1, 100000)(s3),
         timeSeriesResetsToGridMerge(100000, 100000, 1, 100000)(s4)) AS r
    FROM
    (
        SELECT
            c,
            multiIf(n < 1000, 0, n < 2300, 1, 2) AS part,
            timeSeriesRateToGridState(100000, 100000, 1, 100000)(ts, v) AS s1,
            timeSeriesChangesToGridState(100000, 100000, 1, 100000)(ts, v) AS s2,
            timeSeriesLastToGridState(100000, 100000, 1, 100000)(ts, v) AS s3,
            timeSeriesResetsToGridState(100000, 100000, 1, 100000)(ts, v) AS s4
        FROM ts_tail
        GROUP BY c, part
        ORDER BY c, part DESC
        SETTINGS max_threads = 1
    )
    GROUP BY c
    SETTINGS max_threads = 1
) AS a
INNER JOIN ts_tail_expected AS e USING (c)
ORDER BY c;

-- (e) Parallel `GROUP BY`: every thread builds states from its own ranges of the table, so the states have tails
-- and are merged in memory (two-level aggregation merges them in parallel).
SELECT 'parallel GROUP BY (all 1):';
SELECT c, a.r = e.r
FROM
(
    SELECT
        c,
        (timeSeriesRateToGrid(100000, 100000, 1, 100000)(ts, v),
         timeSeriesChangesToGrid(100000, 100000, 1, 100000)(ts, v),
         timeSeriesLastToGrid(100000, 100000, 1, 100000)(ts, v),
         timeSeriesResetsToGrid(100000, 100000, 1, 100000)(ts, v)) AS r
    FROM ts_tail
    GROUP BY c
    SETTINGS max_threads = 4, max_block_size = 100, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1
) AS a
INNER JOIN ts_tail_expected AS e USING (c)
ORDER BY c;

-- (e) Serialization roundtrip. The states are stored while still unsorted in memory, so `serialize` writes a sorted
-- copy. `kind` 0 stores one state per case, `kind` 1 stores three partial states per case; reading merges deserialized states.
CREATE TABLE ts_tail_states
(
    kind UInt8,
    c UInt8,
    part UInt8,
    st AggregateFunction(timeSeriesRateToGrid(100000, 100000, 1, 100000), DateTime64(3, 'UTC'), Float64),
    sl AggregateFunction(timeSeriesLastToGrid(100000, 100000, 1, 100000), DateTime64(3, 'UTC'), Float64)
)
ENGINE = AggregatingMergeTree ORDER BY (kind, c, part);

INSERT INTO ts_tail_states
SELECT 0, c, 0, timeSeriesRateToGridState(100000, 100000, 1, 100000)(ts, v), timeSeriesLastToGridState(100000, 100000, 1, 100000)(ts, v)
FROM ts_tail GROUP BY c SETTINGS max_threads = 1;

INSERT INTO ts_tail_states
SELECT 1, c, multiIf(n < 1000, 0, n < 2300, 1, 2) AS part, timeSeriesRateToGridState(100000, 100000, 1, 100000)(ts, v), timeSeriesLastToGridState(100000, 100000, 1, 100000)(ts, v)
FROM ts_tail GROUP BY c, part SETTINGS max_threads = 1;

SELECT 'serialization roundtrip (all 1):';
SELECT kind, c, (timeSeriesRateToGridMerge(100000, 100000, 1, 100000)(st), timeSeriesLastToGridMerge(100000, 100000, 1, 100000)(sl)) = (e.r.1, e.r.3)
FROM ts_tail_states
INNER JOIN ts_tail_expected AS e USING (c)
GROUP BY kind, c, e.r
ORDER BY kind, c;

-- Serialized-state merge over two loopback shards: each shard sends states built from unsorted input, and every
-- sample arrives twice, so the merge deduplicates equal timestamps.
SELECT 'remote() (all 1):';
SELECT c, a.r = e.r
FROM
(
    SELECT
        c,
        (timeSeriesRateToGrid(100000, 100000, 1, 100000)(ts, v),
         timeSeriesChangesToGrid(100000, 100000, 1, 100000)(ts, v),
         timeSeriesLastToGrid(100000, 100000, 1, 100000)(ts, v),
         timeSeriesResetsToGrid(100000, 100000, 1, 100000)(ts, v)) AS r
    FROM remote('127.0.0.{1,2}', currentDatabase(), ts_tail)
    GROUP BY c
) AS a
INNER JOIN ts_tail_expected AS e USING (c)
ORDER BY c;

DROP TABLE ts_tail_states;
DROP VIEW ts_tail_expected;
DROP TABLE ts_tail_sorted;
DROP TABLE ts_tail;
