-- Tests that the samples of a `timeSeries*ToGrid` bucket are the same as the deduplicated sorted input when the same timestamps
-- come repeatedly, in any order, with different values, for sample counts around the capacity of the bucket buffer.

SET allow_experimental_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS ts_growth;
CREATE TABLE ts_growth (n UInt32, ord UInt64, ts DateTime64(3, 'UTC'), v Float64) ENGINE = MergeTree ORDER BY (n, ord);

-- For each n in 1..70: 5 passes over n timestamps, the passes go in ascending, descending and shuffled orders.
INSERT INTO ts_growth
SELECT n, pass * 1000 + if(pass % 3 = 0, i, if(pass % 3 = 1, 999 - i, intHash64(n * 10000 + pass * 100 + i) % 1000)) AS ord,
    toDateTime64(100 + i, 3, 'UTC'), toFloat64((i * 7 + pass * 13) % 11)
FROM (SELECT number + 1 AS n FROM numbers(70)) AS a
ARRAY JOIN range(5) AS pass
ARRAY JOIN range(n) AS i;

CREATE VIEW ts_growth_expected AS
SELECT n, timeSeriesRateToGrid(0, 200, 200, 200)(ts, v) AS r, timeSeriesChangesToGrid(0, 200, 200, 200)(ts, v) AS c
FROM (SELECT n, ts, max(v) AS v FROM ts_growth GROUP BY n, ts ORDER BY n, ts)
GROUP BY n;

SELECT 'scalar add, one thread (all 1):';
SELECT countIf(a.r = e.r AND a.c = e.c), count() FROM
(SELECT n, timeSeriesRateToGrid(0, 200, 200, 200)(ts, v) AS r, timeSeriesChangesToGrid(0, 200, 200, 200)(ts, v) AS c
 FROM ts_growth GROUP BY n SETTINGS max_threads = 1) AS a
INNER JOIN ts_growth_expected AS e USING (n);

SELECT 'parallel, small blocks (all 1):';
SELECT countIf(a.r = e.r AND a.c = e.c), count() FROM
(SELECT n, timeSeriesRateToGrid(0, 200, 200, 200)(ts, v) AS r, timeSeriesChangesToGrid(0, 200, 200, 200)(ts, v) AS c
 FROM ts_growth GROUP BY n SETTINGS max_threads = 4, max_block_size = 7) AS a
INNER JOIN ts_growth_expected AS e USING (n);

-- addMany with arrays: the 5 passes are added one after another to one bucket.
SELECT 'addMany (all 1):';
SELECT countIf(a.r = e.r AND a.c = e.c), count() FROM
(SELECT n, timeSeriesRateToGrid(0, 200, 200, 200)(tss, vs) AS r, timeSeriesChangesToGrid(0, 200, 200, 200)(tss, vs) AS c
 FROM (SELECT n, groupArray(ts) AS tss, groupArray(v) AS vs FROM (SELECT n, ts, v FROM ts_growth ORDER BY n, ord) GROUP BY n) GROUP BY n) AS a
INNER JOIN ts_growth_expected AS e USING (n);

DROP VIEW ts_growth_expected;
DROP TABLE ts_growth;
