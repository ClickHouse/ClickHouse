-- `studentTTestOneSample(x, mean)` keeps the population mean in its state. The mean was lost when the
-- state was merged into a state without values, which is how every merge starts, so the test was then
-- done against zero: `-Merge`, `finalizeAggregation`, `WITH TOTALS`, `WITH ROLLUP`, `WITH CUBE`, the
-- parts of an `AggregatingMergeTree` table and the shards of a distributed query all returned the
-- t-statistic 3.286335 instead of 1.095445 for the values below.

SELECT 'direct', round(r.1, 6), round(r.2, 6) FROM (SELECT studentTTestOneSample(x, 2.) AS r FROM values('x Float64', 1, 2, 4, 5));

SELECT 'merge', round(r.1, 6), round(r.2, 6)
FROM (SELECT studentTTestOneSampleMerge(s) AS r FROM (SELECT studentTTestOneSampleState(x, 2.) AS s FROM values('x Float64', 1, 2, 4, 5)));

SELECT 'finalizeAggregation', round(r.1, 6), round(r.2, 6)
FROM (SELECT finalizeAggregation(studentTTestOneSampleState(x, 2.)) AS r FROM values('x Float64', 1, 2, 4, 5));

SELECT 'confidence interval', round(r.1, 6), round(r.2, 6), round(r.3, 4), round(r.4, 4)
FROM (SELECT finalizeAggregation(studentTTestOneSampleState(0.95)(x, 2.)) AS r FROM values('x Float64', 1, 2, 4, 5));

SELECT 'totals', k, round(studentTTestOneSample(x, 2.).1, 6)
FROM values('k UInt8, x Float64', (0, 1), (0, 2), (0, 4), (0, 5)) GROUP BY k WITH TOTALS ORDER BY k;

SELECT 'rollup', k, round(studentTTestOneSample(x, 2.).1, 6)
FROM values('k UInt8, x Float64', (1, 1), (1, 2), (1, 4), (1, 5)) GROUP BY k WITH ROLLUP ORDER BY k;

SELECT 'cube', k, round(studentTTestOneSample(x, 2.).1, 6)
FROM values('k UInt8, x Float64', (1, 1), (1, 2), (1, 4), (1, 5)) GROUP BY k WITH CUBE ORDER BY k;

-- The states of two parts, merged by the query and then by the table.
DROP TABLE IF EXISTS t_ttest_one_sample_states;
CREATE TABLE t_ttest_one_sample_states (k UInt8, s AggregateFunction(studentTTestOneSample, Float64, Float64)) ENGINE = AggregatingMergeTree ORDER BY k;
INSERT INTO t_ttest_one_sample_states SELECT 0, studentTTestOneSampleState(x, 2.) FROM values('x Float64', 1, 2);
INSERT INTO t_ttest_one_sample_states SELECT 0, studentTTestOneSampleState(x, 2.) FROM values('x Float64', 4, 5);
SELECT 'two parts', round(studentTTestOneSampleMerge(s).1, 6) FROM t_ttest_one_sample_states;
OPTIMIZE TABLE t_ttest_one_sample_states FINAL;
SELECT 'merged part', count(), round(finalizeAggregation(any(s)).1, 6) FROM t_ttest_one_sample_states;
DROP TABLE t_ttest_one_sample_states;

-- Every shard returns the four values, so this is the test over eight values.
SELECT 'two shards', round(studentTTestOneSample(x, 2.).1, 6)
FROM remote('127.0.0.{1,2}', view(SELECT * FROM values('x Float64', 1, 2, 4, 5)));
SELECT 'eight values', round(studentTTestOneSample(x, 2.).1, 6) FROM values('x Float64', 1, 2, 4, 5, 1, 2, 4, 5);

-- A state without values does not take the mean away from a state that has it.
SELECT 'empty state', round(r.1, 6)
FROM
(
    SELECT studentTTestOneSampleMerge(s) AS r
    FROM
    (
        SELECT studentTTestOneSampleState(x, 2.) AS s FROM values('x Float64', 1, 2, 4, 5)
        UNION ALL
        SELECT studentTTestOneSampleState(x, 7.) AS s FROM values('x Float64', 1) WHERE 0
    )
);
