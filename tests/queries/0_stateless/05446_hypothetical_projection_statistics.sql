-- with no scan, the estimate of a normal projection comes from the column statistics of the first column of its key
-- the twin table has the projection materialized and shows the read that the optimizer selects

SET allow_experimental_statistics = 1, allow_statistics_optimize = 1, materialize_statistics_on_insert = 1;
SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0;

CREATE TABLE t_est (a UInt64, b UInt64 STATISTICS(tdigest, countmin)) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        auto_statistics_types = '';
CREATE TABLE t_real AS t_est;
ALTER TABLE t_real ADD PROJECTION p (SELECT a, b ORDER BY b);
INSERT INTO t_est SELECT number, number % 100 FROM numbers(10000);
INSERT INTO t_real SELECT number, number % 100 FROM numbers(10000);
CREATE HYPOTHETICAL PROJECTION p ON t_est (SELECT a, b ORDER BY b);

SELECT '-- a point';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF empirical = 0 SELECT a, b FROM t_est WHERE b = 42)
WHERE match(line, '^(Baseline|With |marks:|rows:|verdict:|source:|empirical_status:)');
SELECT 'real: ' || toString(marks) || ' marks, ' || toString(rows) || ' rows'
FROM (EXPLAIN ESTIMATE SELECT a, b FROM t_real WHERE b = 42);

SELECT '-- a range';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF empirical = 0 SELECT a, b FROM t_est WHERE b < 10)
WHERE match(line, '^(Baseline|With |marks:|rows:|verdict:|source:)');
SELECT 'real: ' || toString(marks) || ' marks, ' || toString(rows) || ' rows'
FROM (EXPLAIN ESTIMATE SELECT a, b FROM t_real WHERE b < 10);

SELECT '-- the filter also reads a column out of the key';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF empirical = 0 SELECT a, b FROM t_est WHERE b = 42 AND a > 5000)
WHERE match(line, '^(With |marks:|source:)');

-- the first part has no statistics of `b`
CREATE TABLE t_old (a UInt64, b UInt64, c UInt64 STATISTICS(tdigest)) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', auto_statistics_types = '';
INSERT INTO t_old SELECT number, number % 100, 0 FROM numbers(1000);
ALTER TABLE t_old ADD STATISTICS b TYPE tdigest;
INSERT INTO t_old SELECT number, number % 100, 0 FROM numbers(1000);
CREATE HYPOTHETICAL PROJECTION p ON t_old (SELECT a, b ORDER BY b);
SELECT '-- a part with no statistics';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF empirical = 0 SELECT a, b FROM t_old WHERE b = 42)
WHERE match(line, '^(With |marks:|source:)');

DROP TABLE t_est;
DROP TABLE t_real;
DROP TABLE t_old;
