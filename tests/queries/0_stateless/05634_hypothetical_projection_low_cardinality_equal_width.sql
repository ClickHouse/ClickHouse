-- the writer counts the indexes and the dictionary of a `LowCardinality` column for each block, so rows of one size do not show one width
-- the twin table has the projection materialized and shows the read that the optimizer selects

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0;

CREATE TABLE t_est (a UInt64, b UInt64, s LowCardinality(String)) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 2900, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        use_const_adaptive_granularity = 0, ratio_of_defaults_for_sparse_serialization = 1, auto_statistics_types = '';
CREATE TABLE t_real AS t_est;
ALTER TABLE t_real ADD PROJECTION p_b (SELECT a, b, s ORDER BY b);
INSERT INTO t_est SELECT number, number % 100, leftPad(toString(number), 4, '0') FROM numbers(3000);
INSERT INTO t_real SELECT number, number % 100, leftPad(toString(number), 4, '0') FROM numbers(3000);

CREATE HYPOTHETICAL PROJECTION p_b ON t_est (SELECT a, b, s ORDER BY b);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a, b, s FROM t_est WHERE b < 10 AND a < 300)
WHERE match(line, '^(Baseline|With |marks:|marks_span:|verdict:)');
SELECT if(countIf(explain LIKE '%ReadFromMergeTree (p_b)%') > 0, 'real: from the projection', 'real: from the base table')
FROM (EXPLAIN SELECT a, b, s FROM t_real WHERE b < 10 AND a < 300);
SELECT 'real forced: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, s FROM t_real WHERE b < 10 AND a < 300 SETTINGS force_optimize_projection = 1)
WHERE match(trim(explain), '^Granules:');

DROP TABLE t_est;
DROP TABLE t_real;
