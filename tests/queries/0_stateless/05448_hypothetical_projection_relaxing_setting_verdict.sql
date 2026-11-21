-- with a relaxing setting the optimizer takes a usable projection in any granule layout,
-- so rows of different widths leave the verdict open only when another projection competes
-- the twin table has the projection materialized and shows the read that the optimizer selects

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0;

-- a compact part, where the widths can change the granules
CREATE TABLE t_est (a UInt64, b UInt64, s String) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = '1Gi', min_rows_for_wide_part = 1000000000,
        ratio_of_defaults_for_sparse_serialization = 1, auto_statistics_types = '';
CREATE TABLE t_real AS t_est;
ALTER TABLE t_real ADD PROJECTION p_b (SELECT a, b, s ORDER BY b);
INSERT INTO t_est SELECT number, number % 100, repeat('x', number % 7) FROM numbers(1000);
INSERT INTO t_real SELECT number, number % 100, repeat('x', number % 7) FROM numbers(1000);
CREATE HYPOTHETICAL PROJECTION p_b ON t_est (SELECT a, b, s ORDER BY b);

SELECT '-- prefer_optimize_projection';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a, b, s FROM t_est WHERE b = 42 SETTINGS prefer_optimize_projection = 1)
WHERE match(line, '^(With |marks:|verdict:)');
SELECT 'real reads the projection: ' || toString(countIf(explain LIKE '%ReadFromMergeTree (p_b)%'))
FROM (EXPLAIN SELECT a, b, s FROM t_real WHERE b = 42 SETTINGS prefer_optimize_projection = 1);

SELECT '-- force_optimize_projection';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a, b, s FROM t_est WHERE b = 42 SETTINGS force_optimize_projection = 1)
WHERE match(line, '^verdict:');

SELECT '-- with no relaxing setting the widths leave the verdict open';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a, b, s FROM t_est WHERE b = 42)
WHERE match(line, '^verdict:');

DROP TABLE t_est;
DROP TABLE t_real;
