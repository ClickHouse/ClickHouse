-- the block of a projection part has the subcolumns that the projection names, also when the part does not store them
-- the twin tables have the projection materialized and show the read that the optimizer selects

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0;

-- a key over a subcolumn: its bytes take the part over min_bytes_for_wide_part
CREATE TABLE t_est (a UInt64, t Tuple(x UInt64, y UInt64)) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 1024, min_bytes_for_wide_part = 80000, min_rows_for_wide_part = 0,
        merge_max_block_size = 8192, use_const_adaptive_granularity = 0, ratio_of_defaults_for_sparse_serialization = 1,
        auto_statistics_types = '';
CREATE TABLE t_real AS t_est;
ALTER TABLE t_real ADD PROJECTION p_k (SELECT a, t ORDER BY t.x);
INSERT INTO t_est SELECT number, (number % 100, number) FROM numbers(3010);
INSERT INTO t_real SELECT number, (number % 100, number) FROM numbers(3010);
SELECT 'real part: ' || part_type FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_real' AND active ORDER BY name;

CREATE HYPOTHETICAL PROJECTION p_k ON t_est (SELECT a, t ORDER BY t.x);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line FROM (EXPLAIN WHATIF SELECT a, t FROM t_est WHERE t.x = 99)
WHERE match(line, '^(With |marks:|verdict:)');
SELECT 'real: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, t FROM t_real WHERE t.x = 99)
WHERE match(trim(explain), '^(ReadFromMergeTree|Granules:)');

-- a subcolumn in the select list makes the granules smaller
CREATE TABLE s_est (a UInt64, b UInt64, t Tuple(x UInt64, y UInt64)) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 1024, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        merge_max_block_size = 8192, use_const_adaptive_granularity = 0, ratio_of_defaults_for_sparse_serialization = 1,
        auto_statistics_types = '';
CREATE TABLE s_real AS s_est;
ALTER TABLE s_real ADD PROJECTION p_s (SELECT a, b, t, t.y ORDER BY b);
INSERT INTO s_est SELECT number, number % 100, (number, number) FROM numbers(3000);
INSERT INTO s_real SELECT number, number % 100, (number, number) FROM numbers(3000);

CREATE HYPOTHETICAL PROJECTION p_s ON s_est (SELECT a, b, t, t.y ORDER BY b);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line FROM (EXPLAIN WHATIF SELECT a, b, t FROM s_est WHERE b < 50 AND a < 1700)
WHERE match(line, '^(With |marks:|verdict:)');
SELECT if(countIf(explain LIKE '%ReadFromMergeTree (p_s)%') > 0, 'real: from the projection', 'real: from the base table')
FROM (EXPLAIN SELECT a, b, t FROM s_real WHERE b < 50 AND a < 1700);
SELECT 'real forced: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, t FROM s_real WHERE b < 50 AND a < 1700 SETTINGS force_optimize_projection = 1)
WHERE match(trim(explain), '^Granules:');

DROP TABLE t_est;
DROP TABLE t_real;
DROP TABLE s_est;
DROP TABLE s_real;
