-- a merge builds the states of a projection part in the memory of its column, and the writer sizes the granules by that memory
-- the twin table has the projection materialized and shows its marks and the read that the optimizer selects

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0, optimize_aggregation_in_order = 0;

CREATE TABLE t_est (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 8192, index_granularity_bytes = 1024, use_const_adaptive_granularity = 0, min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0, merge_max_block_size = 8192, enable_vertical_merge_algorithm = 0, enable_block_number_column = 0,
        enable_block_offset_column = 0, ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE t_real AS t_est;
ALTER TABLE t_real ADD PROJECTION p (SELECT b, groupArray(v) GROUP BY b);
ALTER TABLE t_real ADD PROJECTION p_u (SELECT b, uniqExact(v) GROUP BY b);

INSERT INTO t_est SELECT number, number % 500, 0 FROM numbers(10000);
INSERT INTO t_est SELECT number, number % 500, 0 FROM numbers(10000);
INSERT INTO t_real SELECT number, number % 500, 0 FROM numbers(10000);
INSERT INTO t_real SELECT number, number % 500, 0 FROM numbers(10000);
OPTIMIZE TABLE t_est FINAL;
OPTIMIZE TABLE t_real FINAL;

SELECT '-- groupArray states of a merged part';
CREATE HYPOTHETICAL PROJECTION p ON t_est (SELECT b, groupArray(v) GROUP BY b);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT b, groupArray(v) FROM t_est GROUP BY b)
WHERE match(line, '^(Baseline|With |marks:|verdict:)');
SELECT 'real projection part: ' || toString(marks - 1) || ' marks' FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_real' AND name = 'p' AND active;
SELECT 'real: ' || toString(marks) || ' marks'
FROM (EXPLAIN ESTIMATE SELECT b, groupArray(v) FROM t_real GROUP BY b);
DROP HYPOTHETICAL PROJECTION p ON t_est;

SELECT '-- uniqExact states of a merged part';
CREATE HYPOTHETICAL PROJECTION p_u ON t_est (SELECT b, uniqExact(v) GROUP BY b);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT b, uniqExact(v) FROM t_est GROUP BY b)
WHERE match(line, '^(Baseline|With |marks:|verdict:)');
SELECT 'real projection part: ' || toString(marks - 1) || ' marks' FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_real' AND name = 'p_u' AND active;
SELECT 'real: ' || toString(marks) || ' marks'
FROM (EXPLAIN ESTIMATE SELECT b, uniqExact(v) FROM t_real GROUP BY b);

DROP TABLE t_est;
DROP TABLE t_real;
