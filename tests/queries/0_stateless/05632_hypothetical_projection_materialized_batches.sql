-- `MATERIALIZE PROJECTION` of a big level-zero part writes a temporary part for each batch and merges these parts
-- with a constant adaptive granularity, that merge sizes the granule from the stored columns, as a merge of the parent does
-- the estimate of a level-zero part has to cover the merged layout, which the twin table has

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0;

CREATE TABLE t_est (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 1024, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        use_const_adaptive_granularity = 1, ratio_of_defaults_for_sparse_serialization = 1, auto_statistics_types = '';
CREATE TABLE t_real AS t_est;

-- more rows than `min_insert_block_size_rows` of the background profile, in one part
INSERT INTO t_est SELECT number + 1, number % 100 + 1, number + 1 FROM numbers(1100000)
    SETTINGS max_block_size = 1100000, max_insert_block_size = 1100000, max_threads = 1;
INSERT INTO t_real SELECT number + 1, number % 100 + 1, number + 1 FROM numbers(1100000)
    SETTINGS max_block_size = 1100000, max_insert_block_size = 1100000, max_threads = 1;
ALTER TABLE t_real ADD PROJECTION p_k (SELECT a, b, v ORDER BY intDiv(b, 10));
ALTER TABLE t_real MATERIALIZE PROJECTION p_k SETTINGS mutations_sync = 1;
SELECT table, level, rows FROM system.parts WHERE database = currentDatabase() AND active ORDER BY table;

CREATE HYPOTHETICAL PROJECTION p_k ON t_est (SELECT a, b, v ORDER BY intDiv(b, 10));
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a, b, v FROM t_est WHERE intDiv(b, 10) = 4 AND a < 125000)
WHERE match(line, '^(With |marks:|marks_span:|verdict:)');
SELECT 'real: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, v FROM t_real WHERE intDiv(b, 10) = 4 AND a < 125000)
WHERE match(trim(explain), '^(ReadFromMergeTree|Granules:)');

DROP TABLE t_est;
DROP TABLE t_real;
