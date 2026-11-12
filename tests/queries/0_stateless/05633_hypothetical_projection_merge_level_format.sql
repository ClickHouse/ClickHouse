-- a merge of projection parts chooses the format at level 1, whatever the level of the parent part
-- the estimate of a merged part has to cover the compact layout under `min_level_for_wide_part = 2`, which the twin table has

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0;

-- stored `_block_number` and `_block_offset` change the granules of the second merge of the parent parts
CREATE TABLE t_est (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 1024, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        min_level_for_wide_part = 2, use_const_adaptive_granularity = 1, merge_max_block_size = 150,
        ratio_of_defaults_for_sparse_serialization = 1, auto_statistics_types = '',
        enable_block_number_column = 0, enable_block_offset_column = 0;
CREATE TABLE t_real AS t_est;
ALTER TABLE t_real ADD PROJECTION p_k (SELECT a, b, v ORDER BY b);

INSERT INTO t_est SELECT number + 1, number % 100 + 1, number + 1 FROM numbers(1500);
INSERT INTO t_est SELECT number + 1501, number % 100 + 1, number + 1 FROM numbers(1500);
INSERT INTO t_real SELECT number + 1, number % 100 + 1, number + 1 FROM numbers(1500);
INSERT INTO t_real SELECT number + 1501, number % 100 + 1, number + 1 FROM numbers(1500);
OPTIMIZE TABLE t_est FINAL;
OPTIMIZE TABLE t_real FINAL;
INSERT INTO t_est SELECT number + 3001, number % 100 + 1, number + 1 FROM numbers(1500);
INSERT INTO t_est SELECT number + 4501, number % 100 + 1, number + 1 FROM numbers(1500);
INSERT INTO t_real SELECT number + 3001, number % 100 + 1, number + 1 FROM numbers(1500);
INSERT INTO t_real SELECT number + 4501, number % 100 + 1, number + 1 FROM numbers(1500);
-- the second merge gives a wide parent part of level 2 with a compact projection part
OPTIMIZE TABLE t_est FINAL;
OPTIMIZE TABLE t_real FINAL;

CREATE HYPOTHETICAL PROJECTION p_k ON t_est (SELECT a, b, v ORDER BY b);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a, b, v FROM t_est WHERE b < 10 AND a < 500)
WHERE match(line, '^(With |marks:|marks_span:|verdict:)');
SELECT 'real: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, v FROM t_real WHERE b < 10 AND a < 500)
WHERE match(trim(explain), '^(ReadFromMergeTree|Granules:)');
SELECT 'real level: ' || toString(level) || ', format: ' || part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_real' AND active;
SELECT 'real projection format: ' || part_type FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_real' AND active;

DROP TABLE t_est;
DROP TABLE t_real;
