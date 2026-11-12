-- a merge of projection parts chooses the format and a constant granule from the bytes that these parts store, not from a block in memory
-- with `single_stream` a string stores its length in one byte, in memory it takes eight bytes of offset
-- the twin tables have the projection materialized and show the read that the optimizer selects

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0;

SELECT '-- a constant granule from the stored bytes';
CREATE TABLE t_est (a UInt64, b UInt64, s String) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 1024, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        use_const_adaptive_granularity = 1, ratio_of_defaults_for_sparse_serialization = 1, string_serialization_version = 'single_stream',
        auto_statistics_types = '';
CREATE TABLE t_real AS t_est;
ALTER TABLE t_real ADD PROJECTION p_k (SELECT a, b, s ORDER BY intDiv(b, 10));
INSERT INTO t_est SELECT number, number % 100, 'x' FROM numbers(1500);
INSERT INTO t_est SELECT number + 1500, number % 100, 'x' FROM numbers(1500);
INSERT INTO t_real SELECT number, number % 100, 'x' FROM numbers(1500);
INSERT INTO t_real SELECT number + 1500, number % 100, 'x' FROM numbers(1500);
OPTIMIZE TABLE t_est FINAL;
OPTIMIZE TABLE t_real FINAL;

CREATE HYPOTHETICAL PROJECTION p_k ON t_est (SELECT a, b, s ORDER BY intDiv(b, 10));
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a, b, s FROM t_est WHERE intDiv(b, 10) = 4 AND a < 378)
WHERE match(line, '^(Baseline|With |marks:|marks_span:|verdict:)');
SELECT 'real: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, s FROM t_real WHERE intDiv(b, 10) = 4 AND a < 378)
WHERE match(trim(explain), '^(ReadFromMergeTree|Granules:)');

SELECT '-- a compact part, the block in memory is over min_bytes_for_wide_part';
CREATE TABLE c_est (a UInt64, b UInt64, s String) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 25000, min_rows_for_wide_part = 0,
        merge_max_block_size = 150, use_const_adaptive_granularity = 0, ratio_of_defaults_for_sparse_serialization = 1,
        string_serialization_version = 'single_stream', auto_statistics_types = '';
CREATE TABLE c_real AS c_est;
ALTER TABLE c_real ADD PROJECTION p_b (SELECT a, b, s ORDER BY b);
INSERT INTO c_est SELECT number, number % 100, repeat('x', number % 7) FROM numbers(500);
INSERT INTO c_est SELECT number + 500, number % 100, repeat('x', number % 7) FROM numbers(500);
INSERT INTO c_real SELECT number, number % 100, repeat('x', number % 7) FROM numbers(500);
INSERT INTO c_real SELECT number + 500, number % 100, repeat('x', number % 7) FROM numbers(500);
OPTIMIZE TABLE c_est FINAL;
OPTIMIZE TABLE c_real FINAL;
SELECT 'real part: ' || part_type FROM system.projection_parts WHERE database = currentDatabase() AND table = 'c_real' AND active;

CREATE HYPOTHETICAL PROJECTION p_b ON c_est (SELECT a, b, s ORDER BY b);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a, b, s FROM c_est WHERE b < 50 AND a < 580)
WHERE match(line, '^(Baseline|With |marks:|marks_span:|verdict:)');
SELECT if(countIf(explain LIKE '%ReadFromMergeTree (p_b)%') > 0, 'real: from the projection', 'real: from the base table')
FROM (EXPLAIN SELECT a, b, s FROM c_real WHERE b < 50 AND a < 580);
SELECT 'real forced: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, s FROM c_real WHERE b < 50 AND a < 580 SETTINGS force_optimize_projection = 1)
WHERE match(trim(explain), '^Granules:');

SELECT '-- a merged part of a sparse column: a part does not record which of the parts it merged were sparse';
CREATE TABLE s_est (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 1024, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        use_const_adaptive_granularity = 1, ratio_of_defaults_for_sparse_serialization = 0.5, auto_statistics_types = '';
CREATE TABLE s_real AS s_est;
ALTER TABLE s_real ADD PROJECTION p_b (SELECT a, b, v ORDER BY b);
INSERT INTO s_est SELECT number, number % 100, 0 FROM numbers(1500);
INSERT INTO s_est SELECT number + 1500, number % 100, number + 1 FROM numbers(1500);
INSERT INTO s_real SELECT number, number % 100, 0 FROM numbers(1500);
INSERT INTO s_real SELECT number + 1500, number % 100, number + 1 FROM numbers(1500);
OPTIMIZE TABLE s_est FINAL;
OPTIMIZE TABLE s_real FINAL;

CREATE HYPOTHETICAL PROJECTION p_b ON s_est (SELECT a, b, v ORDER BY b);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a, b, v FROM s_est WHERE b < 10 AND a < 340)
WHERE match(line, '^(Baseline|With |marks:|verdict:)');
SELECT 'real: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, v FROM s_real WHERE b < 10 AND a < 340)
WHERE match(trim(explain), '^(ReadFromMergeTree|Granules:)');

SELECT '-- the same parts merged after the ratio disables sparse columns: the current ratio does not show how the merged parts were written';
DROP TABLE s_est;
DROP TABLE s_real;
CREATE TABLE s_est (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 1024, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        use_const_adaptive_granularity = 1, ratio_of_defaults_for_sparse_serialization = 0.5, auto_statistics_types = '';
CREATE TABLE s_real AS s_est;
ALTER TABLE s_real ADD PROJECTION p_b (SELECT a, b, v ORDER BY b);
INSERT INTO s_est SELECT number, number % 100, 0 FROM numbers(1500);
INSERT INTO s_est SELECT number + 1500, number % 100, number + 1 FROM numbers(1500);
INSERT INTO s_real SELECT number, number % 100, 0 FROM numbers(1500);
INSERT INTO s_real SELECT number + 1500, number % 100, number + 1 FROM numbers(1500);
ALTER TABLE s_est MODIFY SETTING ratio_of_defaults_for_sparse_serialization = 1;
ALTER TABLE s_real MODIFY SETTING ratio_of_defaults_for_sparse_serialization = 1;
OPTIMIZE TABLE s_est FINAL;
OPTIMIZE TABLE s_real FINAL;

CREATE HYPOTHETICAL PROJECTION p_b ON s_est (SELECT a, b, v ORDER BY b);
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a, b, v FROM s_est WHERE b < 10 AND a < 340)
WHERE match(line, '^(Baseline|With |marks:|verdict:)');
SELECT 'real: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, v FROM s_real WHERE b < 10 AND a < 340)
WHERE match(trim(explain), '^(ReadFromMergeTree|Granules:)');

DROP TABLE t_est;
DROP TABLE t_real;
DROP TABLE c_est;
DROP TABLE c_real;
DROP TABLE s_est;
DROP TABLE s_real;
