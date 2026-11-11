-- a merge sizes the projection granule from the bytes that the merged parts store
-- a part does not record the serialization versions of the parts that it merged, and a wide part stores the offsets of a nested table once
-- the twin tables have the projection materialized and show the read that the optimizer selects

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0;

SELECT '-- the string serialization changes before the merge';
CREATE TABLE t_est (a UInt64, b UInt64, s String) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 1024, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        use_const_adaptive_granularity = 1, ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'with_types',
        string_serialization_version = 'single_stream', auto_statistics_types = '';
CREATE TABLE t_real AS t_est;
ALTER TABLE t_real ADD PROJECTION p_k (SELECT a, b, s ORDER BY intDiv(b, 10));
INSERT INTO t_est SELECT number, number % 100, 'x' FROM numbers(1500);
INSERT INTO t_est SELECT number + 1500, number % 100, 'x' FROM numbers(1500);
INSERT INTO t_real SELECT number, number % 100, 'x' FROM numbers(1500);
INSERT INTO t_real SELECT number + 1500, number % 100, 'x' FROM numbers(1500);
ALTER TABLE t_est MODIFY SETTING string_serialization_version = 'with_size_stream';
ALTER TABLE t_real MODIFY SETTING string_serialization_version = 'with_size_stream';
OPTIMIZE TABLE t_est FINAL;
OPTIMIZE TABLE t_real FINAL;

CREATE HYPOTHETICAL PROJECTION p_k ON t_est (SELECT a, b, s ORDER BY intDiv(b, 10));
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a, b, s FROM t_est WHERE intDiv(b, 10) = 4 AND a < 378)
WHERE match(line, '^(Baseline|With |marks:|marks_span:|verdict:)');
SELECT 'real: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, s FROM t_real WHERE intDiv(b, 10) = 4 AND a < 378)
WHERE match(trim(explain), '^(ReadFromMergeTree|Granules:)');

SELECT '-- two columns of a nested table share the offsets';
CREATE TABLE n_est (a UInt64, b UInt64, n Nested(x UInt64, y UInt64)) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 1024, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        use_const_adaptive_granularity = 1, ratio_of_defaults_for_sparse_serialization = 1, auto_statistics_types = '';
CREATE TABLE n_real AS n_est;
ALTER TABLE n_real ADD PROJECTION p_k (SELECT a, b, n.x, n.y ORDER BY intDiv(b, 10));
INSERT INTO n_est SELECT number, number % 100, [number], [number] FROM numbers(1500);
INSERT INTO n_est SELECT number + 1500, number % 100, [number], [number] FROM numbers(1500);
INSERT INTO n_real SELECT number, number % 100, [number], [number] FROM numbers(1500);
INSERT INTO n_real SELECT number + 1500, number % 100, [number], [number] FROM numbers(1500);
OPTIMIZE TABLE n_est FINAL;
OPTIMIZE TABLE n_real FINAL;

CREATE HYPOTHETICAL PROJECTION p_k ON n_est (SELECT a, b, n.x, n.y ORDER BY intDiv(b, 10));
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a, b, n.x, n.y FROM n_est WHERE intDiv(b, 10) = 4 AND a < 350)
WHERE match(line, '^(Baseline|With |marks:|marks_span:|verdict:)');
SELECT 'real: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, n.x, n.y FROM n_real WHERE intDiv(b, 10) = 4 AND a < 350)
WHERE match(trim(explain), '^(ReadFromMergeTree|Granules:)');

DROP TABLE t_est;
DROP TABLE t_real;
DROP TABLE n_est;
DROP TABLE n_real;
