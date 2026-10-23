-- with a constant adaptive granularity, a merge sizes the projection granule from the stored columns,
-- and an insert or a materialization also from the key expression
-- the estimate of a merged part has to cover the merged layout, which the twin table has

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0;

CREATE TABLE t_est (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = 1024, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        use_const_adaptive_granularity = 1, ratio_of_defaults_for_sparse_serialization = 1, auto_statistics_types = '';
CREATE TABLE t_real AS t_est;
ALTER TABLE t_real ADD PROJECTION p_k (SELECT a, b, v ORDER BY intDiv(b, 10));

INSERT INTO t_est SELECT number, number % 100, number FROM numbers(1500);
INSERT INTO t_est SELECT number + 1500, number % 100, number FROM numbers(1500);
INSERT INTO t_real SELECT number, number % 100, number FROM numbers(1500);
INSERT INTO t_real SELECT number + 1500, number % 100, number FROM numbers(1500);
OPTIMIZE TABLE t_est FINAL;
OPTIMIZE TABLE t_real FINAL;

CREATE HYPOTHETICAL PROJECTION p_k ON t_est (SELECT a, b, v ORDER BY intDiv(b, 10));
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT a, b, v FROM t_est WHERE intDiv(b, 10) = 4 AND a < 378)
WHERE match(line, '^(With |marks:|marks_span:|verdict:)');
SELECT 'real: ' || replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN indexes = 1, projections = 1 SELECT a, b, v FROM t_real WHERE intDiv(b, 10) = 4 AND a < 378)
WHERE match(trim(explain), '^(ReadFromMergeTree|Granules:)');

DROP TABLE t_est;
DROP TABLE t_real;
