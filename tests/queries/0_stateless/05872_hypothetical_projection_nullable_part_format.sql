-- the hypothetical projection chooses the part format with the same size estimate as the writer, in which the
-- default values inside a Nullable column are not counted, so a mostly-NULL key stays in a Compact part like the real one
-- the twin table has the projection materialized and shows the read that the optimizer selects

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0, enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_est;
DROP TABLE IF EXISTS t_real;

CREATE TABLE t_est (b UInt64, n Nullable(UInt64), v UInt8) ENGINE = MergeTree ORDER BY b
    SETTINGS index_granularity = 32, index_granularity_bytes = 1024, use_const_adaptive_granularity = 1;
CREATE TABLE t_real AS t_est;
ALTER TABLE t_real ADD PROJECTION p (SELECT b, n, sum(v) GROUP BY b, n) WITH SETTINGS (index_granularity = 8192, min_bytes_for_wide_part = 10000);

INSERT INTO t_est SELECT number, NULL, 1 FROM numbers(500);
INSERT INTO t_real SELECT number, NULL, 1 FROM numbers(500);

CREATE HYPOTHETICAL PROJECTION p ON t_est (SELECT b, n, sum(v) GROUP BY b, n) WITH SETTINGS (index_granularity = 8192, min_bytes_for_wide_part = 10000);

SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT b, n, sum(v) FROM t_est GROUP BY b, n)
WHERE match(trim(explain), '^(marks|marks_span|verdict):');
SELECT if(explain LIKE '%ReadFromMergeTree (p)%', 'real: from the projection', 'real: from the base table')
FROM (EXPLAIN SELECT b, n, sum(v) FROM t_real GROUP BY b, n) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT part_type FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_real' AND name = 'p' AND active;

DROP TABLE t_est;
DROP TABLE t_real;
