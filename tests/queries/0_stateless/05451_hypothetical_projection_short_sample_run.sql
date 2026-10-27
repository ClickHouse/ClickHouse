-- a sample takes one granule from each run of granules, and the granule of a short last run stands for fewer rows
-- the twin table has the projection materialized and shows the read that the optimizer selects

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0;

CREATE TABLE t_est (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE t_real AS t_est;
ALTER TABLE t_real ADD PROJECTION p (SELECT a, b ORDER BY b);

-- 301 granules in one part: with a step of 10 the last granule is a run of its own, and only its rows have b = 0
INSERT INTO t_est SELECT number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30100);
INSERT INTO t_real SELECT number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30100);
OPTIMIZE TABLE t_est FINAL;
OPTIMIZE TABLE t_real FINAL;
CREATE HYPOTHETICAL PROJECTION p ON t_est (SELECT a, b ORDER BY b);

SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF projection_scan_budget_rows = 3010 SELECT a, b FROM t_est WHERE b = 0)
WHERE match(line, '^(Baseline|With |marks:|marks_span|rows:|verdict:|sampled)');
SELECT 'real: ' || toString(marks) || ' marks, ' || toString(rows) || ' rows'
FROM (EXPLAIN ESTIMATE SELECT a, b FROM t_real WHERE b = 0);

DROP TABLE t_est;
DROP TABLE t_real;
