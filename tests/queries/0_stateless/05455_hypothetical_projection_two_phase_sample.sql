-- the estimate first reads a sample of a quarter of `projection_scan_budget_rows`
-- it reads the sample of the full budget only when the first verdict is too close to call

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0;

CREATE TABLE t (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 10, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        ratio_of_defaults_for_sparse_serialization = 1, merge_max_block_size = 8192, use_const_adaptive_granularity = 0,
        auto_statistics_types = '';
INSERT INTO t SELECT number, cityHash64(number) % 1000 FROM numbers(6000);
OPTIMIZE TABLE t FINAL;
CREATE HYPOTHETICAL PROJECTION p ON t (SELECT a, b ORDER BY b);

-- 600 granules: the full budget of 1200 rows samples one granule of every 5, the first phase one of every 20
SELECT '-- a point on the key of the projection: the first phase is enough';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF projection_scan_budget_rows = 1200 SELECT a, b FROM t WHERE b = 7)
WHERE match(line, '^(verdict|sampled_marks):');

SELECT '-- almost the whole table: the first verdict is too close to call, so the estimate reads the full budget';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF projection_scan_budget_rows = 1200 SELECT a, b FROM t WHERE b < 990)
WHERE match(line, '^(verdict|sampled_marks):');

SELECT '-- a budget that covers the scan reads every granule, as before';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF projection_scan_budget_rows = 6000 SELECT a, b FROM t WHERE b = 7)
WHERE match(line, '^(verdict|sampled_marks):');

SELECT '-- a read limit keeps one sample, so the two samples together cannot pass it';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF projection_scan_budget_rows = 1200 SELECT a, b FROM t WHERE b = 7 SETTINGS max_rows_to_read = 100000)
WHERE match(line, '^(verdict|sampled_marks):');

-- rows of different widths keep the verdict of any sample too close to call, and a larger sample does not change that
CREATE TABLE s (a UInt64, b UInt64, c String) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 10, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        ratio_of_defaults_for_sparse_serialization = 1, merge_max_block_size = 8192, use_const_adaptive_granularity = 0,
        auto_statistics_types = '';
INSERT INTO s SELECT number, cityHash64(number) % 1000, repeat('x', number % 7) FROM numbers(6000);
OPTIMIZE TABLE s FINAL;
CREATE HYPOTHETICAL PROJECTION p ON s (SELECT a, b, c ORDER BY b);
SELECT '-- rows of different widths: the first verdict is final';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF projection_scan_budget_rows = 1200 SELECT a, b, c FROM s WHERE b = 7)
WHERE match(line, '^(verdict|sampled_marks):');

DROP TABLE t;
DROP TABLE s;
