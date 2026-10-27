-- a sampled granule stands for the rows of its run, and the last granule of a part can hold fewer rows than the others
-- in each of 12 parts only the rows of the last run match, so the estimate is exact for any sampled granule of that run
-- the twin table has the projection materialized and shows the read that the optimizer selects

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0;

CREATE TABLE t_est (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        ratio_of_defaults_for_sparse_serialization = 1, merge_max_block_size = 8192, use_const_adaptive_granularity = 0,
        auto_statistics_types = '';
CREATE TABLE t_real AS t_est;
ALTER TABLE t_real ADD PROJECTION p (SELECT a, b ORDER BY b);
SYSTEM STOP MERGES t_est;
SYSTEM STOP MERGES t_real;

-- parts of 30210 to 30617 rows: the last granule has 10 to 17 rows less than a full one, and the last run 1 to 10 granules
INSERT INTO t_est SELECT 0 + number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30210) SETTINGS max_insert_block_size = 30210, min_insert_block_size_rows = 30210, min_insert_block_size_bytes = 0, max_block_size = 65536;
INSERT INTO t_real SELECT 0 + number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30210) SETTINGS max_insert_block_size = 30210, min_insert_block_size_rows = 30210, min_insert_block_size_bytes = 0, max_block_size = 65536;
INSERT INTO t_est SELECT 1000000 + number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30247) SETTINGS max_insert_block_size = 30247, min_insert_block_size_rows = 30247, min_insert_block_size_bytes = 0, max_block_size = 65536;
INSERT INTO t_real SELECT 1000000 + number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30247) SETTINGS max_insert_block_size = 30247, min_insert_block_size_rows = 30247, min_insert_block_size_bytes = 0, max_block_size = 65536;
INSERT INTO t_est SELECT 2000000 + number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30284) SETTINGS max_insert_block_size = 30284, min_insert_block_size_rows = 30284, min_insert_block_size_bytes = 0, max_block_size = 65536;
INSERT INTO t_real SELECT 2000000 + number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30284) SETTINGS max_insert_block_size = 30284, min_insert_block_size_rows = 30284, min_insert_block_size_bytes = 0, max_block_size = 65536;
INSERT INTO t_est SELECT 3000000 + number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30321) SETTINGS max_insert_block_size = 30321, min_insert_block_size_rows = 30321, min_insert_block_size_bytes = 0, max_block_size = 65536;
INSERT INTO t_real SELECT 3000000 + number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30321) SETTINGS max_insert_block_size = 30321, min_insert_block_size_rows = 30321, min_insert_block_size_bytes = 0, max_block_size = 65536;
INSERT INTO t_est SELECT 4000000 + number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30358) SETTINGS max_insert_block_size = 30358, min_insert_block_size_rows = 30358, min_insert_block_size_bytes = 0, max_block_size = 65536;
INSERT INTO t_real SELECT 4000000 + number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30358) SETTINGS max_insert_block_size = 30358, min_insert_block_size_rows = 30358, min_insert_block_size_bytes = 0, max_block_size = 65536;
INSERT INTO t_est SELECT 5000000 + number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30395) SETTINGS max_insert_block_size = 30395, min_insert_block_size_rows = 30395, min_insert_block_size_bytes = 0, max_block_size = 65536;
INSERT INTO t_real SELECT 5000000 + number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30395) SETTINGS max_insert_block_size = 30395, min_insert_block_size_rows = 30395, min_insert_block_size_bytes = 0, max_block_size = 65536;
INSERT INTO t_est SELECT 6000000 + number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30432) SETTINGS max_insert_block_size = 30432, min_insert_block_size_rows = 30432, min_insert_block_size_bytes = 0, max_block_size = 65536;
INSERT INTO t_real SELECT 6000000 + number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30432) SETTINGS max_insert_block_size = 30432, min_insert_block_size_rows = 30432, min_insert_block_size_bytes = 0, max_block_size = 65536;
INSERT INTO t_est SELECT 7000000 + number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30469) SETTINGS max_insert_block_size = 30469, min_insert_block_size_rows = 30469, min_insert_block_size_bytes = 0, max_block_size = 65536;
INSERT INTO t_real SELECT 7000000 + number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30469) SETTINGS max_insert_block_size = 30469, min_insert_block_size_rows = 30469, min_insert_block_size_bytes = 0, max_block_size = 65536;
INSERT INTO t_est SELECT 8000000 + number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30506) SETTINGS max_insert_block_size = 30506, min_insert_block_size_rows = 30506, min_insert_block_size_bytes = 0, max_block_size = 65536;
INSERT INTO t_real SELECT 8000000 + number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30506) SETTINGS max_insert_block_size = 30506, min_insert_block_size_rows = 30506, min_insert_block_size_bytes = 0, max_block_size = 65536;
INSERT INTO t_est SELECT 9000000 + number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30543) SETTINGS max_insert_block_size = 30543, min_insert_block_size_rows = 30543, min_insert_block_size_bytes = 0, max_block_size = 65536;
INSERT INTO t_real SELECT 9000000 + number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30543) SETTINGS max_insert_block_size = 30543, min_insert_block_size_rows = 30543, min_insert_block_size_bytes = 0, max_block_size = 65536;
INSERT INTO t_est SELECT 10000000 + number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30580) SETTINGS max_insert_block_size = 30580, min_insert_block_size_rows = 30580, min_insert_block_size_bytes = 0, max_block_size = 65536;
INSERT INTO t_real SELECT 10000000 + number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30580) SETTINGS max_insert_block_size = 30580, min_insert_block_size_rows = 30580, min_insert_block_size_bytes = 0, max_block_size = 65536;
INSERT INTO t_est SELECT 11000000 + number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30617) SETTINGS max_insert_block_size = 30617, min_insert_block_size_rows = 30617, min_insert_block_size_bytes = 0, max_block_size = 65536;
INSERT INTO t_real SELECT 11000000 + number, if(number >= 30000, 0, 1 + number % 50) FROM numbers(30617) SETTINGS max_insert_block_size = 30617, min_insert_block_size_rows = 30617, min_insert_block_size_bytes = 0, max_block_size = 65536;
CREATE HYPOTHETICAL PROJECTION p ON t_est (SELECT a, b ORDER BY b);

-- the budget reads one granule of every 10
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF projection_scan_budget_rows = 37000 SELECT a, b FROM t_est WHERE b = 0)
WHERE match(line, '^(With |marks:|rows:|sampled_parts)');
SELECT 'real: ' || toString(marks) || ' marks, ' || toString(rows) || ' rows'
FROM (EXPLAIN ESTIMATE SELECT a, b FROM t_real WHERE b = 0);

DROP TABLE t_est;
DROP TABLE t_real;
