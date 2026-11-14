-- A LIMIT above joins stops the probe-side read once enough rows are joined, also when few rows match:
-- the squashing of every join down the probe side does not hold rows back from the LIMIT.
DROP TABLE IF EXISTS t_probe;
DROP TABLE IF EXISTS t_build;
DROP TABLE IF EXISTS t_dense;
DROP TABLE IF EXISTS t_many;
CREATE TABLE t_probe (k UInt64, k2 UInt64, v String) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192;
-- one row in 1000 matches: 2000 rows, fewer than min_joined_block_size_rows and min_joined_block_size_bytes
INSERT INTO t_probe SELECT number % 1000, number % 1000, toString(number) FROM numbers(2000000);
CREATE TABLE t_build (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_build VALUES (7, '7');
CREATE TABLE t_dense (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_dense SELECT number FROM numbers(1000);
CREATE TABLE t_many (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_many SELECT 7 FROM numbers(1000);

SET max_threads = 4, max_block_size = 8192, min_joined_block_size_rows = 65409, min_joined_block_size_bytes = 524288,
    query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0, enable_parallel_replicas = 0;

-- the output of a join with a split hash table is squashed; lazily read columns would change what the join outputs
SELECT v FROM t_probe AS p INNER JOIN t_build AS b ON p.k = b.k LIMIT 1 FORMAT Null
SETTINGS hash_join_layout = 'parallel', enable_join_runtime_filters = 0, query_plan_min_columns_for_join_lazy_indexing = 0,
    log_comment = '05629_join_limit_stops_reading_output';
-- no rows minimum, as with compatibility before 25.7: only min_joined_block_size_bytes squashes
SELECT v FROM t_probe AS p INNER JOIN t_build AS b ON p.k = b.k LIMIT 100 FORMAT Null
SETTINGS hash_join_layout = 'parallel', enable_join_runtime_filters = 0, query_plan_min_columns_for_join_lazy_indexing = 0,
    min_joined_block_size_rows = 0, log_comment = '05629_join_limit_stops_reading_bytes_only';
-- a runtime filter thins out the probe input, which is squashed
SELECT v FROM t_probe AS p INNER JOIN t_build AS b ON p.k = b.k LIMIT 1 FORMAT Null
SETTINGS hash_join_layout = 'serial', enable_join_runtime_filters = 1, log_comment = '05629_join_limit_stops_reading_input';
-- the probe input of a block nested loop join is squashed
SELECT v FROM t_probe AS p LEFT JOIN t_build AS b ON p.k < b.k WHERE p.k = 7 LIMIT 1 FORMAT Null
SETTINGS log_comment = '05629_join_limit_stops_reading_nested_loop';
-- a filter on both tables between the LIMIT and the join
SELECT v FROM t_probe AS p INNER JOIN t_build AS b ON p.k = b.k WHERE p.v != b.s LIMIT 1 FORMAT Null
SETTINGS hash_join_layout = 'parallel', enable_join_runtime_filters = 0, query_plan_min_columns_for_join_lazy_indexing = 0,
    log_comment = '05629_join_limit_stops_reading_filter';
-- the sparse join is on the probe side of another join
SELECT v FROM t_probe AS p INNER JOIN t_build AS b ON p.k = b.k INNER JOIN t_dense AS d ON p.k2 = d.k LIMIT 1 FORMAT Null
SETTINGS hash_join_layout = 'parallel', enable_join_runtime_filters = 0, query_plan_min_columns_for_join_lazy_indexing = 0,
    query_plan_optimize_join_order_limit = 0, log_comment = '05629_join_limit_stops_reading_chain';
-- the sparse join is on the probe side of a block nested loop join
SELECT v FROM t_probe AS p INNER JOIN t_build AS b ON p.k = b.k LEFT JOIN t_dense AS d ON p.k2 < d.k LIMIT 1 FORMAT Null
SETTINGS hash_join_layout = 'parallel', enable_join_runtime_filters = 0, query_plan_min_columns_for_join_lazy_indexing = 0,
    query_plan_optimize_join_order_limit = 0, log_comment = '05629_join_limit_stops_reading_nested_loop_chain';
-- one probe row joins 1000 build rows
SELECT v FROM t_probe AS p INNER JOIN t_many AS m ON p.k = m.k LIMIT 50000 FORMAT Null
SETTINGS hash_join_layout = 'serial', enable_join_runtime_filters = 1, log_comment = '05629_join_limit_stops_reading_multiply';
-- with the LIMIT out of reach, the squashing grows back to min_joined_block_size_rows
SELECT max(bs) >= 100 FROM (SELECT blockSize() AS bs FROM t_probe AS p INNER JOIN t_build AS b ON p.k = b.k LIMIT 100000000)
SETTINGS hash_join_layout = 'parallel', enable_join_runtime_filters = 0, query_plan_min_columns_for_join_lazy_indexing = 0,
    min_joined_block_size_rows = 100;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, read_rows < 1000000 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05629_join_limit_stops_reading\_%'
ORDER BY log_comment;

DROP TABLE t_probe;
DROP TABLE t_build;
DROP TABLE t_dense;
DROP TABLE t_many;
