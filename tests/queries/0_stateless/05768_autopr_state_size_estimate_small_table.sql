-- Automatic parallel replicas estimates the size of the aggregate states from a sample of them.
-- A `UInt16` key is aggregated in a fixed hash map, which is iterated in key order. Of the 215 states, one holds
-- almost all the bytes, at a key the sample of every second state does not reach. The states fit into one
-- compressed block, so the table has to be priced in full.

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_nullable;
DROP TABLE IF EXISTS t_large;
DROP TABLE IF EXISTS t_many;
DROP TABLE IF EXISTS t_nullable_small;
DROP TABLE IF EXISTS t_two;
DROP TABLE IF EXISTS t_1000;
DROP TABLE IF EXISTS t_null_only;
DROP TABLE IF EXISTS t_101;
DROP TABLE IF EXISTS t_compressible;

CREATE TABLE t (k UInt16, s String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t SELECT number % 215, '' FROM numbers(65536);
INSERT INTO t SELECT 205, randomString(100000);

-- With a `Nullable` key, the state of the `NULL` key is stored outside the hash table.
CREATE TABLE t_nullable (k Nullable(UInt16), s String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_nullable SELECT number % 215, '' FROM numbers(65536);
INSERT INTO t_nullable SELECT NULL, randomString(100000);

-- The states do not fit into one compressed block, so the sample stands.
CREATE TABLE t_large (k UInt16, s String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_large SELECT number % 215, '' FROM numbers(65536);
INSERT INTO t_large SELECT 205, randomString(2000000);

-- Above 1000 states the table is sampled, however small its states are.
CREATE TABLE t_many (k UInt16, s String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_many SELECT number % 1001, '' FROM numbers(65536);
INSERT INTO t_many SELECT 995, randomString(100000);

-- With at most 100 groups the sample visits every state except the one of the `NULL` key.
CREATE TABLE t_nullable_small (k Nullable(UInt16), s String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_nullable_small SELECT number % 50, '' FROM numbers(65536);
INSERT INTO t_nullable_small SELECT NULL, randomString(100000);

-- The aggregate functions share the budget: after the first heavy state the second one no longer fits.
CREATE TABLE t_two (k UInt16, s1 String, s2 String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_two SELECT number % 215, '', '' FROM numbers(65536);
INSERT INTO t_two SELECT 205, randomString(600000), randomString(600000);

-- A table of exactly 1000 states is still priced in full; `t_many` holds one state more.
CREATE TABLE t_1000 (k UInt16, s String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_1000 SELECT number % 1000, '' FROM numbers(65536);
INSERT INTO t_1000 SELECT 995, randomString(100000);

-- Only the `NULL` key: after the pass of the second state stops, no state of the call is priced in full, though the third would fit.
CREATE TABLE t_null_only (k Nullable(UInt16), s1 String, s2 String, s3 String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_null_only SELECT NULL, randomString(600000), randomString(600000), randomString(300000);

-- 101 states: the sample of the first 100 states misses the last one.
CREATE TABLE t_101 (k UInt16, s String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_101 SELECT number % 101, '' FROM numbers(65536);
INSERT INTO t_101 SELECT 100, randomString(100000);

-- A compressible state of almost 1 MiB is still priced in full, at its compressed size.
CREATE TABLE t_compressible (k UInt16, s String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_compressible SELECT number % 215, '' FROM numbers(65536);
INSERT INTO t_compressible SELECT 205, repeat(randomString(1000), 1000);

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

-- External aggregation is not supported at the moment, i.e., no statistics will be reported
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

SELECT k, max(s) FROM t GROUP BY k FORMAT Null SETTINGS log_comment='05768_autopr_state_size_estimate_small_table';
SELECT k, max(s) FROM t_nullable GROUP BY k FORMAT Null SETTINGS log_comment='05768_autopr_state_size_estimate_small_table_nullable';
SELECT k, max(s) FROM t_large GROUP BY k FORMAT Null SETTINGS log_comment='05768_autopr_state_size_estimate_small_table_large';
SELECT k, max(s) FROM t_many GROUP BY k FORMAT Null SETTINGS log_comment='05768_autopr_state_size_estimate_small_table_many';
SELECT k, max(s) FROM t_nullable_small GROUP BY k FORMAT Null SETTINGS log_comment='05768_autopr_state_size_estimate_small_table_nullable_small';
SELECT k, max(s1), max(s2) FROM t_two GROUP BY k FORMAT Null SETTINGS log_comment='05768_autopr_state_size_estimate_small_table_two';
SELECT k, max(s) FROM t_1000 GROUP BY k FORMAT Null SETTINGS log_comment='05768_autopr_state_size_estimate_small_table_1000';
SELECT k, max(s1), max(s2), max(s3) FROM t_null_only GROUP BY k FORMAT Null SETTINGS log_comment='05768_autopr_state_size_estimate_small_table_null_only';
SELECT k, max(s) FROM t_101 GROUP BY k FORMAT Null SETTINGS log_comment='05768_autopr_state_size_estimate_small_table_101';
SELECT k, max(s) FROM t_compressible GROUP BY k FORMAT Null SETTINGS log_comment='05768_autopr_state_size_estimate_small_table_compressible';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- The heavy state takes 100000 bytes and does not compress; the other 214 states are empty.
SELECT ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] BETWEEN 5e4 AND 2e5
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase())
    AND (log_comment = '05768_autopr_state_size_estimate_small_table') AND (type = 'QueryFinish');

SELECT ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] BETWEEN 5e4 AND 2e5
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase())
    AND (log_comment = '05768_autopr_state_size_estimate_small_table_nullable') AND (type = 'QueryFinish');

-- In the next two cases the sample stands; it sees only empty states.
SELECT ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] BETWEEN 1 AND 5e4
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase())
    AND (log_comment = '05768_autopr_state_size_estimate_small_table_large') AND (type = 'QueryFinish');

SELECT ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] BETWEEN 1 AND 5e4
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase())
    AND (log_comment = '05768_autopr_state_size_estimate_small_table_many') AND (type = 'QueryFinish');

SELECT ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] BETWEEN 5e4 AND 2e5
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase())
    AND (log_comment = '05768_autopr_state_size_estimate_small_table_nullable_small') AND (type = 'QueryFinish');

-- One heavy state is priced in full, the sample of the other one stands.
SELECT ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] BETWEEN 5e5 AND 9e5
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase())
    AND (log_comment = '05768_autopr_state_size_estimate_small_table_two') AND (type = 'QueryFinish');

SELECT ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] BETWEEN 5e4 AND 2e5
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase())
    AND (log_comment = '05768_autopr_state_size_estimate_small_table_1000') AND (type = 'QueryFinish');

-- Only the first state is priced; there is no non-`NULL` state to sample for the other two.
SELECT ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] BETWEEN 5e5 AND 7.5e5
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase())
    AND (log_comment = '05768_autopr_state_size_estimate_small_table_null_only') AND (type = 'QueryFinish');

SELECT ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] BETWEEN 5e4 AND 2e5
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase())
    AND (log_comment = '05768_autopr_state_size_estimate_small_table_101') AND (type = 'QueryFinish');

-- The state repeats one 1000-byte block, so it is charged a few kilobytes at most instead of its 1000000 bytes.
SELECT ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] BETWEEN 1e3 AND 1e5
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase())
    AND (log_comment = '05768_autopr_state_size_estimate_small_table_compressible') AND (type = 'QueryFinish');

DROP TABLE t;
DROP TABLE t_nullable;
DROP TABLE t_large;
DROP TABLE t_many;
DROP TABLE t_nullable_small;
DROP TABLE t_two;
DROP TABLE t_1000;
DROP TABLE t_null_only;
DROP TABLE t_101;
DROP TABLE t_compressible;
