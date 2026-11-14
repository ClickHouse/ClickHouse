SET query_plan_optimize_join_order_randomize = 0, query_plan_optimize_join_order_limit = 10, query_plan_join_swap_table = false; -- the right side must stay on the right
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0; -- disable automatic spilling
SET enable_parallel_replicas = 0; -- join optimization is disabled with parallel replicas
SET collect_hash_table_stats_during_joins = 1, max_entries_for_hash_table_stats = 10000;
SET join_algorithm = 'hash', hash_join_layout = 'auto', parallel_hash_join_threshold = 1000;

CREATE TABLE lhs (a UInt64) ENGINE = MergeTree ORDER BY ();
CREATE TABLE rhs (a UInt64) ENGINE = MergeTree ORDER BY ();
INSERT INTO lhs SELECT number FROM numbers(1000);
-- Not empty: an empty table is read by `ReadNothingStep`, whose statistics key is shared by every empty table of the server
INSERT INTO rhs VALUES (0);

-- The right side has no row estimate and reads nothing: parallel layout on a cold run, serial once the empty build is cached
SELECT * FROM lhs AS t0 LEFT JOIN (SELECT DISTINCT a FROM rhs WHERE a > 0) AS t1 ON t0.a = t1.a SETTINGS log_comment = '05632_empty_cold' FORMAT Null;
SELECT * FROM lhs AS t0 LEFT JOIN (SELECT DISTINCT a FROM rhs WHERE a > 0) AS t1 ON t0.a = t1.a SETTINGS log_comment = '05632_empty_warm' FORMAT Null;

CREATE TABLE lhs16 (a UInt16) ENGINE = MergeTree ORDER BY ();
CREATE TABLE rhs16 (a UInt16) ENGINE = MergeTree ORDER BY ();
INSERT INTO lhs16 SELECT number FROM numbers(1000);
INSERT INTO rhs16 VALUES (0);
SELECT * FROM lhs16 AS t0 LEFT JOIN (SELECT DISTINCT a FROM rhs16 WHERE a > 0) AS t1 ON t0.a = t1.a SETTINGS log_comment = '05632_empty_cold_uint16' FORMAT Null;
SELECT * FROM lhs16 AS t0 LEFT JOIN (SELECT DISTINCT a FROM rhs16 WHERE a > 0) AS t1 ON t0.a = t1.a SETTINGS log_comment = '05632_empty_warm_uint16' FORMAT Null;
-- Then rows: a parallel build forced by `parallel_hash_join_threshold = 0` updates the cached 0 rows, so the next run is parallel
INSERT INTO rhs16 SELECT number + 1 FROM numbers(3000);
SELECT * FROM lhs16 AS t0 LEFT JOIN (SELECT DISTINCT a FROM rhs16 WHERE a > 0) AS t1 ON t0.a = t1.a SETTINGS parallel_hash_join_threshold = 0, log_comment = '05632_filled_uint16' FORMAT Null;
SELECT * FROM lhs16 AS t0 LEFT JOIN (SELECT DISTINCT a FROM rhs16 WHERE a > 0) AS t1 ON t0.a = t1.a SETTINGS log_comment = '05632_after_filled_uint16' FORMAT Null;

CREATE TABLE lhs2 (a String) ENGINE = MergeTree ORDER BY ();
CREATE TABLE rhs2 (a String, b UInt8) ENGINE = MergeTree ORDER BY ();
INSERT INTO lhs2 SELECT toString(number) FROM numbers(1000);
INSERT INTO rhs2 SELECT '1', 1 FROM numbers(3000);

-- One key in 6000 rows, then no rows with the same plan: the cached 6000 rows are replaced by 0, so the next run is serial
SELECT * FROM lhs2 AS t0 LEFT JOIN (SELECT a FROM rhs2 WHERE b = 1 UNION ALL SELECT a FROM rhs2 WHERE b = 1) AS t1 ON t0.a = t1.a SETTINGS log_comment = '05632_one_key' FORMAT Null;
ALTER TABLE rhs2 UPDATE b = 0 WHERE 1 SETTINGS mutations_sync = 2;
SELECT * FROM lhs2 AS t0 LEFT JOIN (SELECT a FROM rhs2 WHERE b = 1 UNION ALL SELECT a FROM rhs2 WHERE b = 1) AS t1 ON t0.a = t1.a SETTINGS log_comment = '05632_emptied' FORMAT Null;
SELECT * FROM lhs2 AS t0 LEFT JOIN (SELECT a FROM rhs2 WHERE b = 1 UNION ALL SELECT a FROM rhs2 WHERE b = 1) AS t1 ON t0.a = t1.a SETTINGS log_comment = '05632_after_empty' FORMAT Null;

CREATE TABLE rhs4 (a String, b UInt8, n UInt64) ENGINE = MergeTree ORDER BY ();
INSERT INTO rhs4 SELECT '1', 1, number FROM numbers(3000);

-- One key in 6000 rows, then the same key in 2 rows: only `source_rows` drops, and the next run is serial
SELECT * FROM lhs2 AS t0 LEFT JOIN (SELECT a FROM rhs4 WHERE b = 1 UNION ALL SELECT a FROM rhs4 WHERE b = 1) AS t1 ON t0.a = t1.a SETTINGS log_comment = '05632_rows_many' FORMAT Null;
ALTER TABLE rhs4 UPDATE b = 0 WHERE n > 0 SETTINGS mutations_sync = 2;
SELECT * FROM lhs2 AS t0 LEFT JOIN (SELECT a FROM rhs4 WHERE b = 1 UNION ALL SELECT a FROM rhs4 WHERE b = 1) AS t1 ON t0.a = t1.a SETTINGS log_comment = '05632_rows_few' FORMAT Null;
SELECT * FROM lhs2 AS t0 LEFT JOIN (SELECT a FROM rhs4 WHERE b = 1 UNION ALL SELECT a FROM rhs4 WHERE b = 1) AS t1 ON t0.a = t1.a SETTINGS log_comment = '05632_after_rows_few' FORMAT Null;

CREATE TABLE rhs5 (a String, b UInt8) ENGINE = MergeTree ORDER BY ();
INSERT INTO rhs5 VALUES ('1', 1);

-- One row, then none, at `parallel_hash_join_threshold = 1`: 0 is less than half of 1, so the next run is serial
SELECT * FROM lhs2 AS t0 LEFT JOIN (SELECT DISTINCT a FROM rhs5 WHERE b = 1) AS t1 ON t0.a = t1.a SETTINGS parallel_hash_join_threshold = 1, log_comment = '05632_half_one' FORMAT Null;
ALTER TABLE rhs5 UPDATE b = 0 WHERE 1 SETTINGS mutations_sync = 2;
SELECT * FROM lhs2 AS t0 LEFT JOIN (SELECT DISTINCT a FROM rhs5 WHERE b = 1) AS t1 ON t0.a = t1.a SETTINGS parallel_hash_join_threshold = 1, log_comment = '05632_half_emptied' FORMAT Null;
SELECT * FROM lhs2 AS t0 LEFT JOIN (SELECT DISTINCT a FROM rhs5 WHERE b = 1) AS t1 ON t0.a = t1.a SETTINGS parallel_hash_join_threshold = 1, log_comment = '05632_half_after' FORMAT Null;

CREATE TABLE lhs3 (k05632 UInt64) ENGINE = MergeTree ORDER BY ();
CREATE TABLE empty3 (k05632 UInt64) ENGINE = MergeTree ORDER BY ();
CREATE TABLE log3 (k05632 UInt64) ENGINE = Log;
INSERT INTO lhs3 SELECT number FROM numbers(1000);
INSERT INTO log3 SELECT number FROM numbers(3000);

-- An empty table must not share the statistics entry of a table of another engine
SELECT * FROM lhs3 AS t0 LEFT JOIN log3 AS t1 ON t0.k05632 = t1.k05632 SETTINGS log_comment = '05632_log' FORMAT Null;
SELECT * FROM lhs3 AS t0 LEFT JOIN empty3 AS t1 ON t0.k05632 = t1.k05632 SETTINGS log_comment = '05632_empty_table' FORMAT Null;
SELECT * FROM lhs3 AS t0 LEFT JOIN log3 AS t1 ON t0.k05632 = t1.k05632 SETTINGS log_comment = '05632_log_after_empty' FORMAT Null;

CREATE TABLE empty_log3 (k05632 UInt64) ENGINE = Log;
CREATE TABLE stripe3 (k05632 UInt64) ENGINE = StripeLog;
CREATE TABLE empty_stripe3 (k05632 UInt64) ENGINE = StripeLog;
INSERT INTO stripe3 SELECT number FROM numbers(3000);

-- Nor that of another table of its own engine, also when both are read through `remote`
SELECT * FROM lhs3 AS t0 LEFT JOIN empty_log3 AS t1 ON t0.k05632 = t1.k05632 SETTINGS log_comment = '05632_empty_log' FORMAT Null;
SELECT * FROM lhs3 AS t0 LEFT JOIN log3 AS t1 ON t0.k05632 = t1.k05632 SETTINGS log_comment = '05632_log_after_empty_log' FORMAT Null;
SELECT * FROM lhs3 AS t0 LEFT JOIN stripe3 AS t1 ON t0.k05632 = t1.k05632 SETTINGS log_comment = '05632_stripe' FORMAT Null;
SELECT * FROM lhs3 AS t0 LEFT JOIN empty_stripe3 AS t1 ON t0.k05632 = t1.k05632 SETTINGS log_comment = '05632_empty_stripe' FORMAT Null;
SELECT * FROM lhs3 AS t0 LEFT JOIN stripe3 AS t1 ON t0.k05632 = t1.k05632 SETTINGS log_comment = '05632_stripe_after_empty' FORMAT Null;
SELECT * FROM lhs3 AS t0 LEFT JOIN remote('127.0.0.1', currentDatabase(), log3) AS t1 ON t0.k05632 = t1.k05632
    SETTINGS prefer_localhost_replica = 0, log_comment = '05632_remote' FORMAT Null;
SELECT * FROM lhs3 AS t0 LEFT JOIN remote('127.0.0.1', currentDatabase(), empty_log3) AS t1 ON t0.k05632 = t1.k05632
    SETTINGS prefer_localhost_replica = 0, log_comment = '05632_remote_empty' FORMAT Null;
SELECT * FROM lhs3 AS t0 LEFT JOIN remote('127.0.0.1', currentDatabase(), log3) AS t1 ON t0.k05632 = t1.k05632
    SETTINGS prefer_localhost_replica = 0, log_comment = '05632_remote_after_empty' FORMAT Null;

-- Nor that of the same table function with other arguments. Only the last query is checked: earlier runs of this test share the entries
SELECT * FROM lhs3 AS t0 LEFT JOIN (SELECT * FROM values('k05632 UInt64', 1, 2, 3) WHERE k05632 > 1) AS t1 ON t0.k05632 = t1.k05632
    SETTINGS parallel_hash_join_threshold = 1, log_comment = '05632_values' FORMAT Null;
SELECT * FROM lhs3 AS t0 LEFT JOIN (SELECT * FROM values('k05632 UInt64', 1) WHERE k05632 > 1) AS t1 ON t0.k05632 = t1.k05632
    SETTINGS parallel_hash_join_threshold = 1, log_comment = '05632_values_empty' FORMAT Null;
SELECT * FROM lhs3 AS t0 LEFT JOIN (SELECT * FROM values('k05632 UInt64', 1, 2, 3) WHERE k05632 > 1) AS t1 ON t0.k05632 = t1.k05632
    SETTINGS parallel_hash_join_threshold = 1, log_comment = '05632_values_after_empty' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['HashJoinBuiltWithSerialLayout'] AS serial, ProfileEvents['HashJoinBuiltWithParallelLayout'] AS parallel
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
    AND log_comment IN ('05632_empty_cold', '05632_empty_warm', '05632_empty_cold_uint16', '05632_empty_warm_uint16', '05632_filled_uint16',
        '05632_after_filled_uint16', '05632_one_key', '05632_emptied', '05632_after_empty', '05632_rows_many', '05632_rows_few',
        '05632_after_rows_few', '05632_half_one', '05632_half_emptied', '05632_half_after', '05632_log', '05632_empty_table',
        '05632_log_after_empty', '05632_empty_log', '05632_log_after_empty_log', '05632_stripe', '05632_empty_stripe',
        '05632_stripe_after_empty', '05632_remote', '05632_remote_empty', '05632_remote_after_empty', '05632_values_after_empty')
ORDER BY event_time_microseconds;
