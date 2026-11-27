-- an empty set behind IN (subquery) ends the read also when the AND around it is JIT-compiled
DROP TABLE IF EXISTS t_jit_empty_in;
DROP TABLE IF EXISTS t_jit_empty_in_set;
DROP TABLE IF EXISTS t_jit_empty_in_storage;

CREATE TABLE t_jit_empty_in (a UInt64, b UInt64, n Nullable(UInt64)) ENGINE = ReplacingMergeTree ORDER BY a;
CREATE TABLE t_jit_empty_in_set (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_jit_empty_in_storage (x UInt64) ENGINE = Set;
INSERT INTO t_jit_empty_in SELECT number, number, number FROM numbers(100000);

SET compile_expressions = 1, min_count_to_compile_expression = 0;
SET optimize_move_to_prewhere = 0, optimize_move_to_prewhere_if_final = 0, use_query_condition_cache = 0;

-- the parenthesized AND stays a nested `and` of the filter, so the `and` above the IN is compiled
SELECT count() /* assert_no_read */ FROM t_jit_empty_in WHERE (a > 10 AND a < 90000) AND b IN (SELECT x FROM t_jit_empty_in_set);
SELECT count() /* assert_no_read */ FROM t_jit_empty_in WHERE (a > 10 AND a < 90000) AND n IN (SELECT x FROM t_jit_empty_in_set);
SELECT count() /* assert_no_read */ FROM t_jit_empty_in FINAL WHERE (a > 10 AND a < 90000) AND n IN (SELECT x FROM t_jit_empty_in_set);
SELECT count() /* assert_no_read */ FROM t_jit_empty_in WHERE (a > 10 AND a < 90000) AND (b > 5 AND n IN (SELECT x FROM t_jit_empty_in_set));
-- without splitting the AND chain into separate filters, the comparisons are compiled together with the `and`
SELECT count() /* assert_no_read */ FROM t_jit_empty_in WHERE a > 10 AND b IN (SELECT x FROM t_jit_empty_in_set) SETTINGS query_plan_merge_filters = 0;
-- transform_null_in rewrites IN to nullIn, which is 0 over an empty set as well
SELECT count() /* assert_no_read */ FROM t_jit_empty_in WHERE (a > 10 AND a < 90000) AND n IN (SELECT x FROM t_jit_empty_in_set) SETTINGS transform_null_in = 1;
-- NOT IN over an empty set matches everything
SELECT count() FROM t_jit_empty_in WHERE (a > 10 AND a < 90000) AND b NOT IN (SELECT x FROM t_jit_empty_in_set);
-- an ENGINE = Set table can be filled while the query runs, so its empty set does not end the read
SELECT count() /* set_table_read */ FROM t_jit_empty_in WHERE (a > 10 AND a < 90000) AND b IN t_jit_empty_in_storage;

SYSTEM FLUSH LOGS query_log;
SELECT read_rows FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND query LIKE '%assert_no_read%' AND query NOT LIKE '%query_log%'
ORDER BY event_time_microseconds;
SELECT read_rows > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND query LIKE '%set_table_read%' AND query NOT LIKE '%query_log%';

DROP TABLE t_jit_empty_in;
DROP TABLE t_jit_empty_in_set;
DROP TABLE t_jit_empty_in_storage;
