-- Test rewrite of `INTERSECT DISTINCT` and `EXCEPT DISTINCT` to a join.
-- `INTERSECT DISTINCT` runs as INNER ANY. `EXCEPT DISTINCT` runs as LEFT ANTI followed by `DISTINCT`, or as RIGHT ANTI
-- when the sides are swapped.

SET rewrite_intersect_except_distinct_to_join = 1;
SET query_plan_optimize_join_order_limit = 10; -- `query_plan_join_swap_table` needs the join order optimizer.
SET max_threads = 4;
SET max_block_size = 100;

-- Keys repeat many times, and their duplicates are read in different blocks and threads.
DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;
CREATE TABLE t_left (a Nullable(UInt64), b LowCardinality(Nullable(String)), c Array(UInt8)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 100;
CREATE TABLE t_right (a Nullable(UInt64), b LowCardinality(Nullable(String)), c Array(UInt8)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 100;
INSERT INTO t_left SELECT if(number % 13 = 0, NULL, number % 500), if(number % 11 = 0, NULL, toString(number % 7)), [number % 2] FROM numbers(10000);
INSERT INTO t_right SELECT if(number % 17 = 0, NULL, number % 350 + 100), if(number % 5 = 0, NULL, toString(number % 7)), [number % 2] FROM numbers(5000);

SELECT '-- semantics and plan';
SELECT count(), sum(a), countIf(a IS NULL), countIf(b IS NULL) FROM (SELECT * FROM t_left INTERSECT DISTINCT SELECT * FROM t_right);
SELECT count(), sum(a), countIf(a IS NULL), countIf(b IS NULL) FROM (SELECT * FROM t_left INTERSECT DISTINCT SELECT * FROM t_right)
SETTINGS query_plan_join_swap_table = 'true';

SELECT countIf(explain LIKE '%Type: inner | Strictness: any%'), countIf(explain LIKE '%Skip deduplication: 1%')
FROM (EXPLAIN actions = 1 SELECT * FROM t_left INTERSECT DISTINCT SELECT * FROM t_right);

SELECT count(), sum(a), countIf(a IS NULL), countIf(b IS NULL) FROM (SELECT * FROM t_left EXCEPT DISTINCT SELECT * FROM t_right)
SETTINGS query_plan_join_swap_table = 'false';

SELECT countIf(explain LIKE '%Type: left | Strictness: anti%'), countIf(explain LIKE '%Skip deduplication: 1%')
FROM (EXPLAIN actions = 1 SELECT * FROM t_left EXCEPT DISTINCT SELECT * FROM t_right)
SETTINGS query_plan_join_swap_table = 'false';

SELECT count(), sum(a), countIf(a IS NULL), countIf(b IS NULL) FROM (SELECT * FROM t_left EXCEPT DISTINCT SELECT * FROM t_right)
SETTINGS query_plan_join_swap_table = 'true';

SELECT countIf(explain LIKE '%Type: right | Strictness: anti%'), countIf(explain LIKE '%Skip deduplication: 1%')
FROM (EXPLAIN actions = 1 SELECT * FROM t_left EXCEPT DISTINCT SELECT * FROM t_right)
SETTINGS query_plan_join_swap_table = 'true';

SELECT '-- `Dynamic` values match only with the same type.';
SELECT d, dynamicType(d) FROM (SELECT arrayJoin([1::Int64::Dynamic, 1::Int32::Dynamic, 'a'::Dynamic, NULL::Dynamic, 2::Int64::Dynamic, 2::Int64::Dynamic]) AS d INTERSECT DISTINCT SELECT arrayJoin([1::Int32::Dynamic, 'a'::Dynamic, NULL::Dynamic, 2::UInt8::Dynamic])) ORDER BY toString(d), dynamicType(d);

SELECT '-- Values are compared after the conversion to the common type.';
SELECT * FROM (SELECT arrayJoin([0, 1, 2]) AS x EXCEPT DISTINCT SELECT arrayJoin([1.0, 1.5])) ORDER BY x;

SELECT '-- Two columns with the same name in a branch stay apart.';
SELECT * FROM (SELECT number, number FROM numbers(3) INTERSECT DISTINCT SELECT 1, 1);

SELECT '-- Under another join.';
SELECT count(), sum(i.a), sum(e.a) FROM (SELECT * FROM t_left INTERSECT DISTINCT SELECT * FROM t_right) AS i
INNER JOIN (SELECT * FROM t_left EXCEPT DISTINCT SELECT * FROM t_right) AS e ON i.a = e.a;

SELECT '-- The empty probe side must not stop RIGHT ANTI from emitting the build side.';
SELECT count() FROM (SELECT number FROM numbers(10) EXCEPT DISTINCT SELECT number FROM numbers(0))
SETTINGS query_plan_join_swap_table = 'true';

SELECT '-- The empty probe side stops LEFT ANTI from reading the build side.';
SELECT count() FROM (SELECT number FROM numbers(0) EXCEPT DISTINCT SELECT number FROM system.numbers)
SETTINGS max_rows_to_read = 10000000, read_overflow_mode = 'throw', query_plan_join_swap_table = 'false';

SELECT '-- A join that spills gives the same result.';
-- The left side is a single stream, so the other probe streams are empty while the spilled buckets are joined.
SELECT count() FROM (SELECT number FROM numbers(10000) INTERSECT DISTINCT SELECT number FROM numbers(2000, 6000))
SETTINGS max_bytes_before_external_join = 400000;
SELECT count() FROM (SELECT number FROM numbers(10000) EXCEPT DISTINCT SELECT number FROM numbers(2000, 6000))
SETTINGS max_bytes_before_external_join = 400000;

SELECT '-- limits of DISTINCT';
SELECT count() FROM (SELECT number FROM numbers(10) INTERSECT DISTINCT SELECT number FROM numbers(10))
SETTINGS max_rows_in_distinct = 3; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
-- No `DISTINCT` set is built when the join emits each row once, so `max_bytes_in_distinct` is not enforced.
SELECT count() FROM (SELECT number FROM numbers(10) INTERSECT DISTINCT SELECT number FROM numbers(5))
SETTINGS max_bytes_in_distinct = 1, make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1,
    distributed_plan_workers_num = 2, distributed_plan_fallback_to_local_execution = 0;
SELECT count() FROM (SELECT number FROM numbers(10) EXCEPT DISTINCT SELECT number FROM numbers(5))
SETTINGS max_bytes_in_distinct = 1, make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1,
    distributed_plan_workers_num = 2, distributed_plan_fallback_to_local_execution = 0, query_plan_join_swap_table = 'false';

SELECT '-- The totals of the first branch win.';
SELECT * FROM (SELECT number % 3 AS k, count() AS c FROM numbers(100000) GROUP BY k WITH TOTALS INTERSECT DISTINCT SELECT number % 3 AS k, count() AS c FROM numbers(0) GROUP BY k WITH TOTALS) ORDER BY k
SETTINGS query_plan_join_swap_table = 'false';
SELECT * FROM (SELECT number % 3 AS k, count() AS c FROM numbers(100000) GROUP BY k WITH TOTALS INTERSECT DISTINCT SELECT number % 3 AS k, count() AS c FROM numbers(0) GROUP BY k WITH TOTALS) ORDER BY k
SETTINGS query_plan_join_swap_table = 'true';

SELECT '-- Only the second branch has totals.';
SELECT * FROM (SELECT number % 3 AS k, count() AS c FROM numbers(9) GROUP BY k EXCEPT DISTINCT SELECT number % 2 AS k2, toUInt64(count() - 1) AS c2 FROM numbers(8) GROUP BY k2 WITH TOTALS) ORDER BY k;

DROP TABLE t_left;
DROP TABLE t_right;
