-- Test rewrite of `INTERSECT ALL` and `EXCEPT ALL` to a join.
-- The join counts the right rows of each key, and every left row uses up one of them. `INTERSECT ALL` runs as INNER ANY.
-- `EXCEPT ALL` runs as LEFT ANTI, or as RIGHT ANTI when the sides are swapped.

SET rewrite_intersect_except_all_to_join = 1;
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
SELECT count(), sum(a), countIf(a IS NULL), countIf(b IS NULL) FROM (SELECT * FROM t_left INTERSECT ALL SELECT * FROM t_right)
SETTINGS query_plan_join_swap_table = 'false';
SELECT count(), sum(a), countIf(a IS NULL), countIf(b IS NULL) FROM (SELECT * FROM t_left INTERSECT ALL SELECT * FROM t_right)
SETTINGS query_plan_join_swap_table = 'true';

SELECT countIf(explain LIKE '%Type: inner | Strictness: any%')
FROM (EXPLAIN actions = 1 SELECT * FROM t_left INTERSECT ALL SELECT * FROM t_right);

SELECT count(), sum(a), countIf(a IS NULL), countIf(b IS NULL) FROM (SELECT * FROM t_left EXCEPT ALL SELECT * FROM t_right)
SETTINGS query_plan_join_swap_table = 'false';

SELECT countIf(explain LIKE '%Type: left | Strictness: anti%')
FROM (EXPLAIN actions = 1 SELECT * FROM t_left EXCEPT ALL SELECT * FROM t_right)
SETTINGS query_plan_join_swap_table = 'false';

SELECT count(), sum(a), countIf(a IS NULL), countIf(b IS NULL) FROM (SELECT * FROM t_left EXCEPT ALL SELECT * FROM t_right)
SETTINGS query_plan_join_swap_table = 'true';

SELECT countIf(explain LIKE '%Type: right | Strictness: anti%')
FROM (EXPLAIN actions = 1 SELECT * FROM t_left EXCEPT ALL SELECT * FROM t_right)
SETTINGS query_plan_join_swap_table = 'true';

SELECT '-- `Dynamic` values match only with the same type.';
SELECT d, dynamicType(d) FROM (SELECT arrayJoin([1::Int64::Dynamic, 1::Int32::Dynamic, 1::Int32::Dynamic, 'a'::Dynamic, NULL::Dynamic, NULL::Dynamic]) AS d INTERSECT ALL SELECT arrayJoin([1::Int32::Dynamic, 'a'::Dynamic, NULL::Dynamic, NULL::Dynamic, 2::UInt8::Dynamic])) ORDER BY toString(d), dynamicType(d);

SELECT '-- Values are compared after the conversion to the common type.';
SELECT * FROM (SELECT arrayJoin([0, 1, 1, 2]) AS x EXCEPT ALL SELECT arrayJoin([1.0, 1.5])) ORDER BY x;

SELECT '-- Two columns with the same name in a branch stay apart.';
SELECT * FROM (SELECT number % 2, number % 2 FROM numbers(4) INTERSECT ALL SELECT 1, 1);

SELECT '-- Under another join.';
SELECT count(), sum(i.a), sum(e.a) FROM (SELECT * FROM t_left INTERSECT ALL SELECT * FROM t_right) AS i
INNER JOIN (SELECT * FROM t_left EXCEPT ALL SELECT * FROM t_right) AS e ON i.a = e.a;

SELECT '-- The empty probe side must not stop RIGHT ANTI from emitting the build side.';
SELECT count() FROM (SELECT number % 5 FROM numbers(10) EXCEPT ALL SELECT number FROM numbers(0))
SETTINGS query_plan_join_swap_table = 'true';

SELECT '-- The empty probe side stops LEFT ANTI from reading the build side.';
SELECT count() FROM (SELECT number FROM numbers(0) EXCEPT ALL SELECT number FROM system.numbers)
SETTINGS max_rows_to_read = 10000000, read_overflow_mode = 'throw', query_plan_join_swap_table = 'false';

SELECT '-- A join that spills gives the same result.';
SELECT count() FROM (SELECT number % 5000 FROM numbers(10000) INTERSECT ALL SELECT number FROM numbers(2000, 6000))
SETTINGS max_bytes_before_external_join = 400000;
SELECT count() FROM (SELECT number % 5000 FROM numbers(10000) EXCEPT ALL SELECT number FROM numbers(2000, 6000))
SETTINGS max_bytes_before_external_join = 400000;

SELECT '-- The totals of the first branch win.';
SELECT * FROM (SELECT number % 3 AS k, count() AS c FROM numbers(100000) GROUP BY k WITH TOTALS INTERSECT ALL SELECT number % 3 AS k, count() AS c FROM numbers(0) GROUP BY k WITH TOTALS) ORDER BY k
SETTINGS query_plan_join_swap_table = 'false';
SELECT * FROM (SELECT number % 3 AS k, count() AS c FROM numbers(100000) GROUP BY k WITH TOTALS INTERSECT ALL SELECT number % 3 AS k, count() AS c FROM numbers(0) GROUP BY k WITH TOTALS) ORDER BY k
SETTINGS query_plan_join_swap_table = 'true';

SELECT '-- Only the second branch has totals.';
SELECT * FROM (SELECT number % 3 AS k, count() AS c FROM numbers(9) GROUP BY k EXCEPT ALL SELECT number % 2 AS k2, toUInt64(count() - 1) AS c2 FROM numbers(8) GROUP BY k2 WITH TOTALS) ORDER BY k;

SELECT '-- One row of a key stands for its copies in the result, also when the result is split into small blocks.';
SELECT count(), sum(x) FROM (SELECT number % 10 AS x FROM numbers(10000) INTERSECT ALL SELECT number % 15 FROM numbers(5000))
SETTINGS max_block_size = 10000, max_joined_block_size_rows = 100, joined_block_split_single_row = 1, query_plan_join_swap_table = 'false';
SELECT count(), sum(x) FROM (SELECT number % 10 AS x FROM numbers(10000) EXCEPT ALL SELECT number % 15 FROM numbers(5000))
SETTINGS max_block_size = 10000, max_joined_block_size_rows = 100, joined_block_split_single_row = 1, query_plan_join_swap_table = 'false';

SELECT '-- A block with more keys than the probe pre-aggregation table holds.';
SELECT count(), sum(x) FROM (SELECT intDiv(number, 2) % 5000 AS x FROM numbers(100000) INTERSECT ALL SELECT number % 6000 FROM numbers(30000))
SETTINGS max_block_size = 65536, query_plan_join_swap_table = 'false';
SELECT count(), sum(x) FROM (SELECT intDiv(number, 2) % 5000 AS x FROM numbers(100000) EXCEPT ALL SELECT number % 6000 FROM numbers(30000))
SETTINGS max_block_size = 65536, query_plan_join_swap_table = 'false';

SELECT '-- Repeated rows with lazy output, which a small LIMIT above the join turns on.';
SELECT count(), sum(a), sum(b), countIf(c = '1') FROM (
    SELECT * FROM (SELECT number % 10 AS a, number % 3 AS b, toString(number % 2) AS c FROM numbers(10000)
        INTERSECT ALL SELECT number % 15, number % 3, toString(number % 2) FROM numbers(5000))
    ORDER BY ALL LIMIT 1000)
SETTINGS query_plan_join_swap_table = 'false';
SELECT count(), sum(a), sum(b), countIf(c = '1') FROM (
    SELECT * FROM (SELECT number % 10 AS a, number % 3 AS b, toString(number % 2) AS c FROM numbers(10000)
        EXCEPT ALL SELECT number % 15, number % 3, toString(number % 2) FROM numbers(5000))
    ORDER BY ALL LIMIT 1000)
SETTINGS query_plan_join_swap_table = 'false';

SELECT '-- A swapped EXCEPT ALL keeps only the right blocks with a row it outputs.';

DROP TABLE IF EXISTS t_long_left;
DROP TABLE IF EXISTS t_long_right;
-- Wide parts and small granules, so the reads do not affect the memory limit.
CREATE TABLE t_long_left (x String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 100;
CREATE TABLE t_long_right (x String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 100;
INSERT INTO t_long_left SELECT concat(repeat('x', 1024), toString(number % 2)) FROM numbers(40000);
INSERT INTO t_long_right SELECT concat(repeat('x', 1024), toString(number % 2)) FROM numbers(30000);

SELECT count(), uniqExact(x) FROM (SELECT x FROM t_long_left EXCEPT ALL SELECT x FROM t_long_right)
SETTINGS query_plan_join_swap_table = 'true', max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    use_columns_cache = 0, max_memory_usage = 25000000;

DROP TABLE t_left;
DROP TABLE t_right;
DROP TABLE t_long_left;
DROP TABLE t_long_right;
