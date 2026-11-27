-- `DROP PARTITION WHERE` and `DETACH PARTITION WHERE` drop or detach every partition for which
-- the predicate over `_partition_id` and `_partition_value` is true.

-- Date partition key: a range condition on the partition value.
DROP TABLE IF EXISTS t_dpw_date;
CREATE TABLE t_dpw_date (d Date, x UInt32) ENGINE = MergeTree ORDER BY x PARTITION BY d;
INSERT INTO t_dpw_date SELECT toDate('2026-11-20') + number, number FROM numbers(10);

ALTER TABLE t_dpw_date DROP PARTITION WHERE _partition_value.1 <= '2026-11-25';
SELECT 'date', groupArray(partition) FROM (SELECT DISTINCT partition FROM system.parts WHERE database = currentDatabase() AND table = 't_dpw_date' AND active ORDER BY partition);
SELECT 'date', count(), min(d) FROM t_dpw_date;

-- Functions evaluated once per query, such as `today`, are allowed.
ALTER TABLE t_dpw_date DROP PARTITION WHERE _partition_value.1 < today() - INTERVAL 10 YEAR;
SELECT 'date', count() FROM t_dpw_date;

DROP TABLE t_dpw_date;

-- `toYYYYMM` partition key: `LIKE` and `IN` on the partition ID.
DROP TABLE IF EXISTS t_dpw_month;
CREATE TABLE t_dpw_month (d Date, x UInt32) ENGINE = MergeTree ORDER BY x PARTITION BY toYYYYMM(d);
INSERT INTO t_dpw_month SELECT addMonths(toDate('2025-10-01'), number), number FROM numbers(6);

ALTER TABLE t_dpw_month DROP PARTITION WHERE _partition_id LIKE '2025%';
SELECT 'month', groupArray(partition_id) FROM (SELECT DISTINCT partition_id FROM system.parts WHERE database = currentDatabase() AND table = 't_dpw_month' AND active ORDER BY partition_id);

ALTER TABLE t_dpw_month DROP PARTITION WHERE _partition_id IN ('202601', '202603');
SELECT 'month', groupArray(partition_id) FROM (SELECT DISTINCT partition_id FROM system.parts WHERE database = currentDatabase() AND table = 't_dpw_month' AND active ORDER BY partition_id);

-- A query parameter in the predicate.
SET param_partition_id = '202602';
ALTER TABLE t_dpw_month DROP PARTITION WHERE _partition_id = {partition_id:String};
SELECT 'month', groupArray(partition_id) FROM (SELECT DISTINCT partition_id FROM system.parts WHERE database = currentDatabase() AND table = 't_dpw_month' AND active ORDER BY partition_id);

DROP TABLE t_dpw_month;

-- String partition key: the partition ID is a hash, so match the partition value.
DROP TABLE IF EXISTS t_dpw_str;
CREATE TABLE t_dpw_str (p String, x UInt32) ENGINE = MergeTree ORDER BY x PARTITION BY p;
INSERT INTO t_dpw_str VALUES ('1-1', 1), ('1-2', 2), ('2-1', 3), ('2-2', 4), ('2-3', 5), ('50%off', 6), ('50xoff', 7);

ALTER TABLE t_dpw_str DROP PARTITION WHERE _partition_value.1 LIKE '2-%';
SELECT 'str', groupArray(partition) FROM (SELECT DISTINCT partition FROM system.parts WHERE database = currentDatabase() AND table = 't_dpw_str' AND active ORDER BY partition);

-- An escaped wildcard matches only itself.
ALTER TABLE t_dpw_str DROP PARTITION WHERE _partition_value.1 LIKE '50\\%off';
SELECT 'str', groupArray(partition) FROM (SELECT DISTINCT partition FROM system.parts WHERE database = currentDatabase() AND table = 't_dpw_str' AND active ORDER BY partition);

-- A predicate that matches nothing is a no-op.
ALTER TABLE t_dpw_str DROP PARTITION WHERE _partition_value.1 = 'nothing';
ALTER TABLE t_dpw_str DROP PARTITION WHERE 0;
SELECT 'str', groupArray(partition) FROM (SELECT DISTINCT partition FROM system.parts WHERE database = currentDatabase() AND table = 't_dpw_str' AND active ORDER BY partition);

-- Combined with other commands in a single `ALTER`.
ALTER TABLE t_dpw_str DROP PARTITION WHERE _partition_value.1 = '1-1', DROP PARTITION '50xoff';
SELECT 'str', groupArray(partition) FROM (SELECT DISTINCT partition FROM system.parts WHERE database = currentDatabase() AND table = 't_dpw_str' AND active ORDER BY partition);

-- A constant true predicate drops everything.
ALTER TABLE t_dpw_str DROP PARTITION WHERE 1;
SELECT 'str', count() FROM t_dpw_str;

DROP TABLE t_dpw_str;

-- Tuple partition key: conditions on several elements of the partition value.
DROP TABLE IF EXISTS t_dpw_tuple;
CREATE TABLE t_dpw_tuple (a UInt32, b String, x UInt32) ENGINE = MergeTree ORDER BY x PARTITION BY (a, b);
INSERT INTO t_dpw_tuple VALUES (1, 'eu', 1), (1, 'us', 2), (2, 'eu', 3), (2, 'us', 4), (3, 'eu', 5);

ALTER TABLE t_dpw_tuple DROP PARTITION WHERE _partition_value.2 = 'eu' AND _partition_value.1 >= 2;
SELECT 'tuple', a, b, x FROM t_dpw_tuple ORDER BY x;

DROP TABLE t_dpw_tuple;

-- An unpartitioned table has the single partition `all`, and no `_partition_value`.
DROP TABLE IF EXISTS t_dpw_unpartitioned;
CREATE TABLE t_dpw_unpartitioned (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_dpw_unpartitioned VALUES (1), (2);

ALTER TABLE t_dpw_unpartitioned DROP PARTITION WHERE _partition_value = tuple(); -- { serverError UNKNOWN_IDENTIFIER }
ALTER TABLE t_dpw_unpartitioned DROP PARTITION WHERE _partition_id = 'other';
SELECT 'unpartitioned', count() FROM t_dpw_unpartitioned;
ALTER TABLE t_dpw_unpartitioned DROP PARTITION WHERE _partition_id = 'all';
SELECT 'unpartitioned', count() FROM t_dpw_unpartitioned;

DROP TABLE t_dpw_unpartitioned;

-- `DETACH PARTITION WHERE` moves the parts of the matched partitions to `detached`.
DROP TABLE IF EXISTS t_dpw_detach;
CREATE TABLE t_dpw_detach (p UInt32, x UInt32) ENGINE = MergeTree ORDER BY x PARTITION BY p;
INSERT INTO t_dpw_detach SELECT number % 5, number FROM numbers(10);

ALTER TABLE t_dpw_detach DETACH PARTITION WHERE _partition_value.1 % 2 = 1;
SELECT 'detach', groupArray(partition) FROM (SELECT DISTINCT partition FROM system.parts WHERE database = currentDatabase() AND table = 't_dpw_detach' AND active ORDER BY partition);
SELECT 'detach', groupArray(partition_id) FROM (SELECT DISTINCT partition_id FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_dpw_detach' ORDER BY partition_id);

ALTER TABLE t_dpw_detach ATTACH PARTITION 1;
SELECT 'detach', count() FROM t_dpw_detach;

DROP TABLE t_dpw_detach;

-- If one of the matched partitions cannot be dropped, none of them is dropped.
DROP TABLE IF EXISTS t_dpw_limit;
CREATE TABLE t_dpw_limit (p UInt32, s String) ENGINE = MergeTree ORDER BY p PARTITION BY p;
INSERT INTO t_dpw_limit VALUES (1, 'a');
INSERT INTO t_dpw_limit SELECT 2, randomString(100) FROM numbers(1000);

ALTER TABLE t_dpw_limit DROP PARTITION WHERE 1 SETTINGS max_partition_size_to_drop = 10000; -- { serverError TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT }
SELECT 'limit', groupArray(partition) FROM (SELECT DISTINCT partition FROM system.parts WHERE database = currentDatabase() AND table = 't_dpw_limit' AND active ORDER BY partition);

DROP TABLE t_dpw_limit;

-- The partitions with patch parts of lightweight updates: the patch parts of a dropped partition are dropped with it.
DROP TABLE IF EXISTS t_dpw_patches;
CREATE TABLE t_dpw_patches (p UInt32, x UInt32, y UInt32) ENGINE = MergeTree ORDER BY x PARTITION BY p
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_dpw_patches SELECT number % 3, number, 0 FROM numbers(6);

SET enable_lightweight_update = 1;
UPDATE t_dpw_patches SET y = x + 100 WHERE 1;
SELECT 'patches', count() > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_dpw_patches' AND active AND startsWith(partition_id, 'patch-');

ALTER TABLE t_dpw_patches DROP PARTITION WHERE _partition_value.1 = 1;
SELECT 'patches', p, x, y FROM t_dpw_patches ORDER BY x;

DROP TABLE t_dpw_patches;

-- Formatting round-trips.
SELECT formatQuery('ALTER TABLE t DROP PARTITION WHERE _partition_id LIKE \'2026%\' AND _partition_value.1 > 3, DETACH PARTITION WHERE (_partition_id = \'a\') OR 1');
SELECT formatQuerySingleLine('ALTER TABLE t DETACH PARTITION WHERE _partition_value = (1, \'a\')');

-- AST JSON round-trips, and parser-impossible shapes are rejected.
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t DROP PARTITION WHERE _partition_id = \'1\', DETACH PARTITION WHERE 1'));
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t DROP PARTITION WHERE 1'), '"command_type":"DROP_PARTITION"', '"command_type":"ATTACH_PARTITION"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t DROP PARTITION WHERE 1'), '"all":false', '"all":true')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t DROP PARTITION WHERE 1'), '"all":false', '"all":false,"fields_count":1')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t DROP PARTITION WHERE 1'), '"all":false', '"all":false,"id":{"type":"Literal","value":{"field_type":"String","value":"1"}}')); -- { serverError BAD_ARGUMENTS }

-- Errors.
DROP TABLE IF EXISTS t_dpw_errors;
CREATE TABLE t_dpw_errors (p UInt32, x UInt32) ENGINE = MergeTree ORDER BY x PARTITION BY p;
INSERT INTO t_dpw_errors VALUES (1, 1), (2, 2), (3, 3);

-- The predicate can refer only to `_partition_id` and `_partition_value`, not to the columns of a row.
ALTER TABLE t_dpw_errors DROP PARTITION WHERE p = 1; -- { serverError UNKNOWN_IDENTIFIER }
ALTER TABLE t_dpw_errors DROP PARTITION WHERE _part = 'all_1_1_0'; -- { serverError UNKNOWN_IDENTIFIER }
-- It must be deterministic.
ALTER TABLE t_dpw_errors DROP PARTITION WHERE rand() % 2 = 0; -- { serverError BAD_ARGUMENTS }
-- It cannot read other tables.
ALTER TABLE t_dpw_errors DROP PARTITION WHERE _partition_id IN (SELECT '1'); -- { serverError BAD_ARGUMENTS }
DROP TABLE IF EXISTS t_dpw_keys;
CREATE TABLE t_dpw_keys (k UInt32) ENGINE = Memory;
INSERT INTO t_dpw_keys VALUES (1);
ALTER TABLE t_dpw_errors DROP PARTITION WHERE _partition_value.1 IN t_dpw_keys; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_dpw_errors DROP PARTITION WHERE _partition_value.1 NOT IN t_dpw_keys; -- { serverError BAD_ARGUMENTS }
-- Also not through a SQL UDF.
DROP FUNCTION IF EXISTS udf_05875_dpw_in_subquery;
DROP FUNCTION IF EXISTS udf_05875_dpw_in_table;
DROP FUNCTION IF EXISTS udf_05875_dpw_scalar_subquery;
CREATE FUNCTION udf_05875_dpw_in_subquery AS (x) -> (x IN (SELECT k FROM t_dpw_keys));
CREATE FUNCTION udf_05875_dpw_in_table AS (x) -> (x IN t_dpw_keys);
CREATE FUNCTION udf_05875_dpw_scalar_subquery AS (x) -> (x = (SELECT max(k) FROM t_dpw_keys));
ALTER TABLE t_dpw_errors DROP PARTITION WHERE udf_05875_dpw_in_subquery(_partition_value.1); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_dpw_errors DROP PARTITION WHERE udf_05875_dpw_in_table(_partition_value.1); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_dpw_errors DROP PARTITION WHERE udf_05875_dpw_scalar_subquery(_partition_value.1); -- { serverError BAD_ARGUMENTS }
DROP FUNCTION udf_05875_dpw_in_subquery;
DROP FUNCTION udf_05875_dpw_in_table;
DROP FUNCTION udf_05875_dpw_scalar_subquery;
DROP TABLE t_dpw_keys;
-- It must keep a row for each partition: `arrayJoin` adds or removes rows.
ALTER TABLE t_dpw_errors DROP PARTITION WHERE arrayJoin(if(_partition_value.1 = 1, [1, 1], [0])) = 1; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_dpw_errors DROP PARTITION WHERE arrayJoin(if(_partition_value.1 = 1, [], [1])) = 1; -- { serverError BAD_ARGUMENTS }
DROP FUNCTION IF EXISTS udf_05875_dpw_array_join;
CREATE FUNCTION udf_05875_dpw_array_join AS (x) -> arrayJoin(if(x = 1, [1, 1], [0]));
ALTER TABLE t_dpw_errors DROP PARTITION WHERE udf_05875_dpw_array_join(_partition_value.1) = 1; -- { serverError BAD_ARGUMENTS }
DROP FUNCTION udf_05875_dpw_array_join;
-- It must be usable as a filter.
ALTER TABLE t_dpw_errors DROP PARTITION WHERE _partition_id; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }
-- An alias is not allowed.
ALTER TABLE t_dpw_errors DROP PARTITION WHERE (_partition_id = '1' AS c); -- { clientError SYNTAX_ERROR }
-- Only `DROP PARTITION` and `DETACH PARTITION` support `WHERE`.
ALTER TABLE t_dpw_errors DROP PART WHERE 1; -- { clientError SYNTAX_ERROR }
ALTER TABLE t_dpw_errors DROP DETACHED PARTITION WHERE 1; -- { clientError SYNTAX_ERROR }
ALTER TABLE t_dpw_errors FREEZE PARTITION WHERE 1; -- { clientError SYNTAX_ERROR }
ALTER TABLE t_dpw_errors ATTACH PARTITION WHERE 1; -- { clientError SYNTAX_ERROR }
ALTER TABLE t_dpw_errors DROP PARTITION WHERE; -- { clientError SYNTAX_ERROR }

-- Nothing was dropped by the failed queries.
SELECT 'errors', count() FROM t_dpw_errors;

DROP TABLE t_dpw_errors;

-- A materialized view forwards the command to its target table.
DROP TABLE IF EXISTS t_dpw_mv_src;
DROP TABLE IF EXISTS t_dpw_mv;
CREATE TABLE t_dpw_mv_src (p UInt32, x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW t_dpw_mv ENGINE = MergeTree ORDER BY x PARTITION BY p AS SELECT p, x FROM t_dpw_mv_src;
INSERT INTO t_dpw_mv_src VALUES (1, 1), (2, 2), (3, 3);

ALTER TABLE t_dpw_mv DROP PARTITION WHERE _partition_value.1 >= 2;
SELECT 'mv', p, x FROM t_dpw_mv ORDER BY x;

DROP TABLE t_dpw_mv;
DROP TABLE t_dpw_mv_src;

-- A physical column shadowing the virtual one is not a partition-level value.
DROP TABLE IF EXISTS t_dpw_shadow;
SET allow_reserved_column_names = 1;
CREATE TABLE t_dpw_shadow (_partition_id String, p UInt32) ENGINE = MergeTree ORDER BY p PARTITION BY p;
INSERT INTO t_dpw_shadow VALUES ('1', 1), ('2', 2);

ALTER TABLE t_dpw_shadow DROP PARTITION WHERE _partition_id = '1'; -- { serverError UNKNOWN_IDENTIFIER }
ALTER TABLE t_dpw_shadow DROP PARTITION WHERE _partition_value.1 = 1;
SELECT 'shadow', p FROM t_dpw_shadow ORDER BY p;

DROP TABLE t_dpw_shadow;
