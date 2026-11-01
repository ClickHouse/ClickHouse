-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test checks EXPLAIN of plans with reading steps replaced by prepared sources.

-- A part's statistics hold the values of the column type the part was written with. While a part still stores a
-- column with another type, a read converts its values to the current type, so `min`/`max` must not be answered from
-- those statistics: neither while the `MODIFY COLUMN` is pending and not applied on the fly, nor after it is killed.
-- The test cases are ported from https://github.com/ClickHouse/ClickHouse/pull/126076.

SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET use_statistics_for_min_max_aggregation = 1;
SET enable_analyzer = 1;
SET optimize_aggregation_in_order = 0, force_aggregation_in_order = 0;
SET aggregate_functions_null_for_empty = 0;
SET mutations_sync = 0, alter_sync = 0;

SELECT 'UInt64 to UInt8, 260 reads as 4';
DROP TABLE IF EXISTS t_05454_narrowing;
CREATE TABLE t_05454_narrowing (x UInt64 STATISTICS(basic)) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES t_05454_narrowing;
INSERT INTO t_05454_narrowing SETTINGS materialize_statistics_on_insert = 1 VALUES (250), (260);
ALTER TABLE t_05454_narrowing MODIFY COLUMN x UInt8;
-- The mutation is pending and not applied on the fly, so it is not in the mutations snapshot of the query.
SELECT min(x), max(x) FROM t_05454_narrowing SETTINGS apply_mutations_on_fly = 0, apply_patch_parts = 0;
SELECT min(x), max(x) FROM t_05454_narrowing SETTINGS apply_mutations_on_fly = 0, apply_patch_parts = 0, use_statistics_for_min_max_aggregation = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_05454_narrowing' SYNC FORMAT Null;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_05454_narrowing' AND NOT is_done;
SELECT DISTINCT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_05454_narrowing' AND active AND column = 'x';
-- The mutation is killed, and the part keeps the old type.
SELECT min(x), max(x) FROM t_05454_narrowing;
SELECT count() FROM (EXPLAIN SELECT min(x), max(x) FROM t_05454_narrowing) WHERE explain LIKE '%_statistics_min_max_projection%';
DROP TABLE t_05454_narrowing;

-- A change of the scale keeps the type of `Field`, so the old statistics would pass a check of the `Field` type.
SELECT 'Decimal64(2) to Decimal64(1)';
DROP TABLE IF EXISTS t_05454_decimal;
CREATE TABLE t_05454_decimal (d Decimal64(2) STATISTICS(basic)) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES t_05454_decimal;
INSERT INTO t_05454_decimal SETTINGS materialize_statistics_on_insert = 1 VALUES (1.21), (5.61);
ALTER TABLE t_05454_decimal MODIFY COLUMN d Decimal64(1);
KILL MUTATION WHERE database = currentDatabase() AND table = 't_05454_decimal' SYNC FORMAT Null;
SELECT DISTINCT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_05454_decimal' AND active AND column = 'd';
SELECT min(d), max(d) FROM t_05454_decimal;
SELECT min(d), max(d) FROM t_05454_decimal SETTINGS use_statistics_for_min_max_aggregation = 0;
DROP TABLE t_05454_decimal;
