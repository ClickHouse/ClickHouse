-- Tags: no-parallel-replicas
-- no-parallel-replicas: the EXPLAIN check reads a plan node that parallel replicas replaces with a remote read step.
-- Random settings limits: materialize_statistics_on_insert=(1, 1); optimize_use_projections=(1, 1); optimize_use_implicit_projections=(1, 1); optimize_aggregation_in_order=(0, 0)

-- A part's statistics hold values of the column type the part was written with. When a read converts the part's
-- values to a different current type, min/max must not be answered from those statistics.

SET mutations_sync = 0, alter_sync = 0;
SET use_statistics_for_min_max_aggregation = 1;
SET force_aggregation_in_order = 0, aggregate_functions_null_for_empty = 0;

SELECT '-- UInt64 -> UInt8, 260 reads as 4';
DROP TABLE IF EXISTS t_narrowing;
CREATE TABLE t_narrowing (x UInt64 STATISTICS(basic)) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES t_narrowing;
INSERT INTO t_narrowing VALUES (250), (260);
ALTER TABLE t_narrowing MODIFY COLUMN x UInt8;
-- The mutation is pending and not applied on the fly.
SELECT min(x), max(x) FROM t_narrowing SETTINGS apply_mutations_on_fly = 0, apply_patch_parts = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_narrowing' SYNC FORMAT Null;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_narrowing' AND NOT is_done;
SELECT DISTINCT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_narrowing' AND active AND column = 'x';
-- The mutation is killed, the part keeps the old type.
SELECT min(x), max(x) FROM t_narrowing;
DROP TABLE t_narrowing;

SELECT '-- metadata-only Enum8 widening keeps min/max from statistics';
DROP TABLE IF EXISTS t_enum;
CREATE TABLE t_enum (e Enum8('a' = 1, 'b' = 2) STATISTICS(basic)) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES t_enum;
INSERT INTO t_enum VALUES ('a'), ('b');
ALTER TABLE t_enum MODIFY COLUMN e Enum8('a' = 1, 'b' = 2, 'c' = 3);
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_enum';
SELECT DISTINCT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_enum' AND active AND column = 'e';
SELECT min(e), max(e) FROM t_enum;
SELECT count() FROM (EXPLAIN SELECT min(e), max(e) FROM t_enum) WHERE explain LIKE '%_statistics_min_max_projection%';
DROP TABLE t_enum;
