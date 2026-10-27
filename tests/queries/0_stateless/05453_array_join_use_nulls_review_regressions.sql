-- Index analysis over `MergeTree` must keep the empty-array rows that `array_join_use_nulls` turns into `NULL`.
DROP TABLE IF EXISTS t_aj_nulls_mt;
CREATE TABLE t_aj_nulls_mt (id UInt8, arr Array(UInt8)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_aj_nulls_mt VALUES (1, []), (2, [5]);

SELECT 'index';
SELECT id FROM t_aj_nulls_mt LEFT ARRAY JOIN arr AS x WHERE x IS NULL
SETTINGS array_join_use_nulls = 1, query_plan_fuse_filter_into_array_join = 0;
SELECT id FROM t_aj_nulls_mt LEFT ARRAY JOIN arr AS x WHERE x IS NULL
SETTINGS array_join_use_nulls = 1, query_plan_fuse_filter_into_array_join = 0, query_plan_enable_optimizations = 0;
DROP TABLE t_aj_nulls_mt;

-- A filter fused into `ARRAY JOIN` must see the `Nullable` element type.
DROP TABLE IF EXISTS t_aj_nulls_mem;
CREATE TABLE t_aj_nulls_mem (id UInt8, arr Array(UInt8)) ENGINE = Memory;
INSERT INTO t_aj_nulls_mem VALUES (1, []), (2, [0, 5]), (3, [7]);

SELECT 'fused';
SELECT id, x, toTypeName(x) FROM t_aj_nulls_mem LEFT ARRAY JOIN arr AS x WHERE x > 0 ORDER BY id, x
SETTINGS array_join_use_nulls = 1, query_plan_fuse_filter_into_array_join = 1;
SELECT id, x FROM t_aj_nulls_mem LEFT ARRAY JOIN arr AS x WHERE x IS NULL ORDER BY id
SETTINGS array_join_use_nulls = 1, query_plan_fuse_filter_into_array_join = 1;
SELECT 'not fused';
SELECT id, x, toTypeName(x) FROM t_aj_nulls_mem LEFT ARRAY JOIN arr AS x WHERE x > 0 ORDER BY id, x
SETTINGS array_join_use_nulls = 1, query_plan_fuse_filter_into_array_join = 0;
SELECT id, x FROM t_aj_nulls_mem LEFT ARRAY JOIN arr AS x WHERE x IS NULL ORDER BY id
SETTINGS array_join_use_nulls = 1, query_plan_fuse_filter_into_array_join = 0;
DROP TABLE t_aj_nulls_mem;

-- A view over `Array(Nullable(T))` with the setting disabled keeps converting to the declared type.
DROP VIEW IF EXISTS v_aj_nulls;
CREATE VIEW v_aj_nulls (x UInt8) AS
SELECT x FROM (SELECT CAST([1], 'Array(Nullable(UInt8))') AS arr) LEFT ARRAY JOIN arr AS x
SETTINGS array_join_use_nulls = 0;

SELECT 'view';
SELECT x, toTypeName(x) FROM v_aj_nulls SETTINGS array_join_use_nulls = 0;
DROP VIEW v_aj_nulls;
