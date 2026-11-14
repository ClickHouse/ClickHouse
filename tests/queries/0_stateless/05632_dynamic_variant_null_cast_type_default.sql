-- Tags: no-replicated-database
-- no-replicated-database: a refreshable view with a non-replicated target is refused on a Replicated database

-- { echo }
-- A NULL of Dynamic, Variant or a JSON path cast to a non-Nullable type gets the default of the type:
-- the first value for an Enum, not 0.

SET cast_keep_nullable = 0;
SET mutations_sync = 2;

SELECT CAST(NULL::Dynamic AS Enum8('x' = 1, 'y' = 2));
SELECT CAST(NULL::Dynamic AS Enum16('x' = 1000, 'y' = 2000));
SELECT CAST(NULL::Dynamic AS Enum8('m' = -3, 'n' = 0, 'o' = 4));
SELECT CAST(d AS Enum8('x' = 1, 'y' = 2)) FROM (SELECT arrayJoin(['y'::Dynamic, NULL]) AS d);
SELECT j.status::Enum8('x' = 1, 'y' = 2) FROM (SELECT '{"a" : 1}'::JSON AS j);
SELECT CAST(d AS Tuple(a UInt8, status Enum8('x' = 1, 'y' = 2))) FROM (SELECT arrayJoin([CAST(tuple(5), 'Tuple(a UInt8)')::Dynamic, NULL]) AS d);
SELECT CAST(NULL::Dynamic AS JSON(a Enum8('x' = 1, 'y' = 2)));
SELECT CAST([NULL, 'y']::Array(Dynamic) AS Array(Enum8('x' = 1, 'y' = 2)));
SELECT CAST(v AS Enum8('x' = 1, 'y' = 2)) FROM (SELECT arrayJoin(['y'::Variant(String, UInt8), NULL]) AS v);
SELECT CAST(v AS Tuple(a UInt8, status Enum8('x' = 1, 'y' = 2))) FROM (SELECT arrayJoin([CAST(tuple(5), 'Tuple(a UInt8)')::Variant(Tuple(a UInt8), String), NULL]) AS v);
SELECT CAST(v AS JSON(a Enum8('x' = 1, 'y' = 2))) FROM (SELECT arrayJoin(['{"a" : "y"}'::Variant(String, UInt8), NULL]) AS v);

-- A Nullable result keeps NULL.
SELECT CAST(NULL::Dynamic AS Enum8('x' = 1, 'y' = 2)) SETTINGS cast_keep_nullable = 1;
SELECT accurateCastOrNull(NULL::Dynamic, 'Enum8(\'x\' = 1, \'y\' = 2)');
SELECT accurateCastOrNull(t, 'Tuple(e Enum8(\'x\' = 1, \'y\' = 2))') FROM (SELECT CAST(tuple(d), 'Tuple(e Dynamic)') AS t FROM (SELECT arrayJoin([[1]::Array(UInt8)::Dynamic, NULL]) AS d));
SELECT accurateCastOrNull(t, 'Tuple(e Enum8(\'x\' = 1, \'y\' = 2))') FROM (SELECT CAST(tuple(v), 'Tuple(e Variant(Array(UInt8), String))') AS t FROM (SELECT arrayJoin([[1]::Array(UInt8)::Variant(Array(UInt8), String), NULL]) AS v));

-- Stored values: ALTER ... MODIFY COLUMN (a mutation), INSERT ... SELECT, materialized views.
CREATE TABLE t_dynamic (id UInt32, d Dynamic) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_variant (id UInt32, v Variant(String, UInt8)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_insert (id UInt32, e Enum8('x' = 1, 'y' = 2)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_mv (id UInt32, e Enum8('x' = 1, 'y' = 2)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_rmv (id UInt32, e Enum8('x' = 1, 'y' = 2)) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv TO t_mv AS SELECT id, d AS e FROM t_dynamic;
CREATE MATERIALIZED VIEW rmv REFRESH EVERY 1 YEAR APPEND TO t_rmv EMPTY AS SELECT id, d AS e FROM t_dynamic;
INSERT INTO t_dynamic VALUES (1, 'y'), (2, NULL);
INSERT INTO t_variant VALUES (1, 'y'), (2, NULL);
INSERT INTO t_insert SELECT id, d FROM t_dynamic;
SYSTEM REFRESH VIEW rmv;
SYSTEM WAIT VIEW rmv;
SELECT * FROM t_insert ORDER BY id;
SELECT * FROM t_mv ORDER BY id;
SELECT * FROM t_rmv ORDER BY id;
ALTER TABLE t_dynamic MODIFY COLUMN d Enum8('x' = 1, 'y' = 2);
ALTER TABLE t_variant MODIFY COLUMN v Enum8('x' = 1, 'y' = 2);
SELECT * FROM t_dynamic ORDER BY id;
SELECT * FROM t_variant ORDER BY id;

DROP TABLE rmv;
DROP TABLE mv;
DROP TABLE t_rmv;
DROP TABLE t_mv;
DROP TABLE t_insert;
DROP TABLE t_variant;
DROP TABLE t_dynamic;
