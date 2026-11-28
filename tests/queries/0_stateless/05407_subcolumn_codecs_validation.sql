SET enable_subcolumn_codecs = 1;

DROP TABLE IF EXISTS t_subcolumn_codecs_src;
DROP TABLE IF EXISTS t_subcolumn_codecs_target;
DROP TABLE IF EXISTS t_subcolumn_codecs_mem;
DROP TABLE IF EXISTS t_subcolumn_codecs_existing;
DROP TABLE IF EXISTS t_subcolumn_codecs_copy;
DROP TABLE IF EXISTS t_subcolumn_codecs_restored;

-- Declarations of one column never overlap and are declared once.
CREATE TABLE t_subcolumn_codecs_bad (p Tuple(a UInt8, b Tuple(c UInt8)), CODEC p.b (ZSTD), CODEC p.b.c (ZSTD)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_subcolumn_codecs_bad (p Tuple(a UInt8) CODEC(ZSTD), CODEC p.a (ZSTD)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_subcolumn_codecs_bad (p Tuple(a UInt8), CODEC p.a (ZSTD), CODEC p.a (LZ4)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_subcolumn_codecs_bad (p Tuple(a UInt8) CODEC(ZSTD), CODEC p (LZ4)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- The target must be an existing column or subcolumn.
CREATE TABLE t_subcolumn_codecs_bad (p Tuple(a UInt8), CODEC p.x (ZSTD)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_subcolumn_codecs_bad (p Tuple(a UInt8), CODEC q (ZSTD)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_subcolumn_codecs_bad (CODEC p.a (ZSTD)) ENGINE = MergeTree ORDER BY tuple() AS SELECT tuple(1) AS p; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_subcolumn_codecs_bad (x UInt8, p Tuple(a UInt8) ALIAS tuple(x), CODEC p.a (ZSTD)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- The codec is validated against the type of the subcolumn.
CREATE TABLE t_subcolumn_codecs_bad (p Tuple(a String), CODEC p.a (Gorilla)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_subcolumn_codecs_bad (p Tuple(a String), CODEC p.a (Strnig)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError UNKNOWN_CODEC }

-- Nothing below a dynamic type can be a target, the dynamic value itself can.
CREATE TABLE t_subcolumn_codecs_bad (v Variant(String, UInt64), CODEC v.String (ZSTD)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_subcolumn_codecs_bad (p Tuple(j JSON(a UInt8)), CODEC p.j.a (ZSTD)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_subcolumn_codecs_ok (p Tuple(j JSON, m Map(String, Float64), a Array(UInt8)), CODEC p.j (ZSTD(3)), CODEC p.m (ZSTD(1)), CODEC p.a (Delta, LZ4)) ENGINE = MergeTree ORDER BY tuple();
SELECT name, subcolumn_codecs FROM system.columns WHERE database = currentDatabase() AND table = 't_subcolumn_codecs_ok';
INSERT INTO t_subcolumn_codecs_ok VALUES (('{"a":1,"b":"x"}', map('k', 1.5), [1, 2]));
SELECT p.j.a, p.j.b, p.m['k'], p.a FROM t_subcolumn_codecs_ok;
DROP TABLE t_subcolumn_codecs_ok;

-- A declarative codec like `Quantized` works for a column, which gets the serialization it needs, but not for a subcolumn.
SET enable_quantized_codec = 1;
CREATE TABLE t_subcolumn_codecs_ok (vec Array(Float32), CODEC vec (Quantized('int8', 8))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_subcolumn_codecs_ok VALUES ([1, 2, 3, 4, 5, 6, 7, 8]);
SELECT 'quantized column';
SELECT length(vec.quantized) FROM t_subcolumn_codecs_ok;
DETACH TABLE t_subcolumn_codecs_ok;
ATTACH TABLE t_subcolumn_codecs_ok;
SELECT length(vec.quantized) FROM t_subcolumn_codecs_ok;
DROP TABLE t_subcolumn_codecs_ok;
CREATE TABLE t_subcolumn_codecs_bad (p Tuple(v Array(Float32)), CODEC p.v (Quantized('int8', 8))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- Only engines that compress each stream with its own codec accept subcolumn codecs.
CREATE TABLE t_subcolumn_codecs_bad (p Tuple(a UInt8), CODEC p.a (ZSTD)) ENGINE = Memory; -- { serverError NOT_IMPLEMENTED }
CREATE TABLE t_subcolumn_codecs_mem (p Tuple(a UInt8)) ENGINE = Memory;
ALTER TABLE t_subcolumn_codecs_mem MODIFY COLUMN p.a CODEC(ZSTD); -- { serverError NOT_IMPLEMENTED }
DROP TABLE t_subcolumn_codecs_mem;

CREATE TABLE t_subcolumn_codecs_src (p Tuple(a UInt8)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_subcolumn_codecs_target (p Tuple(a UInt8)) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW mv_subcolumn_codecs_to TO t_subcolumn_codecs_target (p Tuple(a UInt8), CODEC p.a (ZSTD)) AS SELECT p FROM t_subcolumn_codecs_src; -- { serverError NOT_IMPLEMENTED }
CREATE MATERIALIZED VIEW mv_subcolumn_codecs_inner (p Tuple(a UInt8), CODEC p.a (ZSTD)) ENGINE = MergeTree ORDER BY tuple() AS SELECT p FROM t_subcolumn_codecs_src;
SELECT 'inner table of a materialized view';
SELECT name, subcolumn_codecs FROM system.columns WHERE database = currentDatabase() AND table LIKE '.inner%' AND name = 'p';
DROP TABLE mv_subcolumn_codecs_inner;
DROP TABLE t_subcolumn_codecs_target;
DROP TABLE t_subcolumn_codecs_src;

-- The setting guards new declarations for subcolumns, not the column form and not tables that already have them.
SELECT 'gate';
SET enable_subcolumn_codecs = 0;
CREATE TABLE t_subcolumn_codecs_bad (p Tuple(a UInt8), CODEC p.a (ZSTD)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE t_subcolumn_codecs_ok (id UInt8, CODEC id (ZSTD)) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE t_subcolumn_codecs_ok;

SET enable_subcolumn_codecs = 1;
CREATE TABLE t_subcolumn_codecs_existing (id UInt64, p Tuple(a UInt64, b String), CODEC p.a (Delta, ZSTD(1))) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_subcolumn_codecs_copy AS t_subcolumn_codecs_existing;
SELECT replaceRegexpOne(create_table_query, ' SETTINGS .*$', '') FROM system.tables WHERE database = currentDatabase() AND name = 't_subcolumn_codecs_copy';
DROP TABLE t_subcolumn_codecs_copy;
SET enable_subcolumn_codecs = 0;
INSERT INTO t_subcolumn_codecs_existing SELECT number, (number, 'x') FROM numbers(10);
DETACH TABLE t_subcolumn_codecs_existing;
ATTACH TABLE t_subcolumn_codecs_existing;
SELECT replaceRegexpOne(create_table_query, ' SETTINGS .*$', '') FROM system.tables WHERE database = currentDatabase() AND name = 't_subcolumn_codecs_existing';
CREATE TABLE t_subcolumn_codecs_copy AS t_subcolumn_codecs_existing; -- { serverError SUPPORT_IS_DISABLED }

BACKUP TABLE t_subcolumn_codecs_existing TO Memory('05407_subcolumn_codecs') FORMAT Null;
RESTORE TABLE t_subcolumn_codecs_existing AS t_subcolumn_codecs_restored FROM Memory('05407_subcolumn_codecs') FORMAT Null;
SELECT 'restored';
SELECT replaceRegexpOne(create_table_query, ' SETTINGS .*$', '') FROM system.tables WHERE database = currentDatabase() AND name = 't_subcolumn_codecs_restored';
SELECT count(), sum(p.a) FROM t_subcolumn_codecs_restored;

DROP TABLE t_subcolumn_codecs_restored;
DROP TABLE t_subcolumn_codecs_existing;
