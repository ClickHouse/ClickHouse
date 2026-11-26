-- CAST of a Nullable(JSON) does not convert the typed paths of its NULL rows.

SET json_use_optimized_type_conversion = 1;

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS mt;

CREATE TABLE src (k UInt8, j Nullable(JSON(a String)), e Nullable(JSON(a String))) ENGINE = Memory;
INSERT INTO src VALUES (1, NULL, NULL), (2, '{"a":"5"}', '{"a":"x"}'), (3, '{"a":"7"}', '{"a":"y"}');

SELECT k, CAST(j, 'Nullable(JSON(a Int32))') FROM src ORDER BY k;
SELECT k, CAST(e, 'Nullable(JSON(a Enum8(\'x\' = 1, \'y\' = 2)))') FROM src ORDER BY k;
SELECT k, CAST(j, 'Nullable(JSON(a Int32))') FROM src WHERE k = 1;
SELECT k, CAST(tuple(j), 'Tuple(Nullable(JSON(a Int32)))') FROM src ORDER BY k;
SELECT k, CAST([j], 'Array(Nullable(JSON(a Int32)))') FROM src ORDER BY k;
SELECT k, CAST(map('m', j), 'Map(String, Nullable(JSON(a Int32)))') FROM src ORDER BY k;
SELECT k, accurateCastOrNull(j, 'Nullable(JSON(a Int32))') FROM src ORDER BY k;
SELECT k, CAST(j, 'Nullable(JSON(a Int32))') FROM src ORDER BY k SETTINGS json_use_optimized_type_conversion = 0;

CREATE TABLE dst (k UInt8, j Nullable(JSON(a Int32))) ENGINE = Memory;
INSERT INTO dst SELECT k, j FROM src;
SELECT * FROM dst ORDER BY k;

CREATE TABLE mt (k UInt8, j Nullable(JSON(a String))) ENGINE = MergeTree ORDER BY k;
INSERT INTO mt SELECT k, j FROM src;
ALTER TABLE mt MODIFY COLUMN j Nullable(JSON(a Int32)) SETTINGS mutations_sync = 2;
SELECT * FROM mt ORDER BY k;

-- An unparsable value under a non-NULL row still throws.
SELECT CAST(e, 'Nullable(JSON(a Int32))') FROM src; -- { serverError INCORRECT_DATA }

DROP TABLE src;
DROP TABLE dst;
DROP TABLE mt;
