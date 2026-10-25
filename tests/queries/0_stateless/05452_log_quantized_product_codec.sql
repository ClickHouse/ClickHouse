-- Tags: log-engine
-- Log and TinyLog refuse the `Quantized('product')` codec: its codebook is written per insert but read once.
-- The data-independent methods keep working.

SET enable_quantized_codec = 1;

DROP TABLE IF EXISTS t_log;
DROP TABLE IF EXISTS t_tinylog;
DROP TABLE IF EXISTS t_mt;
DROP TABLE IF EXISTS t_clone;
DROP TABLE IF EXISTS t_ctas_log;
DROP TABLE IF EXISTS t_ctas_tinylog;
DROP TABLE IF EXISTS t_ctas_tuple_log;
DROP TABLE IF EXISTS t_ctas_tuple_tinylog;
DROP TABLE IF EXISTS t_ctas_array_log;
DROP TABLE IF EXISTS t_ctas_plain;
DROP TABLE IF EXISTS t_log_int8;
DROP TABLE IF EXISTS t_tinylog_int8;

CREATE TABLE t_log (id UInt32, vec Array(Float32) CODEC(Quantized('product', 8, 2, 4))) ENGINE = Log; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_tinylog (id UInt32, vec Array(Float32) CODEC(Quantized('product', 8, 2, 4))) ENGINE = TinyLog; -- { serverError ILLEGAL_COLUMN }

CREATE TABLE t_mt (id UInt32, vec Array(Float32) CODEC(Quantized('product', 8, 2, 4))) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_clone AS t_mt ENGINE = TinyLog; -- { serverError ILLEGAL_COLUMN }
-- A column inferred from a SELECT keeps the source column's serialization, without its codec.
CREATE TABLE t_ctas_log ENGINE = Log AS SELECT * FROM t_mt; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_ctas_tinylog ENGINE = TinyLog AS SELECT * FROM t_mt; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_ctas_tuple_log ENGINE = Log AS SELECT id, tuple(vec) AS tv FROM t_mt; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_ctas_tuple_tinylog ENGINE = TinyLog AS SELECT id, tuple(vec) AS tv FROM t_mt; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_ctas_array_log ENGINE = Log AS SELECT id, [vec] AS vv FROM t_mt; -- { serverError ILLEGAL_COLUMN }
INSERT INTO t_mt SELECT number, arrayMap(j -> toFloat32((number * 7 + j) % 13), range(8)) FROM numbers(4);
CREATE TABLE t_ctas_plain (id UInt32, vec Array(Float32)) ENGINE = Log AS SELECT * FROM t_mt;
SELECT count(), sum(arraySum(vec)) FROM t_ctas_plain;
DROP TABLE t_ctas_plain;
DROP TABLE t_mt;

-- A full-definition ATTACH states a new definition and is refused like CREATE.
SET allow_deprecated_database_ordinary = 1;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary;
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (id UInt32, vec Array(Float32) CODEC(Quantized('product', 8, 2, 4))) ENGINE = Log; -- { serverError ILLEGAL_COLUMN }
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE t_log_int8 (id UInt32, vec Array(Float32) CODEC(Quantized('int8', 8))) ENGINE = Log;
INSERT INTO t_log_int8 SELECT number, arrayMap(j -> toFloat32((number * 7 + j) % 13), range(8)) FROM numbers(4);
SELECT count(), sum(arraySum(vec)), sum(length(vec.quantized)) FROM t_log_int8;

CREATE TABLE t_tinylog_int8 (id UInt32, vec Array(Float32) CODEC(Quantized('int8', 8))) ENGINE = TinyLog;
INSERT INTO t_tinylog_int8 SELECT number, arrayMap(j -> toFloat32((number * 7 + j) % 13), range(8)) FROM numbers(4);
SELECT count(), sum(arraySum(vec)), sum(length(vec.quantized)) FROM t_tinylog_int8;

DROP TABLE t_log_int8;
DROP TABLE t_tinylog_int8;
