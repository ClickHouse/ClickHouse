-- Tags: log-engine
-- A table created AS SELECT from a `Quantized`-coded column gets the type without the codec, and reads back after a reload.

SET enable_quantized_codec = 1;

DROP TABLE IF EXISTS t_rmv;
DROP TABLE IF EXISTS t_mv;
DROP TABLE IF EXISTS t_mv_dst;
DROP TABLE IF EXISTS t_src;
DROP TABLE IF EXISTS t_log;
DROP TABLE IF EXISTS t_log_empty;
DROP TABLE IF EXISTS t_log_nested;
DROP TABLE IF EXISTS t_mt;
DROP TABLE IF EXISTS t_saf_src;
DROP TABLE IF EXISTS t_saf;

CREATE TABLE t_src (id UInt32, vec Array(Float32) CODEC(Quantized('int8', 8))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_src SELECT number, arrayMap(j -> toFloat32((number * 7 + j) % 13), range(8)) FROM numbers(4);

CREATE TABLE t_log ENGINE = Log AS SELECT * FROM t_src;
INSERT INTO t_log SELECT number + 4, arrayMap(j -> toFloat32((number * 5 + j) % 11), range(8)) FROM numbers(4);

CREATE TABLE t_log_empty ENGINE = Log EMPTY AS SELECT * FROM t_src;
INSERT INTO t_log_empty SELECT * FROM t_src;

CREATE TABLE t_log_nested ENGINE = Log AS SELECT id, [vec] AS a, tuple(vec, 1) AS t, map('k', vec) AS m FROM t_src;

CREATE TABLE t_mt ENGINE = MergeTree ORDER BY id AS SELECT * FROM t_src;

CREATE TABLE t_saf_src (id UInt32, vec SimpleAggregateFunction(anyLast, Array(Float32)) CODEC(Quantized('int8', 8))) ENGINE = AggregatingMergeTree ORDER BY id;
INSERT INTO t_saf_src SELECT * FROM t_src;
CREATE TABLE t_saf ENGINE = Log AS SELECT id, vec, tuple(vec) AS t FROM t_saf_src;

-- The custom name is part of the type and is kept.
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_saf' ORDER BY name;

SELECT vec.quantized FROM t_log; -- { serverError UNKNOWN_IDENTIFIER }
SELECT vec.quantized FROM t_mt; -- { serverError UNKNOWN_IDENTIFIER }

DETACH TABLE t_log;
ATTACH TABLE t_log;
DETACH TABLE t_log_empty;
ATTACH TABLE t_log_empty;
DETACH TABLE t_log_nested;
ATTACH TABLE t_log_nested;
DETACH TABLE t_mt;
ATTACH TABLE t_mt;
DETACH TABLE t_saf;
ATTACH TABLE t_saf;

SELECT count(), sum(arraySum(vec)) FROM t_log;
SELECT count(), sum(arraySum(vec)) FROM t_log_empty;
SELECT count(), sum(length(a)), sum(arraySum(t.1)), sum(arraySum(m['k'])) FROM t_log_nested;
SELECT count(), sum(arraySum(vec)) FROM t_mt;
SELECT count(), sum(arraySum(vec)), sum(arraySum(t.1)) FROM t_saf;

-- A view whose query is replaced by MODIFY QUERY gets the same types as one created with it.
CREATE TABLE t_mv_dst (id UInt32, vec Array(Float32) CODEC(Quantized('int8', 8))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_mv_dst SELECT * FROM t_src;
CREATE MATERIALIZED VIEW t_mv TO t_mv_dst AS SELECT id, vec FROM t_src WHERE id >= 100;
ALTER TABLE t_mv MODIFY QUERY SELECT id, vec FROM t_src;
SELECT vec.quantized FROM t_mv; -- { serverError UNKNOWN_IDENTIFIER }
SELECT count(), sum(arraySum(vec)) FROM t_mv;
CREATE MATERIALIZED VIEW t_rmv REFRESH EVERY 1 YEAR APPEND TO t_mv_dst EMPTY AS SELECT id, vec FROM t_src WHERE id >= 100;
ALTER TABLE t_rmv MODIFY QUERY SELECT id, vec FROM t_src;
SELECT vec.quantized FROM t_rmv; -- { serverError UNKNOWN_IDENTIFIER }

-- The source keeps its codec subcolumn.
SELECT count(), sum(length(vec.quantized)) FROM t_src;

DROP TABLE t_rmv;
DROP TABLE t_mv;
DROP TABLE t_mv_dst;
DROP TABLE t_src;
DROP TABLE t_log;
DROP TABLE t_log_empty;
DROP TABLE t_log_nested;
DROP TABLE t_mt;
DROP TABLE t_saf_src;
DROP TABLE t_saf;
