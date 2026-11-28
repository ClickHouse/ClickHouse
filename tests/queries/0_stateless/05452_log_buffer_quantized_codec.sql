-- Tags: log-engine

SET enable_quantized_codec = 1;

CREATE TABLE t_mt (id UInt32, vec Array(Float32) CODEC(Quantized('product', 8, 2, 4))) ENGINE = MergeTree ORDER BY id;

CREATE TABLE t_log (id UInt32, vec Array(Float32) CODEC(Quantized('int8', 8))) ENGINE = Log; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_tinylog AS t_mt ENGINE = TinyLog; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_buffer ENGINE = Buffer(currentDatabase(), t_mt, 1, 1000, 1000, 1000000, 1000000, 100000000, 100000000); -- { serverError ILLEGAL_COLUMN }

CREATE TABLE t_buffer (id UInt32, vec Array(Float32)) ENGINE = Buffer(currentDatabase(), t_mt, 1, 1000, 1000, 1000000, 1000000, 100000000, 100000000);
ALTER TABLE t_buffer MODIFY COLUMN vec CODEC(Quantized('rabitq', 8)); -- { serverError ILLEGAL_COLUMN }
INSERT INTO t_buffer SELECT number, arrayMap(j -> toFloat32(number + j), range(8)) FROM numbers(4);
SELECT count(), sum(arraySum(vec)) FROM t_buffer;

-- A table created AS SELECT gets the column without the codec.
CREATE TABLE t_ctas ENGINE = Log AS SELECT tuple(vec) AS t FROM t_mt;
DROP TABLE t_ctas;

DROP TABLE t_buffer;
DROP TABLE t_mt;
