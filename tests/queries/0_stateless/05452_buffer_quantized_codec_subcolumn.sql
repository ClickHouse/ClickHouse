-- The codes of a `Quantized('product', ...)` column cannot be computed from the rows a `Buffer` table holds in memory,
-- so reading them is refused with NOT_IMPLEMENTED until the buffer is flushed. Other subcolumns are read from the buffer.

SET enable_quantized_codec = 1;

DROP TABLE IF EXISTS t_05452_buf_pq;
DROP TABLE IF EXISTS t_05452_dst_pq;
DROP TABLE IF EXISTS t_05452_buf_int8;
DROP TABLE IF EXISTS t_05452_dst_int8;
DROP TABLE IF EXISTS t_05452_ref_int8;

CREATE TABLE t_05452_dst_pq (id UInt32, vec Array(Float32) CODEC(Quantized('product', 8, 2, 4))) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_05452_buf_pq AS t_05452_dst_pq ENGINE = Buffer(currentDatabase(), t_05452_dst_pq, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
CREATE TABLE t_05452_dst_int8 (id UInt32, vec Array(Float32) CODEC(Quantized('int8', 8))) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_05452_buf_int8 AS t_05452_dst_int8 ENGINE = Buffer(currentDatabase(), t_05452_dst_int8, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
CREATE TABLE t_05452_ref_int8 AS t_05452_dst_int8;

INSERT INTO t_05452_buf_pq SELECT number, arrayMap(j -> toFloat32((number * 7 + j) % 13), range(8)) FROM numbers(4);
INSERT INTO t_05452_buf_int8 SELECT number, arrayMap(j -> toFloat32((number * 7 + j) % 13), range(8)) FROM numbers(4);
INSERT INTO t_05452_ref_int8 SELECT number, arrayMap(j -> toFloat32((number * 7 + j) % 13), range(8)) FROM numbers(4);

-- The rows are in the buffer, not in the destination.
SELECT count() FROM t_05452_buf_pq;
SELECT count() FROM t_05452_dst_pq;

SELECT sum(cityHash64(vec.quantized)) FROM t_05452_buf_pq; -- { serverError NOT_IMPLEMENTED }
SELECT uniqExact(vec.product_quantization_codebook) FROM t_05452_buf_pq; -- { serverError NOT_IMPLEMENTED }

SELECT sum(vec.size0) FROM t_05452_buf_pq;

-- The `int8` codes do not depend on other rows, so the buffer returns the codes MergeTree stores.
SELECT count() FROM t_05452_buf_int8;
SELECT count() FROM t_05452_dst_int8;
SELECT (SELECT groupArray((id, hex(q))) FROM (SELECT id, vec.quantized AS q FROM t_05452_buf_int8 ORDER BY id))
     = (SELECT groupArray((id, hex(q))) FROM (SELECT id, vec.quantized AS q FROM t_05452_ref_int8 ORDER BY id));

-- After a flush the codes are read from the destination.
OPTIMIZE TABLE t_05452_buf_pq;
SELECT count(), uniqExact(length(vec.quantized)) FROM t_05452_buf_pq;

DROP TABLE t_05452_buf_pq;
DROP TABLE t_05452_dst_pq;
DROP TABLE t_05452_buf_int8;
DROP TABLE t_05452_dst_int8;
DROP TABLE t_05452_ref_int8;
