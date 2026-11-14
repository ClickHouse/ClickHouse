-- The conversion of 2^127, 2^128, 2^255 and 2^256 wraps, so index analysis must not treat it as monotonic on granules ending there.

DROP TABLE IF EXISTS t_float_to_big_int;
CREATE TABLE t_float_to_big_int (f Float64) ENGINE = MergeTree ORDER BY f
SETTINGS index_granularity = 2, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_float_to_big_int SELECT arrayJoin([1., pow(2, 127), pow(2, 127), pow(2, 128), pow(2, 128), pow(2, 255), pow(2, 255), pow(2, 256), pow(2, 256)]);

SELECT count() FROM t_float_to_big_int WHERE toInt128(f) < 0;
SELECT count() FROM t_float_to_big_int WHERE toUInt128(f) = 0;
SELECT count() FROM t_float_to_big_int WHERE toInt256(f) < 0;
SELECT count() FROM t_float_to_big_int WHERE toUInt256(f) = 0;

-- Granules that convert without wrapping are still pruned.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT f FROM t_float_to_big_int WHERE toUInt256(f) = 1) WHERE trimLeft(explain) LIKE 'Granules:%' SETTINGS enable_parallel_replicas = 0;

DROP TABLE t_float_to_big_int;
