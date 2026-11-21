-- A `GROUP BY` over fixed-width keys and one `String` key packs the fixed-width values into the hash table key
-- as raw bytes (see `HashMethodKeysAndPackedString`). A negative zero is equal to a positive zero, so they are one group.

SET enable_fixed_and_packed_string_keys_in_aggregation = 1;
SET allow_suspicious_low_cardinality_types = 1;

SELECT x, s, count() FROM (SELECT arrayJoin([0., -0., 1.]) AS x, materialize('a') AS s) GROUP BY x, s ORDER BY ALL;
SELECT x, s, count() FROM (SELECT arrayJoin([0., -0., 1.])::Float32 AS x, materialize('a') AS s) GROUP BY x, s ORDER BY ALL;

SELECT '-- several fixed-width keys';
SELECT x, y, s, count()
FROM (SELECT arrayJoin([0., -0., 1.])::Float32 AS x, materialize(1::UInt32) AS y, materialize('a string that is not kept inline') AS s)
GROUP BY x, y, s ORDER BY ALL;

SELECT '-- LowCardinality';
SELECT x, s, count()
FROM (SELECT toLowCardinality(arrayJoin([0., -0., 1.])) AS x, toLowCardinality(materialize('a')) AS s)
GROUP BY x, s ORDER BY ALL;

SELECT '-- two-level';
SELECT x, s, count()
FROM (SELECT multiIf(number % 3 = 0, 0., number % 3 = 1, -0., 1.) AS x, toString(number % 2) AS s FROM numbers(30000))
GROUP BY x, s ORDER BY ALL
SETTINGS max_threads = 4, max_block_size = 1000, group_by_two_level_threshold = 1;

SELECT '-- aggregation in order';
DROP TABLE IF EXISTS t_negative_zero_packed_string;
CREATE TABLE t_negative_zero_packed_string (k UInt8, x Float64, lx LowCardinality(Float64), s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_negative_zero_packed_string SELECT number % 2, multiIf(number % 3 = 0, 0., number % 3 = 1, -0., 1.) AS x, x, toString(number % 2) FROM numbers(3000);

SELECT k, x, s, count() FROM t_negative_zero_packed_string GROUP BY k, x, s ORDER BY ALL
SETTINGS optimize_aggregation_in_order = 1, max_block_size = 100;
SELECT k, lx, s, count() FROM t_negative_zero_packed_string GROUP BY k, lx, s ORDER BY ALL
SETTINGS optimize_aggregation_in_order = 1, max_block_size = 100;

DROP TABLE t_negative_zero_packed_string;
