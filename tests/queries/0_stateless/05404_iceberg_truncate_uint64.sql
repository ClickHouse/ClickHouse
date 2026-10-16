-- https://github.com/ClickHouse/ClickHouse/issues/125948

SELECT icebergTruncate(3, 9223372036854775807) AS a, icebergTruncate(3, 9223372036854775809) AS b, toTypeName(b), icebergTruncate(3, 18446744073709551615) AS c;
SELECT icebergTruncate(10, toUInt64(10000000000000000000)) AS d, toTypeName(d);
SELECT icebergTruncate(3, x) FROM (SELECT arrayJoin([toUInt64(10), toUInt64(9223372036854775809)]) AS x);
SELECT toTypeName(icebergTruncate(3, toNullable(toUInt64(1)))), toTypeName(icebergTruncate(3, toLowCardinality(toUInt64(1))));
SELECT icebergTruncate(3, toNullable(toUInt64(18446744073709551615))), icebergTruncate(3, toLowCardinality(toUInt64(18446744073709551615)));

-- Unchanged: the result of narrower unsigned types already holds every value.
SELECT icebergTruncate(3, toUInt8(200)) AS e, toTypeName(e), icebergTruncate(3, toUInt32(4000000000)) AS f, toTypeName(f);

DROP TABLE IF EXISTS t_partitioned;
CREATE TABLE t_partitioned (x UInt64) ENGINE = MergeTree PARTITION BY icebergTruncate(1000, x) ORDER BY x;
INSERT INTO t_partitioned VALUES (500), (9223372036854775809), (18446744073709551000);
SELECT DISTINCT _partition_id FROM t_partitioned ORDER BY 1;
SELECT count() FROM t_partitioned WHERE x > 9000000000000000000;
DROP TABLE t_partitioned;

DROP TABLE IF EXISTS t_sorted_trunc;
CREATE TABLE t_sorted_trunc (x UInt64) ENGINE = MergeTree ORDER BY icebergTruncate(1000, x) SETTINGS index_granularity = 1;
INSERT INTO t_sorted_trunc VALUES (0), (1000), (2000), (3000), (4000), (18446744073709550000), (18446744073709551000);
SELECT count() FROM t_sorted_trunc WHERE x >= 18446744073709550000 SETTINGS max_rows_to_read = 3;
DROP TABLE t_sorted_trunc;

-- As `Int64`, the results of these rows are 9223372036854775000 and -9223372036854775616.
DROP TABLE IF EXISTS t_wrap;
CREATE TABLE t_wrap (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_wrap VALUES (9223372036854775900), (9223372036854776500);
SELECT count() FROM t_wrap WHERE icebergTruncate(1000, x) >= 9223372036854776000;
SELECT count() FROM t_wrap WHERE icebergTruncate(1000, x) < 0;

DROP TABLE IF EXISTS t_dynamic;
CREATE TABLE t_dynamic (d Dynamic) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dynamic SELECT toUInt64(18446744073709551615) FROM numbers(1000000);
-- The query condition cache must not serve the verdict of the first query to the second one.
SELECT count() FROM t_dynamic WHERE icebergTruncate(3, d) = '-1' SETTINGS use_query_condition_cache = 1, optimize_move_to_prewhere = 1, parallel_replicas_local_plan = 1;
SELECT count() FROM t_dynamic WHERE icebergTruncate(3, d) = '-1' SETTINGS use_query_condition_cache = 1, optimize_move_to_prewhere = 1, parallel_replicas_local_plan = 1, function_iceberg_truncate_returns_uint64_on_uint64_argument = 0;

SELECT icebergTruncate(3, 18446744073709551615) AS c, toTypeName(c) SETTINGS compatibility = '26.10';

SET function_iceberg_truncate_returns_uint64_on_uint64_argument = 0;

SELECT icebergTruncate(3, 18446744073709551615) AS c, toTypeName(c);
SELECT count() FROM t_wrap WHERE icebergTruncate(1000, x) < 0;

DROP TABLE IF EXISTS t_wrap_sorted;
CREATE TABLE t_wrap_sorted (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1;
INSERT INTO t_wrap_sorted VALUES (9223372036854775900), (9223372036854776500);
SELECT count() FROM t_wrap_sorted WHERE icebergTruncate(1000, x) < 0;

DROP TABLE IF EXISTS t_wrap_nullable;
CREATE TABLE t_wrap_nullable (x Nullable(UInt64)) ENGINE = MergeTree ORDER BY x SETTINGS allow_nullable_key = 1, index_granularity = 1;
INSERT INTO t_wrap_nullable VALUES (9223372036854775900), (9223372036854776500), (NULL);
SELECT count() FROM t_wrap_nullable WHERE icebergTruncate(1000, x) < 0;

SET allow_suspicious_low_cardinality_types = 1;
DROP TABLE IF EXISTS t_wrap_lc;
CREATE TABLE t_wrap_lc (x LowCardinality(UInt64)) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1;
INSERT INTO t_wrap_lc VALUES (9223372036854775900), (9223372036854776500);
SELECT count() FROM t_wrap_lc WHERE icebergTruncate(1000, x) < 0;

DROP TABLE t_wrap;
DROP TABLE t_wrap_sorted;
DROP TABLE t_wrap_nullable;
DROP TABLE t_wrap_lc;
DROP TABLE t_dynamic;
