-- Tags: shard
-- Random settings limits: optimize_read_in_order=(1, 1)

-- Over the native protocol a sparse `Nullable(String)` is sent as the values from index 1, and the NULL
-- default row at index 0 may keep a non-empty nested string ('0' below); the offsets must not include it.

DROP TABLE IF EXISTS t_sparse_int;
CREATE TABLE t_sparse_int (k UInt32, i Int64) ENGINE = MergeTree ORDER BY k
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;
INSERT INTO t_sparse_int SELECT number, if(number % 50 = 49, number * 37, 0) FROM numbers(100);
SELECT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_int' AND column = 'i' AND active;
SELECT k, toString(nullIf(i, 0)) FROM t_sparse_int WHERE k IN (0, 49, 99) OR k < 2 ORDER BY k;

DROP TABLE IF EXISTS t_sparse_nullable;
CREATE TABLE t_sparse_nullable (k UInt32, c Nullable(Float64)) ENGINE = MergeTree ORDER BY k
SETTINGS nullable_serialization_version = 'allow_sparse', serialization_info_version = 'with_types',
    ratio_of_defaults_for_sparse_serialization = 0.0;
INSERT INTO t_sparse_nullable VALUES (0, NULL);
INSERT INTO t_sparse_nullable VALUES (1, -0.), (2, nan), (3, nan), (4, NULL);
OPTIMIZE TABLE t_sparse_nullable FINAL;
SELECT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_nullable' AND column = 'c' AND active;
SELECT k, toString(c) FROM t_sparse_nullable ORDER BY k;

-- The same column read from shards over the protocol and stored.
DROP TABLE IF EXISTS t_copy;
CREATE TABLE t_copy (k UInt32, s Nullable(String)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_copy SELECT k, toString(c) FROM remote('127.0.0.{1,2}', currentDatabase(), t_sparse_nullable)
SETTINGS prefer_localhost_replica = 0;
SELECT k, s, length(s) FROM t_copy ORDER BY k, s;

DROP TABLE t_copy;
DROP TABLE t_sparse_nullable;
DROP TABLE t_sparse_int;
