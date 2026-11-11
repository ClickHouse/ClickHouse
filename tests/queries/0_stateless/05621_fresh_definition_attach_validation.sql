-- Tags: no-parallel, no-ordinary-database
-- Tag no-parallel: static UUIDs
-- Tag no-ordinary-database: a full-definition ATTACH TABLE requires a UUID

-- An ATTACH that states the whole definition is validated like the equivalent CREATE.

SET allow_suspicious_codecs = 0;
SET allow_suspicious_low_cardinality_types = 0;
SET allow_suspicious_indices = 0;
SET allow_minmax_index_for_json = 0;
SET allow_deprecated_syntax_for_merge_tree = 0;
SET allow_deprecated_database_ordinary = 0;
SET enable_unique_key = 1;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS src;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

-- UNIQUE KEY with a table TTL
CREATE TABLE t (id UInt64, ts DateTime) ENGINE = MergeTree UNIQUE KEY (id) ORDER BY id TTL ts + INTERVAL 1 DAY; -- { serverError SUPPORT_IS_DISABLED }
ATTACH TABLE t UUID '05621000-0000-0000-0000-000000000001' (id UInt64, ts DateTime) ENGINE = MergeTree UNIQUE KEY (id) ORDER BY id TTL ts + INTERVAL 1 DAY; -- { serverError SUPPORT_IS_DISABLED }

-- UNIQUE KEY with a column TTL
CREATE TABLE t (id UInt64, ts DateTime, v String TTL ts + INTERVAL 1 DAY) ENGINE = MergeTree UNIQUE KEY (id) ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }
ATTACH TABLE t UUID '05621000-0000-0000-0000-000000000002' (id UInt64, ts DateTime, v String TTL ts + INTERVAL 1 DAY) ENGINE = MergeTree UNIQUE KEY (id) ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }

-- UNIQUE KEY with a projection
CREATE TABLE t (id UInt64, v UInt32, PROJECTION p (SELECT id, v)) ENGINE = MergeTree UNIQUE KEY (id) ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }
ATTACH TABLE t UUID '05621000-0000-0000-0000-000000000003' (id UInt64, v UInt32, PROJECTION p (SELECT id, v)) ENGINE = MergeTree UNIQUE KEY (id) ORDER BY id; -- { serverError SUPPORT_IS_DISABLED }

-- Suspicious codec
CREATE TABLE t (x UInt8 CODEC(Delta, Delta)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
ATTACH TABLE t UUID '05621000-0000-0000-0000-000000000004' (x UInt8 CODEC(Delta, Delta)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- Suspicious LowCardinality type
CREATE TABLE t (x LowCardinality(UInt8)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY }
ATTACH TABLE t UUID '05621000-0000-0000-0000-000000000005' (x LowCardinality(UInt8)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY }

-- Nullable sorting key
CREATE TABLE t (x Nullable(UInt8)) ENGINE = MergeTree ORDER BY x SETTINGS allow_nullable_key = 0; -- { serverError ILLEGAL_COLUMN }
ATTACH TABLE t UUID '05621000-0000-0000-0000-000000000006' (x Nullable(UInt8)) ENGINE = MergeTree ORDER BY x SETTINGS allow_nullable_key = 0; -- { serverError ILLEGAL_COLUMN }

-- Duplicate expression in the sorting key
CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY (x, x); -- { serverError BAD_ARGUMENTS }
ATTACH TABLE t UUID '05621000-0000-0000-0000-000000000007' (x UInt8) ENGINE = MergeTree ORDER BY (x, x); -- { serverError BAD_ARGUMENTS }

-- minmax index on JSON
CREATE TABLE t (j JSON, INDEX i j TYPE minmax) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
ATTACH TABLE t UUID '05621000-0000-0000-0000-000000000008' (j JSON, INDEX i j TYPE minmax) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- Sampling expression not in the primary key
CREATE TABLE t (x UInt8, y UInt32) ENGINE = MergeTree ORDER BY x SAMPLE BY y; -- { serverError BAD_ARGUMENTS }
ATTACH TABLE t UUID '05621000-0000-0000-0000-000000000009' (x UInt8, y UInt32) ENGINE = MergeTree ORDER BY x SAMPLE BY y; -- { serverError BAD_ARGUMENTS }

-- bloom_filter false positive rate out of range
CREATE TABLE t (x UInt8, INDEX i x TYPE bloom_filter(2.0)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
ATTACH TABLE t UUID '05621000-0000-0000-0000-000000000010' (x UInt8, INDEX i x TYPE bloom_filter(2.0)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- Floating-point partition key
CREATE TABLE t (x Float64) ENGINE = MergeTree PARTITION BY x ORDER BY tuple() SETTINGS allow_floating_point_partition_key = 0; -- { serverError BAD_ARGUMENTS }
ATTACH TABLE t UUID '05621000-0000-0000-0000-000000000011' (x Float64) ENGINE = MergeTree PARTITION BY x ORDER BY tuple() SETTINGS allow_floating_point_partition_key = 0; -- { serverError BAD_ARGUMENTS }

-- Deprecated MergeTree syntax
CREATE TABLE t (d Date, x UInt8) ENGINE = MergeTree(d, (d, x), 8192); -- { serverError BAD_ARGUMENTS }
ATTACH TABLE t UUID '05621000-0000-0000-0000-000000000012' (d Date, x UInt8) ENGINE = MergeTree(d, (d, x), 8192); -- { serverError BAD_ARGUMENTS }

-- Distributed table looking at itself
CREATE TABLE t (x UInt8) ENGINE = Distributed(test_shard_localhost, currentDatabase(), t); -- { serverError INFINITE_LOOP }
ATTACH TABLE t UUID '05621000-0000-0000-0000-000000000013' (x UInt8) ENGINE = Distributed(test_shard_localhost, currentDatabase(), t); -- { serverError INFINITE_LOOP }

-- Alias table with explicit columns
CREATE TABLE src (x UInt8) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t (x UInt8) ENGINE = Alias('src'); -- { serverError BAD_ARGUMENTS }
ATTACH TABLE t UUID '05621000-0000-0000-0000-000000000014' (x UInt8) ENGINE = Alias('src'); -- { serverError BAD_ARGUMENTS }

-- FileLog path outside of user_files
CREATE TABLE t (a String) ENGINE = FileLog('/tmp/05621_outside.csv', 'CSV'); -- { serverError BAD_ARGUMENTS }
ATTACH TABLE t UUID '05621000-0000-0000-0000-000000000015' (a String) ENGINE = FileLog('/tmp/05621_outside.csv', 'CSV'); -- { serverError BAD_ARGUMENTS }

-- Deprecated Ordinary database engine
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary; -- { serverError UNKNOWN_DATABASE_ENGINE }
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary; -- { serverError UNKNOWN_DATABASE_ENGINE }

-- A short ATTACH of a valid table still works
DETACH TABLE src;
ATTACH TABLE src;
SELECT count() FROM src;

DROP TABLE src;
