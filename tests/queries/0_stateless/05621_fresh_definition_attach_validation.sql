-- Tags: no-parallel, no-ordinary-database
-- no-parallel: full-definition ATTACH uses fixed UUIDs, which collide across concurrent runs of this test.
-- no-ordinary-database: an Ordinary database rejects the UUID clause of ATTACH.

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
DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS ts_ext;
DROP TABLE IF EXISTS ts_base;
DROP TABLE IF EXISTS ext_samples;
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

-- TimeSeries inner column of a wrong type, inner engine not allowed for its target
SET enable_time_series_table = 1;
CREATE TABLE t ENGINE = TimeSeries SAMPLES INNER COLUMNS(min_time DateTime64(3)); -- { serverError BAD_TYPE_OF_FIELD }
CREATE TABLE t ENGINE = TimeSeries RECENT SAMPLES INNER ENGINE = Memory; -- { serverError INCORRECT_QUERY }
CREATE MATERIALIZED VIEW mv (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 0 AS SELECT x FROM src; -- { serverError BAD_ARGUMENTS }

-- A full-definition ATTACH of a view with inner tables is refused, valid or not, with or without an inner engine
ATTACH TABLE t UUID '05621000-0000-0000-0000-000000000016' ENGINE = TimeSeries SAMPLES INNER COLUMNS(min_time DateTime64(3)); -- { serverError BAD_ARGUMENTS }
ATTACH TABLE t UUID '05621000-0000-0000-0000-000000000017' ENGINE = TimeSeries RECENT SAMPLES INNER ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
ATTACH TABLE t UUID '05621000-0000-0000-0000-000000000018' ENGINE = TimeSeries; -- { serverError BAD_ARGUMENTS }
ATTACH MATERIALIZED VIEW mv UUID '05621000-0000-0000-0000-000000000019' (x UInt8) ENGINE = MergeTree ORDER BY x AS SELECT x FROM src; -- { serverError BAD_ARGUMENTS }
ATTACH MATERIALIZED VIEW mv UUID '05621000-0000-0000-0000-000000000020' (x UInt8) AS SELECT x FROM src; -- { serverError BAD_ARGUMENTS }

-- A full-definition ATTACH of a view with an external target is allowed
CREATE TABLE dst (x UInt8) ENGINE = MergeTree ORDER BY x;
ATTACH MATERIALIZED VIEW mv UUID '05621000-0000-0000-0000-000000000021' TO dst (x UInt8) AS SELECT x FROM src;
INSERT INTO src VALUES (1);
SELECT count() FROM dst;
DROP TABLE mv;
DROP TABLE dst;
SET database_replicated_allow_explicit_uuid = 3;
CREATE TABLE ts_base UUID '05621000-0000-0000-0000-000000000022' ENGINE = TimeSeries SETTINGS version = 9, recent_samples_ttl_seconds = 0, store_time_ranges = 0;
ATTACH TABLE ts_ext UUID '05621000-0000-0000-0000-000000000023' ENGINE = TimeSeries SETTINGS version = 9, recent_samples_ttl_seconds = 0, store_time_ranges = 0
    SAMPLES `.inner_id.samples.05621000-0000-0000-0000-000000000022`
    TAGS `.inner_id.tags.05621000-0000-0000-0000-000000000022`
    METRIC FAMILIES `.inner_id.metricfamilies.05621000-0000-0000-0000-000000000022`;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'ts_ext';
DROP TABLE ts_ext;
-- A repeated setting takes its last value, which enables the inner recent samples table here
ATTACH TABLE ts_ext UUID '05621000-0000-0000-0000-000000000025' ENGINE = TimeSeries SETTINGS version = 9, recent_samples_ttl_seconds = 0, recent_samples_ttl_seconds = 60, store_time_ranges = 0
    SAMPLES `.inner_id.samples.05621000-0000-0000-0000-000000000022`
    TAGS `.inner_id.tags.05621000-0000-0000-0000-000000000022`
    METRIC FAMILIES `.inner_id.metricfamilies.05621000-0000-0000-0000-000000000022`; -- { serverError BAD_ARGUMENTS }
-- The columns of a TimeSeries table attached with a full definition follow its external targets, like on CREATE
CREATE TABLE ext_samples (id Tuple(UInt64, LowCardinality(UUID)), timestamp DateTime64(6), value Float32) ENGINE = MergeTree ORDER BY (id, timestamp);
ATTACH TABLE ts_ext UUID '05621000-0000-0000-0000-000000000024' (metric_name String) ENGINE = TimeSeries SETTINGS version = 9, recent_samples_ttl_seconds = 0, store_time_ranges = 0
    SAMPLES ext_samples
    TAGS `.inner_id.tags.05621000-0000-0000-0000-000000000022`
    METRIC FAMILIES `.inner_id.metricfamilies.05621000-0000-0000-0000-000000000022`;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'ts_ext' AND name = 'samples';
DETACH TABLE ts_ext PERMANENTLY;
ATTACH TABLE ts_ext;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'ts_ext' AND name = 'samples';
DROP TABLE ts_ext;
DROP TABLE ext_samples;
DROP TABLE ts_base;

-- A short ATTACH of a valid table still works
DETACH TABLE src PERMANENTLY;
ATTACH TABLE src;
SELECT count() FROM src;

DROP TABLE src;
