-- Tags: zookeeper

-- Two INSERTs that write the same values into different columns, or in a different column order,
-- are different inserts and must not be deduplicated against each other.
-- https://github.com/ClickHouse/ClickHouse/issues/127388

SET deduplicate_insert = 'enable';
SET async_insert = 0;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4 SYNC;
DROP TABLE IF EXISTS mv5;
DROP TABLE IF EXISTS src5;
DROP TABLE IF EXISTS dst5;
DROP TABLE IF EXISTS rmv6;
DROP TABLE IF EXISTS t6;
DROP TABLE IF EXISTS t7;
DROP TABLE IF EXISTS t8;
DROP TABLE IF EXISTS t9;

SELECT 'sync';
CREATE TABLE t1 (a UInt32, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO t1 (a, b) VALUES (1, 2);
INSERT INTO t1 (a, c) VALUES (1, 2);
INSERT INTO t1 (b, a) VALUES (1, 2);
SELECT count() FROM t1;

SELECT 'async';
CREATE TABLE t2 (a UInt32, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO t2 (a, b) SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES (1, 2);
INSERT INTO t2 (a, c) SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES (1, 2);
INSERT INTO t2 (b, a) SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES (1, 2);
SELECT count() FROM t2;

SELECT 'insert select';
CREATE TABLE t3 (a UInt32, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO t3 (a, b) SELECT 1, 2 ORDER BY ALL;
INSERT INTO t3 (a, c) SELECT 1, 2 ORDER BY ALL;
SELECT count() FROM t3;

SELECT 'replicated';
CREATE TABLE t4 (a UInt32, b UInt32, c UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t4', 'r1') ORDER BY a SETTINGS replicated_deduplication_window = 100;
INSERT INTO t4 (a, b) VALUES (1, 2);
INSERT INTO t4 (a, c) VALUES (1, 2);
SELECT count() FROM t4;

SELECT 'materialized view';
CREATE TABLE src5 (a UInt32, b UInt32, c UInt32) ENGINE = Null;
CREATE TABLE dst5 (a UInt32, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a SETTINGS non_replicated_deduplication_window = 100;
CREATE MATERIALIZED VIEW mv5 TO dst5 AS SELECT a, b, c FROM src5;
SET deduplicate_blocks_in_dependent_materialized_views = 1;
INSERT INTO src5 (a, b) VALUES (1, 2);
INSERT INTO src5 (a, c) VALUES (1, 2);
SELECT count() FROM dst5;

SELECT 'refreshable materialized view';
CREATE TABLE t6 (a UInt32, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO t6 (a, b) VALUES (1, 2);
CREATE MATERIALIZED VIEW rmv6 REFRESH EVERY 1 YEAR APPEND TO t6 EMPTY AS SELECT 1 AS a, 2 AS c ORDER BY ALL;
SYSTEM REFRESH VIEW rmv6;
SYSTEM WAIT VIEW rmv6;
SELECT count() FROM t6;

-- A retry of the same INSERT is still deduplicated.
SELECT 'retry';
CREATE TABLE t7 (a UInt32, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO t7 (a, c) VALUES (1, 2);
INSERT INTO t7 (a, c) VALUES (1, 2);
SELECT count() FROM t7;

-- An explicit list equal to the default one is the same INSERT as no list.
SELECT 'default list';
CREATE TABLE t8 (a UInt32, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO t8 VALUES (1, 2, 3);
INSERT INTO t8 (a, b, c) VALUES (1, 2, 3);
SELECT count() FROM t8;

-- Sync and async inserts of the same data still share the block id.
SELECT 'sync and async';
CREATE TABLE t9 (a UInt32, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO t9 (a, c) SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES (5, 6);
INSERT INTO t9 (a, c) SETTINGS async_insert = 0 VALUES (5, 6);
SELECT count() FROM t9;

DROP TABLE rmv6;
DROP TABLE mv5;
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4 SYNC;
DROP TABLE src5;
DROP TABLE dst5;
DROP TABLE t6;
DROP TABLE t7;
DROP TABLE t8;
DROP TABLE t9;
