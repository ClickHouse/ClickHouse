-- Tags: zookeeper
-- A flush of coalesced async inserts splits the block by partition. `filterToPartition` walks the same
-- selector to keep each insert's token only in the partitions that insert wrote to. The partition ids are
-- `UInt16` when `max_partitions_per_insert_block` bounds the partition count (the default is 100), and
-- `UInt64` when it is 0. With either type, the second insert of a batch repeats the first and must be
-- dropped, and the other two must be kept.

DROP TABLE IF EXISTS async_mt_limit;
DROP TABLE IF EXISTS async_mt_no_limit;
DROP TABLE IF EXISTS async_rmt_limit SYNC;
DROP TABLE IF EXISTS async_rmt_no_limit SYNC;

CREATE TABLE async_mt_limit (key Int64, value String)
ENGINE = MergeTree PARTITION BY key % 4 ORDER BY tuple()
SETTINGS non_replicated_deduplication_window = 100;

CREATE TABLE async_mt_no_limit (key Int64, value String)
ENGINE = MergeTree PARTITION BY key % 4 ORDER BY tuple()
SETTINGS non_replicated_deduplication_window = 100;

CREATE TABLE async_rmt_limit (key Int64, value String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/async_rmt_limit', '1') PARTITION BY key % 4 ORDER BY tuple();

CREATE TABLE async_rmt_no_limit (key Int64, value String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/async_rmt_no_limit', '1') PARTITION BY key % 4 ORDER BY tuple();

SET async_insert = 1, wait_for_async_insert = 0, async_insert_deduplicate = 1;
-- Nothing is flushed before `SYSTEM FLUSH ASYNC INSERT QUEUE`, so the three inserts of a table form one batch.
SET async_insert_use_adaptive_busy_timeout = 0, async_insert_busy_timeout_max_ms = 1000000;

INSERT INTO async_mt_limit VALUES (0, 'a'), (1, 'b'), (2, 'c'), (3, 'd');
INSERT INTO async_mt_limit VALUES (0, 'a'), (1, 'b'), (2, 'c'), (3, 'd');
INSERT INTO async_mt_limit VALUES (4, 'e'), (5, 'f'), (6, 'g'), (7, 'h');
SYSTEM FLUSH ASYNC INSERT QUEUE async_mt_limit;

INSERT INTO async_rmt_limit VALUES (0, 'a'), (1, 'b'), (2, 'c'), (3, 'd');
INSERT INTO async_rmt_limit VALUES (0, 'a'), (1, 'b'), (2, 'c'), (3, 'd');
INSERT INTO async_rmt_limit VALUES (4, 'e'), (5, 'f'), (6, 'g'), (7, 'h');
SYSTEM FLUSH ASYNC INSERT QUEUE async_rmt_limit;

SET max_partitions_per_insert_block = 0;

INSERT INTO async_mt_no_limit VALUES (0, 'a'), (1, 'b'), (2, 'c'), (3, 'd');
INSERT INTO async_mt_no_limit VALUES (0, 'a'), (1, 'b'), (2, 'c'), (3, 'd');
INSERT INTO async_mt_no_limit VALUES (4, 'e'), (5, 'f'), (6, 'g'), (7, 'h');
SYSTEM FLUSH ASYNC INSERT QUEUE async_mt_no_limit;

INSERT INTO async_rmt_no_limit VALUES (0, 'a'), (1, 'b'), (2, 'c'), (3, 'd');
INSERT INTO async_rmt_no_limit VALUES (0, 'a'), (1, 'b'), (2, 'c'), (3, 'd');
INSERT INTO async_rmt_no_limit VALUES (4, 'e'), (5, 'f'), (6, 'g'), (7, 'h');
SYSTEM FLUSH ASYNC INSERT QUEUE async_rmt_no_limit;

SELECT 'MergeTree, UInt16 ids', count(), groupArray((key, value)) FROM (SELECT key, value FROM async_mt_limit ORDER BY key, value);
SELECT 'MergeTree, UInt64 ids', count(), groupArray((key, value)) FROM (SELECT key, value FROM async_mt_no_limit ORDER BY key, value);
SELECT 'ReplicatedMergeTree, UInt16 ids', count(), groupArray((key, value)) FROM (SELECT key, value FROM async_rmt_limit ORDER BY key, value);
SELECT 'ReplicatedMergeTree, UInt64 ids', count(), groupArray((key, value)) FROM (SELECT key, value FROM async_rmt_no_limit ORDER BY key, value);

DROP TABLE async_mt_limit;
DROP TABLE async_mt_no_limit;
DROP TABLE async_rmt_limit SYNC;
DROP TABLE async_rmt_no_limit SYNC;
