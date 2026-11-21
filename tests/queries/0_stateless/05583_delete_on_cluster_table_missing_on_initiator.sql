-- Tags: distributed, no-replicated-database
-- Tag no-replicated-database: ON CLUSTER is not allowed

-- A lightweight `DELETE ... ON CLUSTER` is shipped to the hosts as itself rather than as a rewritten
-- `ALTER`, so the initiator does not have to have the table: `TRUNCATE TABLE ... ON CLUSTER` and
-- `ALTER TABLE ... ON CLUSTER` are enqueued without a local table as well, and the hosts that do have
-- the table run the storage validations against their own settings and metadata.
--
-- The cluster of a table need not contain the host the statement is issued from, so the initiator can
-- see only the database and not the table. Before the fix the initiator resolved the table itself and
-- failed with `UNKNOWN_TABLE`, so nothing reached the distributed DDL queue at all and the hosts that
-- had the table never ran the delete.

-- The initiator does not have the table, so the hosts report an error for the missing table. That is
-- the point: the statement reached them at all instead of being rejected on the initiator.
SET distributed_ddl_output_mode = 'never_throw';

CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

-- The statement is enqueued like the `TRUNCATE` below it, instead of failing on the initiator.
DELETE FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_missing_05583 ON CLUSTER test_shard_localhost WHERE 1 = 1;

TRUNCATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_missing_05583 ON CLUSTER test_shard_localhost;

-- Both statements reach the queue, so the `DELETE` is no longer rejected on the initiator. The scan is
-- restricted to the entries of this test: the suite runs in parallel and shares the queue.
SELECT
    countIf(query LIKE 'DELETE FROM%t_missing_05583%') > 0 AS delete_enqueued,
    countIf(query LIKE 'TRUNCATE%t_missing_05583%') > 0 AS truncate_enqueued
FROM system.distributed_ddl_queue
WHERE query LIKE '%05583%';

-- The clustered path with a table the initiator does have is unchanged, so it is not asserted here: a
-- clustered delete is enqueued and runs asynchronously, which the suite cannot observe deterministically.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_local_05583 (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_local_05583 VALUES (1), (2), (3);

-- A delete without ON CLUSTER is unchanged: it resolves the table on this host and runs synchronously.
DELETE FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_local_05583 WHERE id = 2;
SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_local_05583 ORDER BY id;

DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_local_05583 SYNC;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
