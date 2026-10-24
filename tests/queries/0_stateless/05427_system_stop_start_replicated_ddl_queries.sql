-- Tags: zookeeper, no-parallel
-- no-parallel: SYSTEM STOP REPLICATED DDL QUERIES affects all Replicated databases on the server

SET distributed_ddl_output_mode = 'none';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/test/databases/{database}/test_05427', 's1', 'r1');

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t1 (x UInt64) ENGINE = MergeTree ORDER BY x;

SYSTEM STOP REPLICATED DDL QUERIES;

-- DDL queries in Replicated databases are prohibited while replicated DDL queries are stopped.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t2 (x UInt64) ENGINE = MergeTree ORDER BY x; -- { serverError QUERY_IS_PROHIBITED }
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t1 ADD COLUMN y UInt64; -- { serverError QUERY_IS_PROHIBITED }

-- Non-DDL queries still work.
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t1 VALUES (1);

SYSTEM START REPLICATED DDL QUERIES;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t2 (x UInt64) ENGINE = MergeTree ORDER BY x;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t1 ADD COLUMN y UInt64;

SHOW TABLES FROM {CLICKHOUSE_DATABASE_1:Identifier};
SELECT * FROM {CLICKHOUSE_DATABASE_1:Identifier}.t1;

-- Starting twice is fine.
SYSTEM START REPLICATED DDL QUERIES;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;

-- These action locks are not used by ReplicatedMergeTree, so the statements are no-ops for it.
DROP TABLE IF EXISTS rmt SYNC;
CREATE TABLE rmt (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/rmt_05427', 'r1') ORDER BY x;

SYSTEM STOP VIRTUAL PARTS UPDATE rmt;
SYSTEM STOP REDUCE BLOCKING PARTS rmt;
INSERT INTO rmt VALUES (1), (2);
SYSTEM START VIRTUAL PARTS UPDATE rmt;
SYSTEM START REDUCE BLOCKING PARTS rmt;

SYSTEM STOP VIRTUAL PARTS UPDATE;
SYSTEM STOP REDUCE BLOCKING PARTS;
SYSTEM START VIRTUAL PARTS UPDATE;
SYSTEM START REDUCE BLOCKING PARTS;

SELECT count() FROM rmt;

DROP TABLE rmt SYNC;
