-- Tags: zookeeper, no-shared-merge-tree
-- no-shared-merge-tree: the table engine is replaced.

-- Lightweight updates and deletes modify existing rows, so they are allowed when the database is at its `max_rows` limit.

SET enable_lightweight_update = 1;
SET insert_keeper_fault_injection_probability = 0;
SET async_insert = 0;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS max_rows = 2;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.mt (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.rmt (id UInt64, v UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/rmt', 'r1') ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.mt VALUES (1, 0);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.rmt VALUES (1, 0);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.mt VALUES (2, 0); -- { serverError TOO_MANY_ROWS }

UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.mt SET v = 1 WHERE id = 1;
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.rmt SET v = 1 WHERE id = 1;
SELECT 'mt', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.mt;
SELECT 'rmt', v FROM {CLICKHOUSE_DATABASE_1:Identifier}.rmt;

DELETE FROM {CLICKHOUSE_DATABASE_1:Identifier}.mt WHERE id = 1 SETTINGS lightweight_delete_mode = 'lightweight_update_force';
DELETE FROM {CLICKHOUSE_DATABASE_1:Identifier}.rmt WHERE id = 1 SETTINGS lightweight_delete_mode = 'lightweight_update_force';
SELECT 'mt', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.mt;
SELECT 'rmt', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.rmt;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
