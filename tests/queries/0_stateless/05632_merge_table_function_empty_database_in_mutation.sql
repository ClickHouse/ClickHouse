-- Tags: zookeeper, distributed, no-replicated-database
-- Tag no-replicated-database: ON CLUSTER is not allowed

-- The names in a mutation are bound when it is submitted, as for `ON CLUSTER`: the database of `merge` (also an
-- empty one) is the database of the altered table, and any other `currentDatabase()` is the database of the session.
-- A database name computed while the mutation runs in the background means the database of the table.
-- The queries are issued from the database of the session, whose `t_src` has other rows.

SET distributed_ddl_output_mode = 'none';

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_src VALUES (1), (2), (3);
CREATE TABLE t_src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_src VALUES (9);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mut (id UInt64, s String) ENGINE = MergeTree ORDER BY id SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_mut SELECT number, '' FROM numbers(10);
CREATE TABLE t_alias ENGINE = Alias({CLICKHOUSE_DATABASE_1:String}, 't_mut');

ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mut DELETE WHERE id IN (SELECT id FROM merge('', '^t_src$') WHERE id = 1) SETTINGS mutations_sync = 2;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mut DELETE WHERE id = (SELECT max(id) - 1 FROM merge('', '^t_src$')) SETTINGS mutations_sync = 2, mutations_execute_subqueries_on_initiator = 1;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mut DELETE WHERE id IN (SELECT id FROM merge(concat('', ''), '^t_src$') WHERE id = 3) SETTINGS mutations_sync = 2;
ALTER TABLE t_alias DELETE WHERE id IN (SELECT id FROM merge('', '^t_src$')) SETTINGS mutations_sync = 2;
DELETE FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_mut WHERE id IN (SELECT id + 3 FROM merge('', '^t_src$') WHERE id = 1) SETTINGS lightweight_deletes_sync = 2, enable_lightweight_update = 1;
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.t_mut SET s = 'u' WHERE id IN (SELECT id + 7 FROM merge('', '^t_src$') WHERE id = 1) SETTINGS enable_lightweight_update = 1;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mut UPDATE s = currentDatabase() WHERE id = 5 SETTINGS mutations_sync = 2, alter_update_mode = 'heavy';
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mut UPDATE s = currentDatabase() WHERE id = 6 SETTINGS alter_update_mode = 'lightweight_force', enable_lightweight_update = 1;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mut ON CLUSTER test_shard_localhost UPDATE s = currentDatabase() WHERE id = 7 SETTINGS mutations_sync = 2, alter_update_mode = 'heavy';
-- The bound `currentDatabase()` keeps its alias, which hides the column `s`: nothing is deleted.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mut DELETE WHERE (currentDatabase() AS s) != s SETTINGS mutations_sync = 2;
SELECT id, if(s = currentDatabase(), 'session', s) FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_mut ORDER BY id;

-- The same for a partition and for `ReplicatedMergeTree`.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_part (p String, id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_part', 'r1') PARTITION BY p ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_part VALUES ({CLICKHOUSE_DATABASE:String}, 1), ({CLICKHOUSE_DATABASE_1:String}, 2), ({CLICKHOUSE_DATABASE_1:String}, 3);
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_part DELETE IN PARTITION tuple(currentDatabase()) WHERE 1 SETTINGS mutations_sync = 2;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_part DELETE WHERE id IN (SELECT id FROM merge(concat('', ''), '^t_src$') WHERE id = 2) SETTINGS mutations_sync = 2, allow_nondeterministic_mutations = 1;
SELECT p = currentDatabase(), id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_part ORDER BY id;

DROP TABLE t_alias;
DROP TABLE t_src;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
