-- Tags: zookeeper

-- A mutation is executed in a background context. Its current database is the database of the mutated table,
-- also when the mutation is checked or its subqueries and functions are executed on the initiator,
-- so an empty database name is not resolved to `default_database` or to the database of the session.
-- The queries are issued from another database, which has no `t_src`.

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mut (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_src VALUES (1), (2), (3), (4);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_mut SELECT number, '' FROM numbers(7);

ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mut DELETE WHERE id IN (SELECT id FROM merge('', '^t_src$') WHERE id = 1) SETTINGS mutations_sync = 2;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mut DELETE WHERE id IN (SELECT id FROM merge(concat('', ''), '^t_src$') WHERE id = 2) SETTINGS mutations_sync = 2;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mut DELETE WHERE id IN (SELECT id FROM remote('127.0.0.1', '', 't_src') WHERE id = 3) SETTINGS mutations_sync = 2;
DELETE FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_mut WHERE id IN (SELECT id FROM merge('', '^t_src$') WHERE id = 4) SETTINGS lightweight_deletes_sync = 2, lightweight_delete_mode = 'alter_update', alter_update_mode = 'heavy';
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mut DELETE WHERE id = (SELECT max(id) + 1 FROM merge('', '^t_src$')) SETTINGS mutations_sync = 2, mutations_execute_subqueries_on_initiator = 1;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mut UPDATE s = currentDatabase() WHERE id = 0 SETTINGS mutations_sync = 2, alter_update_mode = 'heavy';
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mut UPDATE s = currentDatabase() WHERE id = 6 SETTINGS mutations_sync = 2, alter_update_mode = 'heavy', mutations_execute_nondeterministic_on_initiator = 1;
SELECT id, s = {CLICKHOUSE_DATABASE_1:String} FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_mut ORDER BY id;

-- A replicated table, whose mutations are also pruned by partition.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_rmt (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_rmt', 'r1') PARTITION BY id ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_rmt SELECT number FROM numbers(6);
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_rmt DELETE WHERE id IN (SELECT id FROM merge('', '^t_src$')) SETTINGS mutations_sync = 2, allow_nondeterministic_mutations = 1;
SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_rmt ORDER BY id;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
