-- Tags: zookeeper, distributed, no-replicated-database
-- Tag no-replicated-database: ON CLUSTER is not allowed

-- In a mutation the current database is the database of the altered table, as for table names: for `merge` (also
-- with an empty or a computed database), `currentDatabase()` with or without parentheses and an `ALIAS` column using it.
-- The queries are issued from the database of the session, whose `t_src` has other rows.

SET distributed_ddl_output_mode = 'none';

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_src (id UInt64, s String DEFAULT 'x') ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_src (id) VALUES (1), (2), (3);
CREATE TABLE t_src (id UInt64, s String DEFAULT 'x') ENGINE = MergeTree ORDER BY id;
INSERT INTO t_src (id) VALUES (9);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mut (id UInt64, s String, db String ALIAS currentDatabase()) ENGINE = MergeTree ORDER BY id SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_mut SELECT number, '' FROM numbers(12);
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
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mut UPDATE s = currentDatabase WHERE id = 10 SETTINGS mutations_sync = 2, alter_update_mode = 'heavy';
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.t_mut SET s = currentDatabase() WHERE id = 11 SETTINGS enable_lightweight_update = 1;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mut DELETE WHERE db != currentDatabase() SETTINGS mutations_sync = 2;
ALTER TABLE t_alias DELETE WHERE db != currentDatabase() SETTINGS mutations_sync = 2;
-- An alias of `currentDatabase()` (also in `merge`) or of an empty database of `merge` keeps its value, which hides the column `s`: nothing is deleted.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mut DELETE WHERE (currentDatabase() AS s) != s SETTINGS mutations_sync = 2;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mut DELETE WHERE id IN (SELECT id + 5 FROM merge((currentDatabase() AS s), '^t_src$') WHERE s != currentDatabase() AND id = 1) SETTINGS mutations_sync = 2;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_mut DELETE WHERE id IN (SELECT id + 5 FROM merge(('' AS s), '^t_src$') WHERE s != '' AND id = 1) SETTINGS mutations_sync = 2;
SELECT id, multiIf(s = currentDatabase(), 'session', s = {CLICKHOUSE_DATABASE_1:String}, 'table', s) FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_mut ORDER BY id;
CREATE VIEW v_merge AS SELECT id FROM merge(('' AS s), '^t_src$') WHERE s != '';
SELECT count() FROM v_merge;

-- The same for a partition and for `ReplicatedMergeTree`.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_part (p String, k UInt8, id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_part', 'r1') PARTITION BY (p, k) ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_part VALUES ({CLICKHOUSE_DATABASE:String}, 0, 1), ({CLICKHOUSE_DATABASE:String}, 1, 2), ({CLICKHOUSE_DATABASE_1:String}, 0, 3), ({CLICKHOUSE_DATABASE_1:String}, 1, 4), ({CLICKHOUSE_DATABASE_1:String}, 2, 5), ({CLICKHOUSE_DATABASE_1:String}, 3, 6), ('x', 4, 7), ('x', 5, 8), ({CLICKHOUSE_DATABASE_1:String}, 6, 9);
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_part DELETE IN PARTITION (currentDatabase(), 0) WHERE 1 SETTINGS mutations_sync = 2;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_part DELETE IN PARTITION (currentDatabase, 1) WHERE 1 SETTINGS mutations_sync = 2;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_part DROP PARTITION (currentDatabase(), 2);
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_part DROP PARTITION (currentDatabase, 3);
-- An alias or a lambda parameter with the name of the function is not the function.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_part DROP PARTITION (('x' AS database), if(database = 'x', 4, 5));
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_part DELETE IN PARTITION (arrayMap(database -> database, ['x'])[1], 5) WHERE 1 SETTINGS mutations_sync = 2;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_part DROP PARTITION (arrayMap(x -> (x AS database), [database])[1], 6);
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_part DELETE WHERE id IN (SELECT id FROM merge(concat('', ''), '^t_src$') WHERE id = 2) SETTINGS mutations_sync = 2, allow_nondeterministic_mutations = 1;
SELECT p = currentDatabase(), k, id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_part ORDER BY id;

-- Several partitions, read before the mutation runs, and mutations that wait while the table is moved to another database.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_ren (p String, id UInt64, db String ALIAS currentDatabase()) ENGINE = MergeTree PARTITION BY p ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_ren VALUES ({CLICKHOUSE_DATABASE_1:String}, 1), ({CLICKHOUSE_DATABASE_1:String}, 2), ('x', 3);
SYSTEM STOP MERGES {CLICKHOUSE_DATABASE_1:Identifier}.t_ren;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_ren DELETE IN PARTITION tuple(currentDatabase), tuple(('x' AS currentDatabase)) WHERE id = 1 SETTINGS mutations_sync = 0;
SELECT groupArray(id) FROM (SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_ren ORDER BY id) SETTINGS apply_mutations_on_fly = 1;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_ren DELETE WHERE db != currentDatabase() SETTINGS mutations_sync = 0;
RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_ren TO t_ren;
SYSTEM START MERGES t_ren;
ALTER TABLE t_ren DELETE WHERE 0 SETTINGS mutations_sync = 2;
SELECT groupArray(id) FROM (SELECT id FROM t_ren ORDER BY id);

DROP VIEW v_merge;
DROP TABLE t_ren;
DROP TABLE t_alias;
DROP TABLE t_src;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
