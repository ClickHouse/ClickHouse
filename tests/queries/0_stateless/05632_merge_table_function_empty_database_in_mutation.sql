-- An empty database name in the `merge` table function means the current database. A mutation is interpreted
-- in a background thread without one, so the database of the mutated table is substituted when the mutation
-- is created, as for `merge(regexp)`.

CREATE TABLE t_mut (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_mut VALUES (1), (2), (3), (4);
INSERT INTO t_src VALUES (2);

ALTER TABLE t_mut DELETE WHERE id IN (SELECT id FROM merge('', '^t_src$')) SETTINGS mutations_sync = 2;
SELECT id FROM t_mut ORDER BY id;
SELECT position(command, concat('merge(''', currentDatabase(), ''', ''^t_src$'')')) > 0
FROM system.mutations WHERE database = currentDatabase() AND table = 't_mut';

INSERT INTO t_src VALUES (3);
DELETE FROM t_mut WHERE id IN (SELECT id FROM merge('', '^t_src$')) SETTINGS lightweight_deletes_sync = 2;
SELECT id FROM t_mut ORDER BY id;

-- The database of the updated table is used, not the one of the session.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_upd (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_upd VALUES (1, 0), (2, 0), (3, 0);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_src VALUES (1);

UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.t_upd SET v = 1 WHERE id IN (SELECT id FROM merge('', '^t_src$'));
SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_upd WHERE v = 1;

-- A view binds the database at `CREATE`, as for `merge(regexp)`.
CREATE VIEW v AS SELECT id FROM merge('', '^t_src$');
SELECT position(create_table_query, concat('merge(''', currentDatabase(), ''', ''^t_src$'')')) > 0
FROM system.tables WHERE database = currentDatabase() AND name = 'v';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
