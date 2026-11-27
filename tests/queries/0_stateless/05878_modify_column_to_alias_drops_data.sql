-- A stored column turned into ALIAS or EPHEMERAL must not keep its data in the parts: a column of the same name
-- stored again later (ADD after DROP, REMOVE ALIAS, MODIFY to DEFAULT) must not read the old values.

SET mutations_sync = 2, alter_sync = 2;

DROP TABLE IF EXISTS t_compact;
DROP TABLE IF EXISTS t_wide;
DROP TABLE IF EXISTS t_ephemeral;
DROP TABLE IF EXISTS t_one_statement;
DROP TABLE IF EXISTS t_remove_alias;
DROP TABLE IF EXISTS t_to_default;
DROP TABLE IF EXISTS t_nested;
DROP TABLE IF EXISTS t_pending;
DROP TABLE IF EXISTS t_created_alias;
DROP TABLE IF EXISTS t_replicated;
DROP TABLE IF EXISTS t_indexed;
DROP TABLE IF EXISTS t_pending_update;
DROP TABLE IF EXISTS t_only_q;
DROP TABLE IF EXISTS t_only_q_drop_index;
DROP TABLE IF EXISTS t_only_q_add_index;
DROP TABLE IF EXISTS t_only_two;

CREATE TABLE t_compact (id UInt64, q UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_compact VALUES (1, 42);
SELECT 'compact', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_compact' AND active;
ALTER TABLE t_compact MODIFY COLUMN q UInt32 ALIAS toUInt32(7);
ALTER TABLE t_compact DROP COLUMN q;
ALTER TABLE t_compact ADD COLUMN q UInt32;
SELECT 'compact', * FROM t_compact;
OPTIMIZE TABLE t_compact FINAL;
SELECT 'compact after merge', * FROM t_compact;

CREATE TABLE t_wide (id UInt64, q UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_wide VALUES (1, 42);
SELECT 'wide', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_wide' AND active;
ALTER TABLE t_wide MODIFY COLUMN q UInt32 ALIAS toUInt32(7);
ALTER TABLE t_wide DROP COLUMN q;
ALTER TABLE t_wide ADD COLUMN q UInt32;
SELECT 'wide', * FROM t_wide;
OPTIMIZE TABLE t_wide FINAL;
SELECT 'wide after merge', * FROM t_wide;

CREATE TABLE t_ephemeral (id UInt64, q UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_ephemeral VALUES (1, 42);
ALTER TABLE t_ephemeral MODIFY COLUMN q UInt32 EPHEMERAL;
ALTER TABLE t_ephemeral DROP COLUMN q;
ALTER TABLE t_ephemeral ADD COLUMN q UInt32;
SELECT 'ephemeral', * FROM t_ephemeral;

CREATE TABLE t_one_statement (id UInt64, q UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_one_statement VALUES (1, 42);
ALTER TABLE t_one_statement MODIFY COLUMN q UInt32 ALIAS toUInt32(7), DROP COLUMN q, ADD COLUMN q UInt32;
SELECT 'one statement', * FROM t_one_statement;

CREATE TABLE t_remove_alias (id UInt64, q UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_remove_alias VALUES (1, 42);
ALTER TABLE t_remove_alias MODIFY COLUMN q UInt32 ALIAS toUInt32(7);
ALTER TABLE t_remove_alias MODIFY COLUMN q REMOVE ALIAS;
SELECT 'remove alias', * FROM t_remove_alias;

CREATE TABLE t_to_default (id UInt64, q UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_to_default VALUES (1, 42);
ALTER TABLE t_to_default MODIFY COLUMN q UInt32 ALIAS toUInt32(7);
ALTER TABLE t_to_default MODIFY COLUMN q UInt32 DEFAULT 5;
SELECT 'to default', * FROM t_to_default;

CREATE TABLE t_nested (id UInt64, n Nested(a UInt32, b UInt32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_nested VALUES (1, [11], [42]);
ALTER TABLE t_nested MODIFY COLUMN n.b Array(UInt32) ALIAS [toUInt32(7)];
ALTER TABLE t_nested DROP COLUMN n.b;
ALTER TABLE t_nested ADD COLUMN n.b Array(UInt32);
SELECT 'nested', * FROM t_nested;

-- While the mutation is not done, the parts' data of the column is not read either.
CREATE TABLE t_pending (id UInt64, q UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_pending VALUES (1, 42);
SYSTEM STOP MERGES t_pending;
ALTER TABLE t_pending MODIFY COLUMN q UInt32 ALIAS toUInt32(7) SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE t_pending MODIFY COLUMN q REMOVE ALIAS;
SELECT 'pending', * FROM t_pending;
SYSTEM START MERGES t_pending;
ALTER TABLE t_pending DELETE WHERE 0 SETTINGS mutations_sync = 2;
SELECT 'pending done', * FROM t_pending;
OPTIMIZE TABLE t_pending FINAL;
SELECT 'pending after merge', * FROM t_pending;

-- A column created as ALIAS has no data, so dropping it does not mutate the parts.
CREATE TABLE t_created_alias (id UInt64, q UInt32 ALIAS toUInt32(7)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_created_alias VALUES (1);
ALTER TABLE t_created_alias DROP COLUMN q;
SELECT 'created alias mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_created_alias';

CREATE TABLE t_replicated (id UInt64, q UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05878_t_replicated', 'r1') ORDER BY id;
INSERT INTO t_replicated VALUES (1, 42);
ALTER TABLE t_replicated MODIFY COLUMN q UInt32 ALIAS toUInt32(7);
ALTER TABLE t_replicated DROP COLUMN q;
ALTER TABLE t_replicated ADD COLUMN q UInt32;
SELECT 'replicated', * FROM t_replicated;

-- A column read by a skip index keeps its data.
CREATE TABLE t_indexed (id UInt64, q UInt32, INDEX i q TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_indexed VALUES (1, 42);
ALTER TABLE t_indexed MODIFY COLUMN q UInt32 ALIAS toUInt32(id + 100);
SELECT 'indexed mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_indexed';

-- A mutation queued before the conversion that reads the column still finishes.
CREATE TABLE t_pending_update (id UInt64, q UInt32, z UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_pending_update VALUES (1, 42, 0);
SYSTEM STOP MERGES t_pending_update;
ALTER TABLE t_pending_update UPDATE z = q WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_pending_update MODIFY COLUMN q UInt32 ALIAS toUInt32(7) SETTINGS alter_sync = 0, mutations_sync = 0;
SYSTEM START MERGES t_pending_update;
ALTER TABLE t_pending_update DELETE WHERE 0 SETTINGS mutations_sync = 2;
SELECT 'pending update', id, q, z FROM t_pending_update;
SELECT 'pending update mutations', countIf(NOT is_done), countIf(latest_fail_reason != '') FROM system.mutations
WHERE database = currentDatabase() AND table = 't_pending_update';

-- Like DROP COLUMN, the conversion is refused when it would leave a part without columns.
CREATE TABLE t_only_q (q UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_only_q VALUES (42);
ALTER TABLE t_only_q ADD COLUMN id UInt64;
ALTER TABLE t_only_q MODIFY COLUMN q UInt32 ALIAS toUInt32(7); -- { serverError BAD_ARGUMENTS }
SELECT 'only q', id, q FROM t_only_q;
SELECT 'only q mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_only_q';

-- The same when an earlier clause of the ALTER drops or adds the column's only dependent.
CREATE TABLE t_only_q_drop_index (q UInt32, INDEX i q TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_only_q_drop_index VALUES (42);
ALTER TABLE t_only_q_drop_index ADD COLUMN id UInt64;
ALTER TABLE t_only_q_drop_index DROP INDEX i, MODIFY COLUMN q UInt32 ALIAS toUInt32(id + 100); -- { serverError BAD_ARGUMENTS }
SELECT 'only q drop index', id, q FROM t_only_q_drop_index;

CREATE TABLE t_only_q_add_index (q UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_only_q_add_index VALUES (42);
ALTER TABLE t_only_q_add_index ADD COLUMN id UInt64;
ALTER TABLE t_only_q_add_index ADD INDEX i q TYPE minmax GRANULARITY 1, MODIFY COLUMN q UInt32 ALIAS toUInt32(id + 100);
SELECT 'only q add index', id, q FROM t_only_q_add_index;
SELECT 'only q index mutations', count() FROM system.mutations
WHERE database = currentDatabase() AND table IN ('t_only_q_drop_index', 't_only_q_add_index');

-- The same when the conversion of the part's other column is still pending.
CREATE TABLE t_only_two (q UInt32, r UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_only_two VALUES (42, 52);
ALTER TABLE t_only_two ADD COLUMN id UInt64;
SYSTEM STOP MERGES t_only_two;
ALTER TABLE t_only_two MODIFY COLUMN q UInt32 ALIAS toUInt32(id + 7) SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE t_only_two MODIFY COLUMN r UInt32 ALIAS toUInt32(id + 8) SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_only_two DROP COLUMN r SETTINGS alter_sync = 0; -- { serverError BAD_ARGUMENTS }
SYSTEM START MERGES t_only_two;
ALTER TABLE t_only_two DELETE WHERE 0 SETTINGS mutations_sync = 2;
SELECT 'only two', id, q, r FROM t_only_two;
SELECT 'only two mutations', countIf(NOT is_done), countIf(latest_fail_reason != '') FROM system.mutations
WHERE database = currentDatabase() AND table = 't_only_two';

DROP TABLE t_compact;
DROP TABLE t_wide;
DROP TABLE t_ephemeral;
DROP TABLE t_one_statement;
DROP TABLE t_remove_alias;
DROP TABLE t_to_default;
DROP TABLE t_nested;
DROP TABLE t_pending;
DROP TABLE t_created_alias;
DROP TABLE t_replicated;
DROP TABLE t_indexed;
DROP TABLE t_pending_update;
DROP TABLE t_only_q;
DROP TABLE t_only_q_drop_index;
DROP TABLE t_only_q_add_index;
DROP TABLE t_only_two;
