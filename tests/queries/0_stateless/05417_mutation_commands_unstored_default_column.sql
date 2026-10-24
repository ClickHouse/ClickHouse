-- A mutation with several commands on a column added by ADD COLUMN ... DEFAULT, which the part does not store,
-- gives the same result as running the commands one by one.

SET mutations_sync = 2;

-- MATERIALIZE COLUMN keeps an earlier UPDATE of the column (compact part).
DROP TABLE IF EXISTS t_compact;
CREATE TABLE t_compact (id Int64, x Int64) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000;
INSERT INTO t_compact SELECT number, number FROM numbers(4);
ALTER TABLE t_compact ADD COLUMN a Int64 DEFAULT 7;
ALTER TABLE t_compact UPDATE a = -1 WHERE id IN (1, 2), MATERIALIZE COLUMN a;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_compact' AND active;
SELECT 'compact', id, a FROM t_compact ORDER BY id;
DROP TABLE t_compact;

-- Same with an UPDATE in between of a column the DEFAULT does not read (wide part).
DROP TABLE IF EXISTS t_wide;
CREATE TABLE t_wide (id Int64, x Int64) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_wide SELECT number, number FROM numbers(4);
ALTER TABLE t_wide ADD COLUMN a Int64 DEFAULT 7;
ALTER TABLE t_wide UPDATE a = -1 WHERE id IN (1, 2), UPDATE x = x + 1 WHERE 1, MATERIALIZE COLUMN a;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_wide' AND active;
SELECT 'wide', id, x, a FROM t_wide ORDER BY id;
DROP TABLE t_wide;

-- A command after an UPDATE of an input of the DEFAULT sees the DEFAULT of the new value.
DROP TABLE IF EXISTS t_update;
CREATE TABLE t_update (id Int64, x Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_update SELECT number, number FROM numbers(4);
ALTER TABLE t_update ADD COLUMN a Int64 DEFAULT x * 10;
ALTER TABLE t_update UPDATE x = 100 WHERE id = 3, UPDATE a = -1 WHERE id IN (1, 2);
SELECT 'update', id, x, a FROM t_update ORDER BY id;
DROP TABLE t_update;

-- Chain of defaults, ALIAS and a subcolumn of an unstored column.
DROP TABLE IF EXISTS t_chain;
CREATE TABLE t_chain (id Int64, x Int64, y Int64, z UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_chain SELECT number, number, 0, 0 FROM numbers(4);
ALTER TABLE t_chain ADD COLUMN b Int64 DEFAULT x + 1;
ALTER TABLE t_chain ADD COLUMN c Int64 DEFAULT b * 10;
ALTER TABLE t_chain ADD COLUMN cc Int64 ALIAS c;
ALTER TABLE t_chain ADD COLUMN n Nullable(Int64) DEFAULT if(x = 100, NULL, x);
ALTER TABLE t_chain UPDATE x = 100 WHERE id = 3, UPDATE y = cc WHERE 1, UPDATE z = n.null WHERE 1;
SELECT 'chain', id, x, y, z FROM t_chain ORDER BY id;
DROP TABLE t_chain;

-- Subcolumns inside the DEFAULT.
DROP TABLE IF EXISTS t_subcolumns;
CREATE TABLE t_subcolumns (id Int64, t Tuple(p Int64), n Nullable(Int64), y Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_subcolumns SELECT number, tuple(number), number, 0 FROM numbers(4);
ALTER TABLE t_subcolumns ADD COLUMN a Int64 DEFAULT t.p * 10;
ALTER TABLE t_subcolumns ADD COLUMN b UInt8 DEFAULT n.null;
ALTER TABLE t_subcolumns UPDATE t = tuple(100), n = NULL WHERE id = 3, UPDATE y = a + b WHERE 1;
SELECT 'subcolumns', id, y, a, b FROM t_subcolumns ORDER BY id;
DROP TABLE t_subcolumns;

-- Subcolumn of a Nested element inside the DEFAULT.
DROP TABLE IF EXISTS t_nested;
CREATE TABLE t_nested (id Int64, n Nested(e Int64), y Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_nested SELECT number, range(number), 0 FROM numbers(4);
ALTER TABLE t_nested ADD COLUMN a UInt64 DEFAULT n.e.size0;
ALTER TABLE t_nested UPDATE `n.e` = [1, 2, 3, 4, 5] WHERE id = 3, UPDATE y = a WHERE 1;
SELECT 'nested', id, n.e, y, a FROM t_nested ORDER BY id;
DROP TABLE t_nested;

-- The commands also read the subcolumn the DEFAULT reads.
DROP TABLE IF EXISTS t_subcolumn_read;
CREATE TABLE t_subcolumn_read (id Int64, t Tuple(p Int64), y Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_subcolumn_read SELECT number, tuple(number), 0 FROM numbers(4);
ALTER TABLE t_subcolumn_read ADD COLUMN a Int64 DEFAULT t.p * 10;
ALTER TABLE t_subcolumn_read UPDATE t = tuple(100) WHERE id = 3, UPDATE y = a + t.p WHERE t.p > 1, DELETE WHERE a = 30 AND t.p > 0;
SELECT 'subcolumn_read', id, t.p, y, a FROM t_subcolumn_read ORDER BY id;
DROP TABLE t_subcolumn_read;

-- The DEFAULT reads a subcolumn of an ALIAS column.
DROP TABLE IF EXISTS t_alias_subcolumn;
CREATE TABLE t_alias_subcolumn (id Int64, x Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_alias_subcolumn SELECT number, number FROM numbers(3);
ALTER TABLE t_alias_subcolumn ADD COLUMN b Tuple(p Int64) ALIAS tuple(x);
ALTER TABLE t_alias_subcolumn ADD COLUMN a Int64 DEFAULT b.p * 10;
ALTER TABLE t_alias_subcolumn UPDATE x = 100 WHERE id = 1, MATERIALIZE COLUMN a;
SELECT 'alias_subcolumn', id, x, a FROM t_alias_subcolumn ORDER BY id;
DROP TABLE t_alias_subcolumn;

-- DELETE predicate and the columns DELETE writes.
DROP TABLE IF EXISTS t_delete;
CREATE TABLE t_delete (id Int64, x Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_delete SELECT number, number FROM numbers(4);
ALTER TABLE t_delete ADD COLUMN a Int64 DEFAULT x * 10;
ALTER TABLE t_delete UPDATE x = 100 WHERE id IN (2, 3), DELETE WHERE a = 1000 AND id = 2;
SELECT 'delete', id, x, a FROM t_delete ORDER BY id;
DROP TABLE t_delete;

-- UPDATE of an input, UPDATE of the column, MATERIALIZE COLUMN.
DROP TABLE IF EXISTS t_combined;
CREATE TABLE t_combined (id Int64, x Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_combined SELECT number, number FROM numbers(4);
ALTER TABLE t_combined ADD COLUMN a Int64 DEFAULT x * 10;
ALTER TABLE t_combined UPDATE x = 100 WHERE id = 3, UPDATE a = -1 WHERE id IN (1, 2), MATERIALIZE COLUMN a;
SELECT 'combined', id, x, a FROM t_combined ORDER BY id;
DROP TABLE t_combined;

-- A second MATERIALIZE COLUMN keeps what the first one wrote.
DROP TABLE IF EXISTS t_materialize_twice;
CREATE TABLE t_materialize_twice (id Int64, x Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_materialize_twice SELECT number, number FROM numbers(4);
ALTER TABLE t_materialize_twice ADD COLUMN a Int64 DEFAULT x * 10;
ALTER TABLE t_materialize_twice MATERIALIZE COLUMN a, UPDATE x = 100 WHERE id = 3, MATERIALIZE COLUMN a;
SELECT 'materialize_twice', id, x, a FROM t_materialize_twice ORDER BY id;
DROP TABLE t_materialize_twice;

-- The DEFAULT reads a stored column, which does not change when its own input does.
DROP TABLE IF EXISTS t_stored_input;
CREATE TABLE t_stored_input (id Int64, x Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_stored_input SELECT number, number FROM numbers(4);
ALTER TABLE t_stored_input ADD COLUMN b Int64 DEFAULT x + 1;
ALTER TABLE t_stored_input MATERIALIZE COLUMN b;
ALTER TABLE t_stored_input ADD COLUMN a Int64 DEFAULT b * 10;
ALTER TABLE t_stored_input UPDATE a = -1 WHERE id = 1, UPDATE x = 100 WHERE id = 3, MATERIALIZE COLUMN a;
SELECT 'stored_input', id, x, b, a FROM t_stored_input ORDER BY id;
DROP TABLE t_stored_input;

-- The DEFAULT reads a column with a dotted name, which an UPDATE of the column named by its prefix does not change.
DROP TABLE IF EXISTS t_dotted;
CREATE TABLE t_dotted (id Int64, a Int64, `a.b` Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_dotted SELECT number, number, number + 10 FROM numbers(3);
ALTER TABLE t_dotted ADD COLUMN d Int64 DEFAULT `a.b` * 10;
ALTER TABLE t_dotted UPDATE d = -1 WHERE id = 1, UPDATE a = 100 WHERE id = 2, MATERIALIZE COLUMN d;
SELECT 'dotted', id, a, `a.b`, d FROM t_dotted ORDER BY id;
DROP TABLE t_dotted;

-- MATERIALIZE COLUMN after an UPDATE of the column that matches no row and an UPDATE of an input recomputes the column.
DROP TABLE IF EXISTS t_no_match;
CREATE TABLE t_no_match (id Int64, x Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_no_match SELECT number, number FROM numbers(4);
ALTER TABLE t_no_match ADD COLUMN a Int64 DEFAULT x * 10;
ALTER TABLE t_no_match UPDATE a = -1 WHERE id = 100, UPDATE x = 100 WHERE id = 3, MATERIALIZE COLUMN a;
SELECT 'no_match', id, x, a FROM t_no_match ORDER BY id;
DROP TABLE t_no_match;

-- The DEFAULT reads a stored MATERIALIZED column that reads an EPHEMERAL one, which an UPDATE of its other input does not recompute.
DROP TABLE IF EXISTS t_ephemeral;
CREATE TABLE t_ephemeral (id Int64, x Int64, e Int64 EPHEMERAL 1, m Int64 MATERIALIZED x + e) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_ephemeral (id, x) SELECT number, number FROM numbers(3);
ALTER TABLE t_ephemeral ADD COLUMN d Int64 DEFAULT m * 10;
ALTER TABLE t_ephemeral UPDATE d = -1 WHERE id = 1, UPDATE x = 100 WHERE id = 2, MATERIALIZE COLUMN d SETTINGS send_logs_level = 'error';
SELECT 'ephemeral', id, x, m, d FROM t_ephemeral ORDER BY id;
DROP TABLE t_ephemeral;

-- The DEFAULT reads a stored MATERIALIZED column that an UPDATE of its input recomputes.
DROP TABLE IF EXISTS t_materialized_input;
CREATE TABLE t_materialized_input (id Int64, x Int64, m Int64 MATERIALIZED x + 1) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_materialized_input (id, x) SELECT number, number FROM numbers(3);
ALTER TABLE t_materialized_input ADD COLUMN d Int64 DEFAULT m * 10;
ALTER TABLE t_materialized_input UPDATE d = -1 WHERE id = 100, UPDATE x = 100 WHERE id = 2, MATERIALIZE COLUMN d;
SELECT 'materialized_input', id, x, m, d FROM t_materialized_input ORDER BY id;
DROP TABLE t_materialized_input;

-- MATERIALIZE COLUMN of an unstored input stores it, so an UPDATE of the input's own input does not change what the DEFAULT reads.
DROP TABLE IF EXISTS t_barrier;
CREATE TABLE t_barrier (id Int64, x Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_barrier SELECT number, number FROM numbers(3);
ALTER TABLE t_barrier ADD COLUMN b Int64 DEFAULT x + 1;
ALTER TABLE t_barrier ADD COLUMN d Int64 DEFAULT b * 10;
ALTER TABLE t_barrier UPDATE d = -1 WHERE id = 1, MATERIALIZE COLUMN b, UPDATE x = 100 WHERE id = 2, MATERIALIZE COLUMN d;
SELECT 'barrier', id, x, b, d FROM t_barrier ORDER BY id;
DROP TABLE t_barrier;

-- Same with MATERIALIZE COLUMN of the input before the UPDATE of the column.
DROP TABLE IF EXISTS t_barrier_first;
CREATE TABLE t_barrier_first (id Int64, x Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_barrier_first SELECT number, number FROM numbers(3);
ALTER TABLE t_barrier_first ADD COLUMN b Int64 DEFAULT x + 1;
ALTER TABLE t_barrier_first ADD COLUMN d Int64 DEFAULT b * 10;
ALTER TABLE t_barrier_first MATERIALIZE COLUMN b, UPDATE d = -1 WHERE id = 1, UPDATE x = 100 WHERE id = 2, MATERIALIZE COLUMN d;
SELECT 'barrier_first', id, x, b, d FROM t_barrier_first ORDER BY id;
DROP TABLE t_barrier_first;

-- Same with an unstored MATERIALIZED input.
DROP TABLE IF EXISTS t_materialized_barrier;
CREATE TABLE t_materialized_barrier (id Int64, x Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_materialized_barrier SELECT number, number FROM numbers(3);
ALTER TABLE t_materialized_barrier ADD COLUMN c Int64 MATERIALIZED x + 1;
ALTER TABLE t_materialized_barrier ADD COLUMN d Int64 DEFAULT c * 10;
ALTER TABLE t_materialized_barrier UPDATE d = -1 WHERE id = 1, MATERIALIZE COLUMN c, MATERIALIZE COLUMN d;
SELECT 'materialized_barrier', id, x, c, d FROM t_materialized_barrier ORDER BY id;
DROP TABLE t_materialized_barrier;

-- Queued mutations with a DROP COLUMN of another column, added again, between the UPDATE and the MATERIALIZE COLUMN.
DROP TABLE IF EXISTS t_queued_drop;
CREATE TABLE t_queued_drop (id Int64, x Int64, z Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_queued_drop SELECT number, number, number FROM numbers(3);
ALTER TABLE t_queued_drop ADD COLUMN d Int64 DEFAULT x * 10;
SYSTEM STOP MERGES t_queued_drop;
ALTER TABLE t_queued_drop UPDATE d = -1 WHERE id = 1 SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_queued_drop DROP COLUMN z, ADD COLUMN z Int64 DEFAULT 7 SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_queued_drop MATERIALIZE COLUMN d SETTINGS mutations_sync = 0, alter_sync = 0;
SYSTEM START MERGES t_queued_drop;
ALTER TABLE t_queued_drop DELETE WHERE 0;
SELECT 'queued_drop', id, x, z, d FROM t_queued_drop ORDER BY id;
DROP TABLE t_queued_drop;

-- A stored column is not recomputed.
DROP TABLE IF EXISTS t_stored;
CREATE TABLE t_stored (id Int64, x Int64, y Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_stored SELECT number, number, 0 FROM numbers(4);
ALTER TABLE t_stored ADD COLUMN a Int64 DEFAULT x * 10;
ALTER TABLE t_stored MATERIALIZE COLUMN a;
ALTER TABLE t_stored UPDATE x = 100 WHERE id = 3, UPDATE y = a WHERE 1;
SELECT 'stored', id, y FROM t_stored ORDER BY id;
DROP TABLE t_stored;

-- MATERIALIZE COLUMN after an UPDATE of an input recomputes the column.
DROP TABLE IF EXISTS t_materialize;
CREATE TABLE t_materialize (id Int64, x Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_materialize SELECT number, number FROM numbers(4);
ALTER TABLE t_materialize ADD COLUMN a Int64 DEFAULT x * 10;
ALTER TABLE t_materialize UPDATE x = 100 WHERE id = 1, MATERIALIZE COLUMN a;
SELECT 'materialize', id, a FROM t_materialize ORDER BY id;
DROP TABLE t_materialize;

-- A column updated by a patch part keeps the patched value.
DROP TABLE IF EXISTS t_patch;
CREATE TABLE t_patch (id Int64, x Int64, y Int64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_patch SELECT number, number, 0 FROM numbers(4);
ALTER TABLE t_patch ADD COLUMN a Int64 DEFAULT x * 10;
UPDATE t_patch SET a = -1 WHERE id = 1;
ALTER TABLE t_patch UPDATE x = 100 WHERE id = 3, UPDATE y = a WHERE 1;
SELECT 'patch', id, y FROM t_patch ORDER BY id;
DROP TABLE t_patch;
