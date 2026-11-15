-- Tags: no-replicated-database
-- `MODIFY COLUMN` on an `EPHEMERAL` column declared without an expression kept the column's
-- `ephemeral_default` flag, which says "do not print the expression". The new kind was then printed
-- with no expression at all (`value UInt64 DEFAULT`), and the metadata failed to parse back with
-- `SYNTAX_ERROR`. Asked for `EPHEMERAL 7` the same flag dropped the `7` from the stored metadata.

DROP TABLE IF EXISTS t_ephemeral_modify;

CREATE TABLE t_ephemeral_modify (id UInt64, value UInt64 EPHEMERAL) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_ephemeral_modify MODIFY COLUMN value UInt64 DEFAULT 7;
SELECT default_kind FROM system.columns WHERE database = currentDatabase() AND table = 't_ephemeral_modify' AND name = 'value';
DROP TABLE t_ephemeral_modify;

CREATE TABLE t_ephemeral_modify (id UInt64, value UInt64 EPHEMERAL) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_ephemeral_modify MODIFY COLUMN value UInt64 MATERIALIZED 7;
SELECT default_kind FROM system.columns WHERE database = currentDatabase() AND table = 't_ephemeral_modify' AND name = 'value';
DROP TABLE t_ephemeral_modify;

CREATE TABLE t_ephemeral_modify (id UInt64, value UInt64 EPHEMERAL) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_ephemeral_modify MODIFY COLUMN value UInt64 ALIAS 7;
SELECT default_kind FROM system.columns WHERE database = currentDatabase() AND table = 't_ephemeral_modify' AND name = 'value';
DROP TABLE t_ephemeral_modify;

CREATE TABLE t_ephemeral_modify (id UInt64, value UInt64 EPHEMERAL) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_ephemeral_modify MODIFY COLUMN value UInt64 EPHEMERAL 7;
SELECT position('EPHEMERAL 7' IN create_table_query) > 0 FROM system.tables WHERE database = currentDatabase() AND name = 't_ephemeral_modify';
DROP TABLE t_ephemeral_modify;
