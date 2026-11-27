-- An ALTER that would change the sorting key of a projection is rejected: its parts stay sorted by the old key.
-- A column matcher in the key expands to the current columns, so adding, dropping or moving a column changes it.

DROP TABLE IF EXISTS t_projection_key_change;
CREATE TABLE t_projection_key_change (id UInt64, x UInt64, y UInt64, PROJECTION p INDEX * TYPE basic)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128;
INSERT INTO t_projection_key_change SELECT number, number % 100, number FROM numbers(1000);
INSERT INTO t_projection_key_change SELECT number, number % 100, number FROM numbers(1000);

ALTER TABLE t_projection_key_change MODIFY COLUMN y UInt64 FIRST; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_projection_key_change DROP COLUMN y; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_projection_key_change ADD COLUMN z UInt64; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
-- a no-op `ADD PROJECTION IF NOT EXISTS` of the existing projection keeps its parts
ALTER TABLE t_projection_key_change ADD PROJECTION IF NOT EXISTS p INDEX * TYPE basic, ADD COLUMN z UInt64; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
-- the last column stays the same, so the key does
ALTER TABLE t_projection_key_change ADD COLUMN w UInt64 FIRST;

SELECT name, sorting_key FROM system.projections WHERE database = currentDatabase() AND table = 't_projection_key_change';
OPTIMIZE TABLE t_projection_key_change FINAL;
SELECT count() FROM t_projection_key_change WHERE y = 5;

DROP TABLE IF EXISTS t_projection_key_tuple;
CREATE TABLE t_projection_key_tuple (id UInt64, x UInt64, y UInt64, PROJECTION q (SELECT id ORDER BY tuple(*)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128;
INSERT INTO t_projection_key_tuple SELECT number, number % 100, number FROM numbers(1000);
ALTER TABLE t_projection_key_tuple ADD COLUMN z UInt64 FIRST; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT name, sorting_key FROM system.projections WHERE database = currentDatabase() AND table = 't_projection_key_tuple';

-- Keys that do not depend on the columns are kept, so these ALTERs are allowed.
DROP TABLE IF EXISTS t_projection_key_kept;
CREATE TABLE t_projection_key_kept (id UInt64, x UInt64, PROJECTION p_all (SELECT * ORDER BY x), PROJECTION p_commit INDEX * TYPE commit_order)
ENGINE = MergeTree ORDER BY id SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1;
INSERT INTO t_projection_key_kept SELECT number, number % 100 FROM numbers(1000);
ALTER TABLE t_projection_key_kept ADD COLUMN z UInt64;
ALTER TABLE t_projection_key_kept DROP COLUMN z;
SELECT name, sorting_key FROM system.projections WHERE database = currentDatabase() AND table = 't_projection_key_kept' ORDER BY name;

DROP TABLE t_projection_key_change;
DROP TABLE t_projection_key_tuple;
DROP TABLE t_projection_key_kept;
