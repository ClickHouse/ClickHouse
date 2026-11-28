-- `CLEAR COLUMN` of a whole `Nested` group clears every column of the group, also on wide parts, and rebuilds
-- what reads them (a MATERIALIZED column, a skip index, a projection), the same as clearing the columns one by one.
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_wide;
DROP TABLE IF EXISTS t_compact;
DROP TABLE IF EXISTS t_flat;

CREATE TABLE t_wide (p UInt8, n Nested(a UInt32, b UInt32), m UInt64 MATERIALIZED arraySum(n.a),
    INDEX ia n.a TYPE bloom_filter GRANULARITY 1, PROJECTION pr (SELECT p, n.a ORDER BY n.a))
ENGINE = MergeTree PARTITION BY p ORDER BY p
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_wide VALUES (1, [1, 2], [3, 4]) (2, [7], [8]);
ALTER TABLE t_wide CLEAR COLUMN n IN PARTITION 1;
SELECT p, n.a, n.b, m FROM t_wide ORDER BY p;
SELECT p, `n.a` FROM mergeTreeProjection(currentDatabase(), t_wide, 'pr') ORDER BY p;
ALTER TABLE t_wide CLEAR COLUMN n;
SELECT p, n.a, n.b, m FROM t_wide ORDER BY p;
SELECT p, `n.a` FROM mergeTreeProjection(currentDatabase(), t_wide, 'pr') ORDER BY p;
SELECT DISTINCT part_type, part_storage_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_wide' AND active;
ALTER TABLE t_wide MODIFY SETTING alter_column_secondary_index_mode = 'throw';
ALTER TABLE t_wide CLEAR COLUMN n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

CREATE TABLE t_compact (p UInt8, n Nested(a UInt32, b UInt32), m UInt64 MATERIALIZED arraySum(n.a),
    INDEX ia n.a TYPE bloom_filter GRANULARITY 1, PROJECTION pr (SELECT p, n.a ORDER BY n.a))
ENGINE = MergeTree PARTITION BY p ORDER BY p
SETTINGS min_rows_for_wide_part = 1000000000, min_bytes_for_wide_part = 1000000000, min_bytes_for_full_part_storage = 0;

INSERT INTO t_compact VALUES (1, [1, 2], [3, 4]) (2, [7], [8]);
ALTER TABLE t_compact CLEAR COLUMN n IN PARTITION 1;
SELECT p, n.a, n.b, m FROM t_compact ORDER BY p;
SELECT p, `n.a` FROM mergeTreeProjection(currentDatabase(), t_compact, 'pr') ORDER BY p;
ALTER TABLE t_compact CLEAR COLUMN n;
SELECT p, n.a, n.b, m FROM t_compact ORDER BY p;
SELECT p, `n.a` FROM mergeTreeProjection(currentDatabase(), t_compact, 'pr') ORDER BY p;
SELECT DISTINCT part_type, part_storage_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_compact' AND active;
INSERT INTO t_compact VALUES (3, [5], [6]);
ALTER TABLE t_compact RENAME COLUMN n.b TO n.c, CLEAR COLUMN n;
SELECT p, n.a, n.c, m FROM t_compact ORDER BY p;

CREATE TABLE t_flat (p UInt8, `n.a` Array(UInt32), INDEX ia `n.a` TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY p SETTINGS share_nested_offsets = 0, alter_column_secondary_index_mode = 'throw';
INSERT INTO t_flat VALUES (1, [1]);
ALTER TABLE t_flat CLEAR COLUMN IF EXISTS n;
SELECT p, `n.a` FROM t_flat;

DROP TABLE t_wide;
DROP TABLE t_compact;
DROP TABLE t_flat;
