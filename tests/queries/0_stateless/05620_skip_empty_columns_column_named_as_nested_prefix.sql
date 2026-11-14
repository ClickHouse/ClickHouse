-- A column `n` next to a column `n.x`: the part omits `n` when it holds only defaults, and the reader must not take the marker
-- of `n` for `n.x`, which `Nested::convertToSubcolumns` represents as the subcolumn `x` of `n`.

DROP TABLE IF EXISTS t_prefix_wide;
DROP TABLE IF EXISTS t_prefix_compact;
DROP TABLE IF EXISTS t_prefix_reverse;

CREATE TABLE t_prefix_wide (id UInt64, n UInt64, `n.x` Array(UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
INSERT INTO t_prefix_wide VALUES (1, 0, [10, 20]), (2, 0, [30]);

SELECT 'wide part columns', column FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_prefix_wide' AND active ORDER BY column;
SELECT `n.x` FROM t_prefix_wide ORDER BY id;
SELECT id, `n.x` FROM t_prefix_wide WHERE length(`n.x`) = 1;

CREATE TABLE t_prefix_compact (id UInt64, n UInt64, `n.x` Array(UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
INSERT INTO t_prefix_compact VALUES (1, 0, [10, 20]), (2, 0, [30]);

SELECT 'compact part', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_prefix_compact' AND active;
SELECT `n.x` FROM t_prefix_compact ORDER BY id;

-- The reverse: `n.x` holds only empty arrays and is omitted, `n` is stored.
CREATE TABLE t_prefix_reverse (id UInt64, n UInt64, `n.x` Array(UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
INSERT INTO t_prefix_reverse VALUES (1, 7, []), (2, 8, []);

SELECT 'reverse part columns', column FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_prefix_reverse' AND active ORDER BY column;
SELECT id, `n.x` FROM t_prefix_reverse ORDER BY id;

DROP TABLE t_prefix_wide;
DROP TABLE t_prefix_compact;
DROP TABLE t_prefix_reverse;
