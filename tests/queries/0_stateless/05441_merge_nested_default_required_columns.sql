-- A `Nested` subcolumn whose `DEFAULT` reads a column absent from all source parts is expired by the
-- merge (see `04050_vertical_merge_nested_default_corruption`), unless the merge itself needs it or it
-- cannot corrupt shared `Nested` offsets.
-- https://github.com/ClickHouse/ClickHouse/issues/86123

-- The merge algorithm consumes the column: `SummingMergeTree` sums the `nMap` structure, so the summed
-- `nMap.v` must be written instead of being recomputed from its `DEFAULT` on read.
DROP TABLE IF EXISTS t_nested_default_summing;
CREATE TABLE t_nested_default_summing (id UInt32, `nMap.k` Array(UInt64))
ENGINE = SummingMergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1, max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO t_nested_default_summing VALUES (1, [1, 2]);
INSERT INTO t_nested_default_summing VALUES (1, [1, 2]);
ALTER TABLE t_nested_default_summing ADD COLUMN m UInt64;
ALTER TABLE t_nested_default_summing ADD COLUMN `nMap.v` Array(UInt64) DEFAULT arrayMap(x -> x + m, `nMap.k`);
OPTIMIZE TABLE t_nested_default_summing FINAL;
SELECT 'summing', `nMap.k`, `nMap.v` FROM t_nested_default_summing;
DROP TABLE t_nested_default_summing;

-- A TTL expression reads the column: it must be produced by the merge, otherwise the TTL step throws.
SET materialize_ttl_after_modify = 0;
DROP TABLE IF EXISTS t_nested_default_ttl;
CREATE TABLE t_nested_default_ttl (id UInt32, ts DateTime, `n.a` Array(UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1, max_bytes_to_merge_at_max_space_in_pool = 1, enable_vertical_merge_algorithm = 0;
INSERT INTO t_nested_default_ttl VALUES (1, '2000-01-01 00:00:00', [10, 20]);
INSERT INTO t_nested_default_ttl VALUES (2, '2000-01-01 00:00:00', [30, 40]);
ALTER TABLE t_nested_default_ttl ADD COLUMN m Array(String);
ALTER TABLE t_nested_default_ttl ADD COLUMN `n.b` Array(String) DEFAULT arrayConcat(m, ['x', 'y']);
ALTER TABLE t_nested_default_ttl MODIFY TTL ts DELETE WHERE has(`n.b`, 'x');
OPTIMIZE TABLE t_nested_default_ttl FINAL;
SELECT 'ttl', count() FROM t_nested_default_ttl;
DROP TABLE t_nested_default_ttl;

-- A dotted scalar does not share `Nested` offsets, so its default is materialized by the merge.
DROP TABLE IF EXISTS t_dotted_scalar_default;
CREATE TABLE t_dotted_scalar_default (id UInt32)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1, max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO t_dotted_scalar_default VALUES (1);
INSERT INTO t_dotted_scalar_default VALUES (2);
ALTER TABLE t_dotted_scalar_default ADD COLUMN m UInt64;
ALTER TABLE t_dotted_scalar_default ADD COLUMN `x.y` UInt64 DEFAULT rand64() + m;
OPTIMIZE TABLE t_dotted_scalar_default FINAL;
SELECT 'dotted scalar stored', count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_dotted_scalar_default' AND active AND column = 'x.y';
SELECT 'dotted scalar stable', (SELECT groupArray(`x.y`) FROM (SELECT `x.y` FROM t_dotted_scalar_default ORDER BY id))
    = (SELECT groupArray(`x.y`) FROM (SELECT `x.y` FROM t_dotted_scalar_default ORDER BY id));
DROP TABLE t_dotted_scalar_default;

-- With `share_nested_offsets = 0` every array writes its own offsets, so its default is materialized too.
DROP TABLE IF EXISTS t_unshared_offsets_default;
CREATE TABLE t_unshared_offsets_default (id UInt32, `x.a` Array(UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1, max_bytes_to_merge_at_max_space_in_pool = 1, share_nested_offsets = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;
INSERT INTO t_unshared_offsets_default VALUES (1, [1]);
INSERT INTO t_unshared_offsets_default VALUES (2, [2, 3]);
ALTER TABLE t_unshared_offsets_default ADD COLUMN m UInt64;
ALTER TABLE t_unshared_offsets_default ADD COLUMN `x.y` Array(UInt64) DEFAULT [rand64() + m];
OPTIMIZE TABLE t_unshared_offsets_default FINAL;
SELECT 'unshared offsets stored', count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_unshared_offsets_default' AND active AND column = 'x.y';
SELECT 'unshared offsets stable', (SELECT groupArray(`x.y`) FROM (SELECT `x.y` FROM t_unshared_offsets_default ORDER BY id))
    = (SELECT groupArray(`x.y`) FROM (SELECT `x.y` FROM t_unshared_offsets_default ORDER BY id));
SELECT 'unshared offsets', id, `x.a`, length(`x.y`) FROM t_unshared_offsets_default ORDER BY id;
DROP TABLE t_unshared_offsets_default;
