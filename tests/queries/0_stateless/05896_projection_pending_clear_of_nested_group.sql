-- With `share_nested_offsets`, a pending `CLEAR COLUMN n` of a `Nested` group is applied at read time to
-- every column `n.x` of the group. The projection part still holds the old `n.x`, so a part with such a
-- mutation must be read without a projection that uses `n.x`.

SET min_table_rows_to_use_projection_index = 0;

DROP TABLE IF EXISTS t_nested_clear_index;
CREATE TABLE t_nested_clear_index
(
    id UInt64,
    k UInt64,
    `n.x` Array(UInt64) DEFAULT [1],
    PROJECTION p (SELECT _part_offset WHERE has(n.x, 1) ORDER BY k)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 16;
INSERT INTO t_nested_clear_index SELECT number, number % 100, [toUInt64(number % 2 = 0)] FROM numbers(2000);

SYSTEM STOP MERGES t_nested_clear_index;
ALTER TABLE t_nested_clear_index CLEAR COLUMN n IN PARTITION tuple() SETTINGS alter_sync = 0;

SELECT 'projection index';
SELECT count() FROM t_nested_clear_index WHERE k = 5 AND has(n.x, 1);
SELECT count() FROM t_nested_clear_index WHERE k = 5 AND has(n.x, 1) SETTINGS optimize_use_projections = 0;

DROP TABLE IF EXISTS t_nested_clear_normal;
CREATE TABLE t_nested_clear_normal
(
    id UInt64,
    k UInt64,
    `n.x` Array(UInt64) DEFAULT [1],
    `m.y` Array(UInt64),
    PROJECTION p (SELECT id, k, n.x ORDER BY k)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 16;
INSERT INTO t_nested_clear_normal SELECT number, number % 100, [toUInt64(number % 2 = 0)], [number] FROM numbers(2000);

SYSTEM STOP MERGES t_nested_clear_normal;

SELECT 'pending clear of another group';
ALTER TABLE t_nested_clear_normal CLEAR COLUMN m IN PARTITION tuple() SETTINGS alter_sync = 0;
SELECT sum(n.x[1]) FROM t_nested_clear_normal WHERE k = 5 SETTINGS force_optimize_projection = 1;

SELECT 'normal projection';
ALTER TABLE t_nested_clear_normal CLEAR COLUMN n IN PARTITION tuple() SETTINGS alter_sync = 0;
SELECT sum(n.x[1]) FROM t_nested_clear_normal WHERE k = 5;
SELECT sum(n.x[1]) FROM t_nested_clear_normal WHERE k = 5 SETTINGS optimize_use_projections = 0;

DROP TABLE t_nested_clear_index;
DROP TABLE t_nested_clear_normal;
