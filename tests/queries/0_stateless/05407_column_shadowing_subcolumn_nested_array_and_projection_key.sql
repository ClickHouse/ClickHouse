-- A column `a.x` of type `Array` added next to a `JSON` column `a` is read as a subcolumn of a `Nested` column `a`
-- from wide parts. A part written before the column was added holds the path `x` of `a`, not the column, and reads
-- its default.

DROP TABLE IF EXISTS t_shadow_nested_array;

CREATE TABLE t_shadow_nested_array (id UInt64, a JSON) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, share_nested_offsets = 1;

INSERT INTO t_shadow_nested_array VALUES (1, '{"x":[11,22]}');

ALTER TABLE t_shadow_nested_array ADD COLUMN `a.x` Array(UInt64) DEFAULT [7];

SELECT id, `a.x`, `a.x.size0` FROM t_shadow_nested_array;

DROP TABLE t_shadow_nested_array;

-- A column added under the name of a subcolumn that the key of a projection reads is refused: the projection parts are
-- indexed and sorted by the old subcolumn.

DROP TABLE IF EXISTS t_shadow_projection_key;

CREATE TABLE t_shadow_projection_key (id UInt64, a Array(UInt64), PROJECTION p (SELECT id, a ORDER BY `a.size0`))
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;

INSERT INTO t_shadow_projection_key VALUES (1, [0]), (2, [1, 2]);

ALTER TABLE t_shadow_projection_key ADD COLUMN `a.size0` UInt64 DEFAULT 7; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_shadow_projection_key ADD COLUMN `b.size0` UInt64 DEFAULT 7;

SELECT count() FROM t_shadow_projection_key WHERE `a.size0` = 2 SETTINGS optimize_use_projections = 0;
SELECT count() FROM t_shadow_projection_key WHERE `a.size0` = 2 SETTINGS optimize_use_projections = 1;

DROP TABLE t_shadow_projection_key;
