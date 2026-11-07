-- A projection over a column named like a subcolumn of another column (`a.size0` next to an `Array` column `a`)
-- stays usable in a part that records the column as missing: the projection was computed from the column's
-- recorded default, not from the sizes of `a`.

DROP TABLE IF EXISTS t_projection_missing_marker;
CREATE TABLE t_projection_missing_marker (id UInt64, v UInt64, a Array(UInt64), `a.size0` UInt64,
    PROJECTION p (SELECT sum(`a.size0`)))
ENGINE = MergeTree ORDER BY id
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_level_for_wide_part = 0;

INSERT INTO t_projection_missing_marker (id, a) VALUES (1, [1, 2, 3]), (2, [4]);

SELECT sum(`a.size0`) FROM t_projection_missing_marker SETTINGS optimize_use_projections = 0;
SELECT sum(`a.size0`) FROM t_projection_missing_marker SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

-- After a merge and a mutation the projection is still used.
INSERT INTO t_projection_missing_marker (id, a) VALUES (3, [5, 6]);
OPTIMIZE TABLE t_projection_missing_marker FINAL;
SELECT sum(`a.size0`) FROM t_projection_missing_marker SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
ALTER TABLE t_projection_missing_marker UPDATE v = 1 WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT sum(`a.size0`) FROM t_projection_missing_marker SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_projection_missing_marker;
