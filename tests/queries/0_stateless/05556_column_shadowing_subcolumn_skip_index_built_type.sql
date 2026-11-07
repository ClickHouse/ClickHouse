-- A skip index materialized over a lazy JSON type hint records the type it was built against.
-- That record must not make a granule built over the path `a` of `j` usable for a column `j.a`
-- added later: the column `j.a` shadows the subcolumn and reads its own default.

SET enable_json_lazy_type_hints = 1;

DROP TABLE IF EXISTS t_shadow_built_type;

CREATE TABLE t_shadow_built_type (id UInt64, j JSON) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_shadow_built_type VALUES (1, '{"a":11}'), (2, '{"a":22}');
ALTER TABLE t_shadow_built_type MODIFY COLUMN j JSON(a UInt64);
ALTER TABLE t_shadow_built_type ADD INDEX idx `j.a` TYPE minmax GRANULARITY 1;
ALTER TABLE t_shadow_built_type MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;
ALTER TABLE t_shadow_built_type ADD COLUMN `j.a` UInt64 DEFAULT 7;

SELECT id, `j.a` FROM t_shadow_built_type ORDER BY id;
SELECT count() FROM t_shadow_built_type WHERE `j.a` = 7 SETTINGS use_skip_indexes = 1, optimize_move_to_prewhere = 0;
SELECT count() FROM t_shadow_built_type WHERE `j.a` = 7 SETTINGS use_skip_indexes = 0, optimize_move_to_prewhere = 0;

DROP TABLE t_shadow_built_type;
