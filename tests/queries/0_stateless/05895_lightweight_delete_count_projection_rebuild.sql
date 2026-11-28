-- Keep exact live-row counts when a partial mutation reads `_row_exists` to rebuild a projection.
SET optimize_trivial_count_query = 0;
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;
SET apply_deleted_mask = 1;
SET lightweight_deletes_sync = 2;
SET mutations_sync = 2;
SET lightweight_delete_mode = 'alter_update';

DROP TABLE IF EXISTS lwd_count_projection_rebuild;
CREATE TABLE lwd_count_projection_rebuild
(
    id UInt64,
    value UInt64,
    PROJECTION p (SELECT sum(value))
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    lightweight_mutation_projection_mode = 'rebuild',
    exclude_deleted_rows_for_part_size_in_merge = 1, load_existing_rows_count_for_old_parts = 1;

INSERT INTO lwd_count_projection_rebuild SELECT number, number + 1 FROM numbers(12);
DELETE FROM lwd_count_projection_rebuild WHERE id % 3 = 0;
SELECT count() FROM lwd_count_projection_rebuild SETTINGS force_optimize_projection_name = '_minmax_count_projection';
SELECT sum(value) FROM lwd_count_projection_rebuild SETTINGS force_optimize_projection_name = 'p';

-- Updating only `value` keeps the mask and passes it to the projection in a readonly mutation stage.
ALTER TABLE lwd_count_projection_rebuild UPDATE value = value + 100 WHERE id = 1;
SELECT count() FROM lwd_count_projection_rebuild SETTINGS force_optimize_projection_name = '_minmax_count_projection';
SELECT sum(value) FROM lwd_count_projection_rebuild SETTINGS force_optimize_projection_name = 'p';
-- With the mask disabled, neither projection may hide the physically present deleted rows.
SELECT count() FROM lwd_count_projection_rebuild SETTINGS apply_deleted_mask = 0;
SELECT sum(value) FROM lwd_count_projection_rebuild SETTINGS apply_deleted_mask = 0;

ALTER TABLE lwd_count_projection_rebuild CLEAR PROJECTION p;
ALTER TABLE lwd_count_projection_rebuild MATERIALIZE PROJECTION p;
SELECT count() FROM lwd_count_projection_rebuild SETTINGS force_optimize_projection_name = '_minmax_count_projection';
SELECT sum(value) FROM lwd_count_projection_rebuild SETTINGS force_optimize_projection_name = 'p';

ALTER TABLE lwd_count_projection_rebuild CLEAR COLUMN value;
SELECT count() FROM lwd_count_projection_rebuild SETTINGS force_optimize_projection_name = '_minmax_count_projection';
SELECT sum(value) FROM lwd_count_projection_rebuild SETTINGS force_optimize_projection_name = 'p';
DETACH TABLE lwd_count_projection_rebuild;
ATTACH TABLE lwd_count_projection_rebuild;
SELECT count() FROM lwd_count_projection_rebuild SETTINGS force_optimize_projection_name = '_minmax_count_projection';

DROP TABLE lwd_count_projection_rebuild;
