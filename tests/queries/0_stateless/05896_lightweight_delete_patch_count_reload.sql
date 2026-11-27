-- A delete patch stores zero-valued `_row_exists` updates, not a visibility mask for the patch itself.
-- Loading live-row counts must not reject this valid sparse column and detach the patch.
SET enable_lightweight_update = 1;
SET apply_patch_parts = 1;
SET apply_deleted_mask = 1;
SET optimize_trivial_count_query = 0;
SET optimize_use_implicit_projections = 0;

DROP TABLE IF EXISTS lwd_patch_count_reload;
CREATE TABLE lwd_patch_count_reload (id UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
    ratio_of_defaults_for_sparse_serialization = 0.5,
    exclude_deleted_rows_for_part_size_in_merge = 1,
    load_existing_rows_count_for_old_parts = 1;

SYSTEM STOP MERGES lwd_patch_count_reload;
INSERT INTO lwd_patch_count_reload SELECT number FROM numbers(100);
DELETE FROM lwd_patch_count_reload WHERE id % 2 = 0
SETTINGS lightweight_delete_mode = 'lightweight_update_force';

SELECT count(), sum(id) FROM lwd_patch_count_reload;
DETACH TABLE lwd_patch_count_reload;
ATTACH TABLE lwd_patch_count_reload;
SELECT count(), sum(id) FROM lwd_patch_count_reload;
SELECT count() FROM system.detached_parts
WHERE database = currentDatabase() AND table = 'lwd_patch_count_reload';

SYSTEM START MERGES lwd_patch_count_reload;
OPTIMIZE TABLE lwd_patch_count_reload FINAL;
SELECT count(), sum(id) FROM lwd_patch_count_reload;
DROP TABLE lwd_patch_count_reload;
