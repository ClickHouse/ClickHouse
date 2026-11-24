DROP TABLE IF EXISTS t_ephemeral_pending_clear;

CREATE TABLE t_ephemeral_pending_clear (p UInt64, a UInt64, c UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
    min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0, min_level_for_full_part_storage = 0,
    enable_block_number_column = 0, enable_block_offset_column = 0,
    skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
INSERT INTO t_ephemeral_pending_clear VALUES (0, 7, 8), (1, 9, 10);
ALTER TABLE t_ephemeral_pending_clear ADD COLUMN b UInt64 DEFAULT 42;

SYSTEM STOP MERGES t_ephemeral_pending_clear;
ALTER TABLE t_ephemeral_pending_clear CLEAR COLUMN c IN PARTITION 1
    SETTINGS alter_sync = 0, mutations_sync = 0;
-- The queued clear must not hide the surviving physical `c` in partition 0.
ALTER TABLE t_ephemeral_pending_clear MODIFY COLUMN a UInt64 EPHEMERAL;
SELECT p, b, c FROM t_ephemeral_pending_clear ORDER BY p;
SYSTEM START MERGES t_ephemeral_pending_clear;
-- Wait for the earlier clear without rewriting the untouched partition.
ALTER TABLE t_ephemeral_pending_clear UPDATE b = b IN PARTITION 1 WHERE 1 SETTINGS mutations_sync = 2;
SELECT p, b, c FROM t_ephemeral_pending_clear ORDER BY p;
DROP TABLE t_ephemeral_pending_clear;

CREATE TABLE t_ephemeral_pending_clear (p UInt64, a UInt64, c UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0, min_level_for_full_part_storage = 0,
    enable_block_number_column = 0, enable_block_offset_column = 0,
    skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
INSERT INTO t_ephemeral_pending_clear VALUES (0, 7, 8), (1, 9, 10);
ALTER TABLE t_ephemeral_pending_clear ADD COLUMN b UInt64 DEFAULT 42;

SYSTEM STOP MERGES t_ephemeral_pending_clear;
ALTER TABLE t_ephemeral_pending_clear CLEAR COLUMN c IN PARTITION 1
    SETTINGS alter_sync = 0, mutations_sync = 0;
-- The queued clear must not hide the surviving physical `c` in partition 0.
ALTER TABLE t_ephemeral_pending_clear MODIFY COLUMN a UInt64 EPHEMERAL;
SELECT p, b, c FROM t_ephemeral_pending_clear ORDER BY p;
SYSTEM START MERGES t_ephemeral_pending_clear;
-- Wait for the earlier clear without rewriting the untouched partition.
ALTER TABLE t_ephemeral_pending_clear UPDATE b = b IN PARTITION 1 WHERE 1 SETTINGS mutations_sync = 2;
SELECT p, b, c FROM t_ephemeral_pending_clear ORDER BY p;
DROP TABLE t_ephemeral_pending_clear;
