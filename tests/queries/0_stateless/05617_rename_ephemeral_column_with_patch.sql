DROP TABLE IF EXISTS t_ephemeral_patch;

CREATE TABLE t_ephemeral_patch (a UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
    enable_block_number_column = 0, enable_block_offset_column = 0, apply_patches_on_merge = 0;

INSERT INTO t_ephemeral_patch VALUES (7), (9);
ALTER TABLE t_ephemeral_patch ADD COLUMN b UInt64 DEFAULT 42;
ALTER TABLE t_ephemeral_patch MATERIALIZE COLUMN b SETTINGS mutations_sync = 2;
ALTER TABLE t_ephemeral_patch MODIFY COLUMN a UInt64 EPHEMERAL;

-- Enable lightweight updates after materializing `b`.
ALTER TABLE t_ephemeral_patch MODIFY SETTING enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES t_ephemeral_patch;

UPDATE t_ephemeral_patch SET b = 43 WHERE b = 42 SETTINGS enable_lightweight_update = 1;
ALTER TABLE t_ephemeral_patch RENAME COLUMN a TO a_old SETTINGS alter_sync = 0;

SYSTEM START MERGES t_ephemeral_patch;
ALTER TABLE t_ephemeral_patch RENAME COLUMN a_old TO a_final SETTINGS alter_sync = 2;

-- The rewrite must omit historical `a` and preserve the patched `b` values.
SELECT count(), sum(b) FROM t_ephemeral_patch;
SELECT count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_ephemeral_patch' AND active
    AND column IN ('a', 'a_old', 'a_final');

DROP TABLE t_ephemeral_patch;
