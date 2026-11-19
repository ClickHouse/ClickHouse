DROP TABLE IF EXISTS t_pending_read;

CREATE TABLE t_pending_read (a UInt64, c UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
    enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_pending_read VALUES (7, 8), (9, 10);

SYSTEM STOP MERGES t_pending_read;
ALTER TABLE t_pending_read MODIFY COLUMN a UInt32 SETTINGS alter_sync = 0, mutations_sync = 0;
-- A surviving `c` cannot satisfy the pending `READ_COLUMN a`.
ALTER TABLE t_pending_read MODIFY COLUMN a UInt32 EPHEMERAL; -- { serverError BAD_ARGUMENTS }
SELECT type = 'UInt32', default_kind = '' FROM system.columns
WHERE database = currentDatabase() AND table = 't_pending_read' AND name = 'a';
SYSTEM START MERGES t_pending_read;

ALTER TABLE t_pending_read UPDATE c = c + 1 WHERE 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_pending_read MODIFY COLUMN a UInt32 EPHEMERAL;
ALTER TABLE t_pending_read UPDATE c = c + 1 WHERE 1 SETTINGS mutations_sync = 2;
SELECT c FROM t_pending_read ORDER BY c;
SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_pending_read' AND NOT is_done;
DROP TABLE t_pending_read;

CREATE TABLE t_pending_read (a UInt64, c UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
    enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_pending_read VALUES (7, 8), (9, 10);

SYSTEM STOP MERGES t_pending_read;
ALTER TABLE t_pending_read MODIFY COLUMN a UInt32 SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE t_pending_read MODIFY COLUMN a UInt32 ALIAS c; -- { serverError BAD_ARGUMENTS }
SELECT type = 'UInt32', default_kind = '' FROM system.columns
WHERE database = currentDatabase() AND table = 't_pending_read' AND name = 'a';
SYSTEM START MERGES t_pending_read;

ALTER TABLE t_pending_read UPDATE c = c + 1 WHERE 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_pending_read MODIFY COLUMN a UInt32 ALIAS c;
ALTER TABLE t_pending_read UPDATE c = c + 1 WHERE 1 SETTINGS mutations_sync = 2;
SELECT a, c FROM t_pending_read ORDER BY c;
SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_pending_read' AND NOT is_done;
DROP TABLE t_pending_read;
