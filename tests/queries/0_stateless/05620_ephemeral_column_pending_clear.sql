CREATE TABLE t_pending_clear (a UInt64, c UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
    enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_pending_clear VALUES (7, 8), (9, 10);
ALTER TABLE t_pending_clear ADD COLUMN b UInt64 DEFAULT 42;
SELECT part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_pending_clear' AND active;

SYSTEM STOP MERGES t_pending_clear;
ALTER TABLE t_pending_clear CLEAR COLUMN c SETTINGS alter_sync = 0, mutations_sync = 0;
-- Pending `CLEAR COLUMN c` removes stored `c`, so it cannot preserve rows after converting `a`.
ALTER TABLE t_pending_clear MODIFY COLUMN a UInt64 EPHEMERAL; -- { serverError BAD_ARGUMENTS }
SELECT default_kind = '' FROM system.columns
WHERE database = currentDatabase() AND table = 't_pending_clear' AND name = 'a';
SYSTEM START MERGES t_pending_clear;

ALTER TABLE t_pending_clear MATERIALIZE COLUMN b SETTINGS mutations_sync = 2;
ALTER TABLE t_pending_clear MODIFY COLUMN a UInt64 EPHEMERAL;
SELECT count(), sum(b), sum(c) FROM t_pending_clear;
SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_pending_clear' AND NOT is_done;
DROP TABLE t_pending_clear;

CREATE TABLE t_pending_clear (a UInt64, c UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_pending_clear VALUES (7, 8), (9, 10);
ALTER TABLE t_pending_clear ADD COLUMN b UInt64 DEFAULT 42;
SELECT part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_pending_clear' AND active;

SYSTEM STOP MERGES t_pending_clear;
ALTER TABLE t_pending_clear CLEAR COLUMN c SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE t_pending_clear MODIFY COLUMN a UInt64 EPHEMERAL; -- { serverError BAD_ARGUMENTS }
SELECT default_kind = '' FROM system.columns
WHERE database = currentDatabase() AND table = 't_pending_clear' AND name = 'a';
SYSTEM START MERGES t_pending_clear;

ALTER TABLE t_pending_clear MATERIALIZE COLUMN b SETTINGS mutations_sync = 2;
ALTER TABLE t_pending_clear MODIFY COLUMN a UInt64 EPHEMERAL;
SELECT count(), sum(b), sum(c) FROM t_pending_clear;
SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_pending_clear' AND NOT is_done;
DROP TABLE t_pending_clear;
