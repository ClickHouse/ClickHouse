DROP TABLE IF EXISTS t_pending_subcolumn;

CREATE TABLE t_pending_subcolumn (a Tuple(x UInt64), c UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
    enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_pending_subcolumn VALUES ((7), 8), ((9), 10);

SYSTEM STOP MERGES t_pending_subcolumn;
ALTER TABLE t_pending_subcolumn UPDATE c = a.x WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_pending_subcolumn MODIFY COLUMN a Tuple(x UInt64) EPHEMERAL; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_pending_subcolumn MODIFY COLUMN a Tuple(x UInt64) ALIAS tuple(c); -- { serverError BAD_ARGUMENTS }
SYSTEM START MERGES t_pending_subcolumn;
ALTER TABLE t_pending_subcolumn UPDATE c = c WHERE 1 SETTINGS mutations_sync = 2;
SELECT c FROM t_pending_subcolumn ORDER BY c;

SYSTEM STOP MERGES t_pending_subcolumn;
ALTER TABLE t_pending_subcolumn UPDATE c = c + 1 WHERE a.x > 7 SETTINGS mutations_sync = 0;
ALTER TABLE t_pending_subcolumn MODIFY COLUMN a Tuple(x UInt64) EPHEMERAL; -- { serverError BAD_ARGUMENTS }
SYSTEM START MERGES t_pending_subcolumn;
ALTER TABLE t_pending_subcolumn UPDATE c = c WHERE 1 SETTINGS mutations_sync = 2;
SELECT c FROM t_pending_subcolumn ORDER BY c;
ALTER TABLE t_pending_subcolumn MODIFY COLUMN a Tuple(x UInt64) EPHEMERAL;
DROP TABLE t_pending_subcolumn;

-- Resolve the actual storage column rather than taking the first component of a dotted name.
CREATE TABLE t_pending_subcolumn (a UInt64, `a.b` Tuple(x UInt64), c UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
    enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_pending_subcolumn VALUES (1, (7), 8), (2, (9), 10);
SYSTEM STOP MERGES t_pending_subcolumn;
ALTER TABLE t_pending_subcolumn UPDATE c = `a.b`.x WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_pending_subcolumn MODIFY COLUMN a UInt64 EPHEMERAL;
ALTER TABLE t_pending_subcolumn MODIFY COLUMN `a.b` Tuple(x UInt64) EPHEMERAL; -- { serverError BAD_ARGUMENTS }
SYSTEM START MERGES t_pending_subcolumn;
ALTER TABLE t_pending_subcolumn UPDATE c = c WHERE 1 SETTINGS mutations_sync = 2;
SELECT c FROM t_pending_subcolumn ORDER BY c;
DROP TABLE t_pending_subcolumn;
