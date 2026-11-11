-- Tags: no-replicated-database
-- Replicated databases reject mixing schema changes and data mutations in one `ALTER`.

DROP TABLE IF EXISTS t_ephemeral_clear;

CREATE TABLE t_ephemeral_clear (a UInt64, c UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
    enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_ephemeral_clear VALUES (7, 8), (9, 10);
ALTER TABLE t_ephemeral_clear ADD COLUMN b UInt64 DEFAULT 42;
ALTER TABLE t_ephemeral_clear MODIFY COLUMN a UInt64 EPHEMERAL;

SELECT part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_ephemeral_clear' AND active;
SELECT count(), sum(b), sum(c) FROM t_ephemeral_clear;

-- Materialize missing `b` before clearing the last stored physical column.
ALTER TABLE t_ephemeral_clear CLEAR COLUMN c; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_ephemeral_clear MATERIALIZE COLUMN b SETTINGS mutations_sync = 2;
ALTER TABLE t_ephemeral_clear
    RENAME COLUMN a TO a_old,
    CLEAR COLUMN c
SETTINGS mutations_sync = 2, alter_sync = 2;

SELECT count(), sum(b), sum(c) FROM t_ephemeral_clear;
SELECT arraySort(groupUniqArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_ephemeral_clear' AND active;

DROP TABLE t_ephemeral_clear;

CREATE TABLE t_ephemeral_clear (a UInt64, c UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_ephemeral_clear VALUES (7, 8), (9, 10);
ALTER TABLE t_ephemeral_clear ADD COLUMN b UInt64 DEFAULT 42;
ALTER TABLE t_ephemeral_clear MODIFY COLUMN a UInt64 EPHEMERAL;

SELECT part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_ephemeral_clear' AND active;
SELECT count(), sum(b), sum(c) FROM t_ephemeral_clear;

ALTER TABLE t_ephemeral_clear CLEAR COLUMN c; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_ephemeral_clear MATERIALIZE COLUMN b SETTINGS mutations_sync = 2;
ALTER TABLE t_ephemeral_clear
    RENAME COLUMN a TO a_old,
    CLEAR COLUMN c
SETTINGS mutations_sync = 2, alter_sync = 2;

SELECT count(), sum(b), sum(c) FROM t_ephemeral_clear;
SELECT arraySort(groupUniqArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_ephemeral_clear' AND active;

DROP TABLE t_ephemeral_clear;
