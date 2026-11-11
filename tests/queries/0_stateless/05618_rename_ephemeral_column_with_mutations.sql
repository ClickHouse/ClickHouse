-- Tags: no-replicated-database
-- Replicated databases reject mixing schema changes and data mutations in one `ALTER`.

DROP TABLE IF EXISTS t_ephemeral_mutations;

-- The `UPDATE` already produces missing physical `b`; the rename must not add a conflicting read.
CREATE TABLE t_ephemeral_mutations (a UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
    enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_ephemeral_mutations VALUES (7), (9);
ALTER TABLE t_ephemeral_mutations ADD COLUMN b UInt64 DEFAULT 42;
ALTER TABLE t_ephemeral_mutations MODIFY COLUMN a UInt64 EPHEMERAL;
ALTER TABLE t_ephemeral_mutations
    UPDATE b = b + 1 WHERE 1,
    RENAME COLUMN a TO a_old
SETTINGS mutations_sync = 2;

SELECT 'compact update', count(), sum(b) FROM t_ephemeral_mutations;
DROP TABLE t_ephemeral_mutations;

CREATE TABLE t_ephemeral_mutations (a UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_ephemeral_mutations VALUES (7), (9);
ALTER TABLE t_ephemeral_mutations ADD COLUMN b UInt64 DEFAULT 42;
ALTER TABLE t_ephemeral_mutations MODIFY COLUMN a UInt64 EPHEMERAL;
ALTER TABLE t_ephemeral_mutations
    UPDATE b = b + 1 WHERE 1,
    RENAME COLUMN a TO a_old
SETTINGS mutations_sync = 2;

SELECT 'wide update', count(), sum(b) FROM t_ephemeral_mutations;
DROP TABLE t_ephemeral_mutations;

-- Historical `a` supplies the source row count, but the `DELETE` must determine the output row count.
CREATE TABLE t_ephemeral_mutations (a UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
    enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_ephemeral_mutations VALUES (7), (9);
ALTER TABLE t_ephemeral_mutations ADD COLUMN b UInt64 DEFAULT 42;
ALTER TABLE t_ephemeral_mutations MODIFY COLUMN a UInt64 EPHEMERAL;
ALTER TABLE t_ephemeral_mutations
    DELETE WHERE b = 42,
    RENAME COLUMN a TO a_old
SETTINGS mutations_sync = 2;

SELECT 'compact delete', count(), sum(b) FROM t_ephemeral_mutations;
DROP TABLE t_ephemeral_mutations;

CREATE TABLE t_ephemeral_mutations (a UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    enable_block_number_column = 0, enable_block_offset_column = 0;

INSERT INTO t_ephemeral_mutations VALUES (7), (9);
ALTER TABLE t_ephemeral_mutations ADD COLUMN b UInt64 DEFAULT 42;
ALTER TABLE t_ephemeral_mutations MODIFY COLUMN a UInt64 EPHEMERAL;
ALTER TABLE t_ephemeral_mutations
    DELETE WHERE b = 42,
    RENAME COLUMN a TO a_old
SETTINGS mutations_sync = 2;

SELECT 'wide delete', count(), sum(b) FROM t_ephemeral_mutations;
DROP TABLE t_ephemeral_mutations;
