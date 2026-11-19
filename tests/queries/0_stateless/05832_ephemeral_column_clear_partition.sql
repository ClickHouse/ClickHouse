DROP TABLE IF EXISTS t_ephemeral_clear_partition;

CREATE TABLE t_ephemeral_clear_partition (p UInt64, a UInt64, c UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000,
    enable_block_number_column = 0, enable_block_offset_column = 0,
    skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';

INSERT INTO t_ephemeral_clear_partition VALUES (0, 7, 8), (1, 9, 10);
ALTER TABLE t_ephemeral_clear_partition ADD COLUMN b UInt64 DEFAULT 42;
ALTER TABLE t_ephemeral_clear_partition MODIFY COLUMN a UInt64 EPHEMERAL;

-- Only partition 1 has stored `p`; partition 0 still needs its stored `c`.
ALTER TABLE t_ephemeral_clear_partition CLEAR COLUMN c IN PARTITION 1 SETTINGS mutations_sync = 2;
SELECT p, b, c FROM t_ephemeral_clear_partition ORDER BY p;
ALTER TABLE t_ephemeral_clear_partition CLEAR COLUMN c IN PARTITION 0; -- { serverError BAD_ARGUMENTS }
SELECT p, b, c FROM t_ephemeral_clear_partition ORDER BY p;

DROP TABLE t_ephemeral_clear_partition;

CREATE TABLE t_ephemeral_clear_partition (p UInt64, a UInt64, c UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    enable_block_number_column = 0, enable_block_offset_column = 0,
    skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';

INSERT INTO t_ephemeral_clear_partition VALUES (0, 7, 8), (1, 9, 10);
ALTER TABLE t_ephemeral_clear_partition ADD COLUMN b UInt64 DEFAULT 42;
ALTER TABLE t_ephemeral_clear_partition MODIFY COLUMN a UInt64 EPHEMERAL;

ALTER TABLE t_ephemeral_clear_partition CLEAR COLUMN c IN PARTITION 1 SETTINGS mutations_sync = 2;
SELECT p, b, c FROM t_ephemeral_clear_partition ORDER BY p;
ALTER TABLE t_ephemeral_clear_partition CLEAR COLUMN c IN PARTITION 0; -- { serverError BAD_ARGUMENTS }
SELECT p, b, c FROM t_ephemeral_clear_partition ORDER BY p;

DROP TABLE t_ephemeral_clear_partition;
