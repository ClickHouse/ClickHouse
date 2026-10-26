-- Tags: zookeeper
-- A column renamed or dropped by an ALTER is dropped under the name the part stores it, so a column of that name
-- added again by the same ALTER starts empty in a wide part.

CREATE TABLE t_rename_drop_readd_1 (id UInt64, q UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/{table}', 'r1') ORDER BY id SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_rename_drop_readd_1 VALUES (1, 42);
ALTER TABLE t_rename_drop_readd_1 RENAME COLUMN q TO r, DROP COLUMN r, ADD COLUMN r UInt32 DEFAULT 7 SETTINGS alter_sync = 2;
SELECT id, r FROM t_rename_drop_readd_1;
DROP TABLE t_rename_drop_readd_1;
