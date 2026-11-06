-- Tags: zookeeper, no-fasttest, no-replicated-database
-- Tag zookeeper: only ReplicatedMergeTree stores freeze metadata next to a frozen part
-- Tag no-fasttest: the s3_disk storage policy is installed only with EXPORT_S3_STORAGE_POLICIES=1, which --fast-test turns off
-- Tag no-replicated-database: an explicit ReplicatedMergeTree path and replica name are not accepted inside a Replicated database

-- ALTER TABLE ... FREEZE of a ReplicatedMergeTree part on a disk that supports zero-copy
-- replication stores a freeze-metadata file next to the frozen part, and ALTER TABLE ... UNFREEZE
-- parses that file back before it drops the frozen part and keeps the blobs that the working set
-- still references (FreezeMetaData::save / FreezeMetaData::load, src/Storages/Freeze.cpp).
-- Expected behaviour per docs/reference/statements/alter/partition.mdx: UNFREEZE PARTITION
-- "removes frozen partitions with the specified name from the disk", FREEZE PARTITION creates
-- a local backup of a partition.

DROP TABLE IF EXISTS t_frozen_meta SYNC;

CREATE TABLE t_frozen_meta (id UInt64, s String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_frozen_meta', 'r1')
ORDER BY id
SETTINGS disk = 's3_disk',
         -- an object storage disk supports zero-copy replication, where FREEZE is refused by default
         disable_freeze_partition_for_zero_copy_replication = 0,
         -- a wide part in full storage, so the frozen snapshot hardlinks the part's files one by one
         min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0;

INSERT INTO t_frozen_meta
SETTINGS async_insert = 0, insert_keeper_fault_injection_probability = 0
VALUES (1, 'a'), (2, 'b');

-- two independent snapshots of the same part
ALTER TABLE t_frozen_meta FREEZE WITH NAME '05556_one';
ALTER TABLE t_frozen_meta FREEZE WITH NAME '05556_two';

-- the table uuid keeps the counts below exact when copies of this test run at the same time:
-- the shadow directory is global to the disk, the path of a frozen part is not
WITH (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 't_frozen_meta') AS t
SELECT 'frozen', countIf(local_path LIKE 'shadow/05556_one/%') > 0, countIf(local_path LIKE 'shadow/05556_two/%') > 0
FROM system.remote_data_paths
WHERE disk_name = 's3_disk' AND local_path LIKE '%/' || t || '/%'
SETTINGS traverse_shadow_remote_data_paths = 1;

-- reads and accepts the freeze metadata of the frozen part, then removes the frozen part
ALTER TABLE t_frozen_meta UNFREEZE WITH NAME '05556_one';

WITH (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 't_frozen_meta') AS t
SELECT 'one unfrozen', countIf(local_path LIKE 'shadow/05556_one/%'), countIf(local_path LIKE 'shadow/05556_two/%') > 0
FROM system.remote_data_paths
WHERE disk_name = 's3_disk' AND local_path LIKE '%/' || t || '/%'
SETTINGS traverse_shadow_remote_data_paths = 1;

-- the data the working set shares with the frozen snapshot is still readable
SELECT 'data', count(), sum(id) FROM t_frozen_meta;

ALTER TABLE t_frozen_meta UNFREEZE WITH NAME '05556_two';
-- a backup name that was never frozen is a no-op, not an error
ALTER TABLE t_frozen_meta UNFREEZE WITH NAME '05556_absent';

WITH (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 't_frozen_meta') AS t
SELECT 'both unfrozen', count()
FROM system.remote_data_paths
WHERE disk_name = 's3_disk' AND local_path LIKE 'shadow/%' AND local_path LIKE '%/' || t || '/%'
SETTINGS traverse_shadow_remote_data_paths = 1;

SELECT 'data', count(), sum(id) FROM t_frozen_meta;

DROP TABLE t_frozen_meta SYNC;
