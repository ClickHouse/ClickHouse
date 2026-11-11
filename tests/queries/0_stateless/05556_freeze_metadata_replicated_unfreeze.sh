#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-replicated-database, no-shared-merge-tree
# Tag zookeeper: only ReplicatedMergeTree stores freeze metadata next to a frozen part
# Tag no-fasttest: the s3_disk storage policy is installed only with EXPORT_S3_STORAGE_POLICIES=1, which --fast-test turns off
# Tag no-replicated-database: an explicit ReplicatedMergeTree path and replica name are not accepted inside a Replicated database
# Tag no-shared-merge-tree: FreezeMetaData exists only for ReplicatedMergeTree on a zero-copy disk; the private runner rewrites the engine to SharedMergeTree, which refuses the local-metadata disk s3_disk

# ALTER TABLE ... FREEZE of a ReplicatedMergeTree part on a disk that supports zero-copy
# replication stores a freeze-metadata file next to the frozen part, and ALTER TABLE ... UNFREEZE
# parses that file back before it drops the frozen part and keeps the blobs that the working set
# still references (FreezeMetaData::save / FreezeMetaData::load, src/Storages/Freeze.cpp).
# Expected behaviour per docs/reference/statements/alter/partition.mdx: UNFREEZE PARTITION
# "removes frozen partitions with the specified name from the disk", FREEZE PARTITION creates
# a local backup of a partition.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BACKUP_ONE="05556_one_${CLICKHOUSE_DATABASE}"
BACKUP_TWO="05556_two_${CLICKHOUSE_DATABASE}"

FREEZE_STRUCTURE="command_type String, partition_id String, part_name String, backup_name String, backup_path String, part_backup_path String"

# Only the snapshot directories this test created are inspected, so the cost does not depend on
# what else lives on the shared disk.
function freeze_part_path()
{
    ${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_frozen_meta FREEZE WITH NAME '$1' FORMAT TSV SETTINGS alter_partition_verbose_result = 1" \
        | ${CLICKHOUSE_LOCAL} --structure "$FREEZE_STRUCTURE" --query "SELECT part_backup_path FROM table"
}

# For every frozen part: does its directory still exist, and does it still carry the freeze metadata?
function snapshot_state()
{
    local line="$1"
    shift
    local path
    for path in "$@"; do
        if [ -d "$path" ]; then line="$line	1"; else line="$line	0"; fi
        if [ -f "$path/frozen_metadata.txt" ]; then line="$line	1"; else line="$line	0"; fi
    done
    echo "$line"
}

${CLICKHOUSE_CLIENT} -q "
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
"

# two independent snapshots of the same part
PART_ONE=$(freeze_part_path "$BACKUP_ONE")
PART_TWO=$(freeze_part_path "$BACKUP_TWO")

snapshot_state "frozen" "$PART_ONE" "$PART_TWO"

# reads and accepts the freeze metadata of the frozen part, then removes the frozen part
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_frozen_meta UNFREEZE WITH NAME '$BACKUP_ONE'"

snapshot_state "one unfrozen" "$PART_ONE" "$PART_TWO"

# the data the working set shares with the frozen snapshot is still readable
${CLICKHOUSE_CLIENT} -q "SELECT 'data', count(), sum(id) FROM t_frozen_meta"

${CLICKHOUSE_CLIENT} -q "
    ALTER TABLE t_frozen_meta UNFREEZE WITH NAME '$BACKUP_TWO';
    -- a backup name that was never frozen is a no-op, not an error
    ALTER TABLE t_frozen_meta UNFREEZE WITH NAME '05556_absent_${CLICKHOUSE_DATABASE}';
"

snapshot_state "both unfrozen" "$PART_ONE" "$PART_TWO"

${CLICKHOUSE_CLIENT} -q "
    SELECT 'data', count(), sum(id) FROM t_frozen_meta;
    DROP TABLE t_frozen_meta SYNC;
"
