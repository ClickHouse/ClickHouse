#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-replicated-database, no-shared-merge-tree
# Tag no-fasttest: needs the `s3_no_cache` storage policy

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A mutation hardlinks the files of the columns it does not touch, so the mutated part shares blobs with its parent.
# In the compatible mode of zero-copy replication a part is locked under two ZooKeeper paths, but the names of the
# hardlinked files are written only under the first one. The replica that fetched the mutated part has no local
# reference counter to protect them, and when it removed the parent part last, it read the names from the second
# path, found none, and deleted the blobs that the mutated part still uses.

T="t_zero_copy_compat"
SETTINGS="storage_policy = 's3_no_cache', allow_remote_fs_zero_copy_replication = 1, remote_fs_zero_copy_path_compatible_mode = 1,
    min_bytes_for_wide_part = 0, old_parts_lifetime = 3600, cleanup_delay_period = 1, max_cleanup_delay_period = 1, cleanup_delay_period_random_add = 0"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS ${T}_1 SYNC;
    DROP TABLE IF EXISTS ${T}_2 SYNC;
    CREATE TABLE ${T}_1 (k UInt64, a UInt64, b String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/$T', '1') ORDER BY k SETTINGS $SETTINGS;
    CREATE TABLE ${T}_2 (k UInt64, a UInt64, b String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/$T', '2') ORDER BY k SETTINGS $SETTINGS;
    INSERT INTO ${T}_1 SELECT number, number, toString(number) FROM numbers(1000);
    SYSTEM SYNC REPLICA ${T}_2;
    ALTER TABLE ${T}_1 UPDATE a = a + 1 WHERE 1 SETTINGS mutations_sync = 2;
    SYSTEM SYNC REPLICA ${T}_2;
    SYSTEM FLUSH LOGS part_log;
"

# One replica executes the mutation, the other one fetches the mutated part. Make the fetching replica remove the parent part last.
fetched=$($CLICKHOUSE_CLIENT -q "SELECT table FROM system.part_log WHERE database = currentDatabase() AND table LIKE '${T}_%' AND part_name = 'all_0_0_0_1' AND event_type = 'DownloadPart'")
if [ "$fetched" = "${T}_1" ]; then order="${T}_2 ${T}_1"; else order="${T}_1 ${T}_2"; fi

for table in $order
do
    $CLICKHOUSE_CLIENT -q "ALTER TABLE $table MODIFY SETTING old_parts_lifetime = 1"
    for _ in $(seq 1 240)
    do
        inactive=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '$table' AND NOT active")
        [ "$inactive" = "0" ] && break
        sleep 0.5
    done
done

# Column `b` was hardlinked by the mutation, its blobs must still exist and be readable on both replicas
for i in 1 2
do
    $CLICKHOUSE_CLIENT -q "SELECT count(), sum(a), sum(length(b)) FROM ${T}_$i" 2>&1 | grep -oE '^[0-9]+\s[0-9]+\s[0-9]+|Code: [0-9]+' | head -n 1
done

$CLICKHOUSE_CLIENT -q "
    DROP TABLE ${T}_1 SYNC;
    DROP TABLE ${T}_2 SYNC;
"
