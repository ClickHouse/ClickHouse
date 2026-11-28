#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-catalog, no-shared-merge-tree

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A lagging replica must not deadlock on DROP_PART (patch) / MUTATE_PART / GET_PART (patch).

for r in 1 2; do
    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS t_r$r SYNC;
        CREATE TABLE t_r$r (part_key Int64, id Int64, val Int64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_patch_drop', 'r$r')
        PARTITION BY part_key ORDER BY (part_key, id)
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
            cleanup_delay_period = 1, max_cleanup_delay_period = 1, cleanup_delay_period_random_add = 0,
            old_parts_lifetime = 1, min_bytes_for_wide_part = 0"
done

$CLICKHOUSE_CLIENT --query "
    SET insert_keeper_fault_injection_probability = 0;
    INSERT INTO t_r1 VALUES (1, 1, 10), (2, 2, 20);
    SYSTEM SYNC REPLICA t_r2 STRICT;
    SYSTEM STOP REPLICATION QUEUES t_r2;
    UPDATE t_r1 SET val = 25 WHERE id = 2 SETTINGS enable_lightweight_update = 1;
    ALTER TABLE t_r1 APPLY PATCHES IN PARTITION 2 SETTINGS mutations_sync = 1;
    ALTER TABLE t_r1 UPDATE val = 15 WHERE part_key = 1 SETTINGS mutations_sync = 1;
"

# Wait until t_r1 drops the applied patch part and t_r2 pulls that DROP_PART into its stopped queue.
for _ in {1..120}; do
    pulled=$($CLICKHOUSE_CLIENT --query "SELECT count() > 0 FROM system.replication_queue WHERE database = currentDatabase() AND table = 't_r2' AND type = 'DROP_PART'")
    [[ $pulled == 1 ]] && break
    sleep 0.5
done
echo "$pulled"

$CLICKHOUSE_CLIENT --query "SYSTEM START REPLICATION QUEUES t_r2"
$CLICKHOUSE_CLIENT --receive_timeout 60 --query "SYSTEM SYNC REPLICA t_r2 STRICT"

$CLICKHOUSE_CLIENT --query "
    SELECT * FROM t_r2 ORDER BY part_key;
    SELECT count() FROM system.replication_queue WHERE database = currentDatabase() AND table = 't_r2';
    SYSTEM FLUSH LOGS part_log;
    -- partition 1 is mutated locally, partition 2 is fetched: t_r2 never had the patch
    SELECT partition_id, event_type FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_r2' AND error = 0 AND event_type IN ('MutatePart', 'DownloadPart')
        AND part_name IN (SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_r2' AND active)
    ORDER BY ALL;
    SELECT count() FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_r2' AND event_type = 'MutatePart' AND partition_id = '2';
    DROP TABLE t_r1 SYNC;
    DROP TABLE t_r2 SYNC;
"
