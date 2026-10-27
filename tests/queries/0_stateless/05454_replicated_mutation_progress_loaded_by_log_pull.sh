#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-parallel, no-replicated-database, no-shared-merge-tree
# Tag no-parallel: the failpoints are global.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A read of `system.mutations` reads the committing blocks of the mutations it lists, then pulls the
# replication log, which can load a new mutation. Those blocks say nothing about the new mutation, so
# its `progress` must stay NULL while an older INSERT has not published its part, on that read and later.
# Persistent `block-0000000000` nodes under block_numbers stand in for such INSERTs. The first mutation
# covers only partition 0 and the second only partition 1, so the blocks of partition 1 are not read
# before the second mutation is loaded.

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS t_loaded_by_pull SYNC;
    CREATE TABLE t_loaded_by_pull (p UInt8, k UInt64, v UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_loaded_by_pull', 'r1') PARTITION BY p ORDER BY k;
    SYSTEM STOP MERGES t_loaded_by_pull;
    INSERT INTO t_loaded_by_pull SELECT 0, number, 1 FROM numbers(1000);
    INSERT INTO t_loaded_by_pull SELECT 1, number, 2 FROM numbers(1000);
"

zk_path=$($CLICKHOUSE_CLIENT -q "SELECT zookeeper_path FROM system.replicas WHERE database = currentDatabase() AND table = 't_loaded_by_pull'")
$CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (name, path, value) VALUES ('block-0000000000', '$zk_path/block_numbers/0', '')"
$CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (name, path, value) VALUES ('block-0000000000', '$zk_path/block_numbers/1', '')"

# The older INSERT keeps the first mutation unknown, so every read checks it and pulls the log.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_loaded_by_pull UPDATE v = v + 1 IN PARTITION 0 WHERE 1 SETTINGS mutations_sync = 0"
loaded=""
for _ in {1..300}; do
    loaded=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_loaded_by_pull' AND parts_to_do = 1")
    [ "$loaded" = "1" ] && break
    sleep 0.2
done
[ "$loaded" = "1" ] || echo "FAIL: the first mutation was never loaded"

# The second mutation is in Keeper, but its loading stops before it reaches the queue.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT replicated_queue_pause_before_loading_mutations"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_loaded_by_pull UPDATE v = v + 1 IN PARTITION 1 WHERE 1 SETTINGS mutations_sync = 0"
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT replicated_queue_pause_before_loading_mutations PAUSE"

# The read lists only the first mutation and reads the committing blocks of partition 0. The second
# mutation is loaded before its log pull finishes, so it is in the result.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT rmt_mutations_status_pause_before_pulling_log"
echo "read that loads the second mutation"
$CLICKHOUSE_CLIENT -q "SELECT mutation_id, progress IS NULL FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_loaded_by_pull' ORDER BY mutation_id" &
reader=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT rmt_mutations_status_pause_before_pulling_log PAUSE"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT replicated_queue_pause_before_loading_mutations"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT rmt_mutations_status_pause_before_pulling_log"
wait $reader

# The second mutation was not remembered as having no older INSERT, so this read checks it.
echo "next read"
$CLICKHOUSE_CLIENT -q "SELECT mutation_id, parts_to_do, progress IS NULL FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_loaded_by_pull' ORDER BY mutation_id"

$CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE database = currentDatabase() AND table = 't_loaded_by_pull' SYNC FORMAT Null"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_loaded_by_pull SYNC"
