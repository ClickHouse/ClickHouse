#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database
# Tag no-parallel, no-fasttest: uses failpoints, which affect the whole server.
# Tag no-replicated-database: the durable metadata commit lives in ZooKeeper there and follows a different path.

# `StorageMergeTree::mutate` must hold the `ALTER` lock until the mutation entry is written, not only check it.
# `persist_mutation_author` gates the format of `mutation_*.txt`, and a local `ALTER TABLE ... MODIFY SETTING`
# applies it in memory before the durable metadata write and rolls it back when that write throws. A mutation
# that passed the lock check before the `ALTER` started must therefore not observe the transient value, or a
# failed enabling `ALTER` would still leave a `format version: 2` entry on disk.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

TABLE=t_mutation_holds_alter_lock
ALTER_QUERY_ID="alter_settings_${CLICKHOUSE_DATABASE}_$RANDOM$RANDOM"
ALTER_OUTPUT="${CLICKHOUSE_TMP}/${ALTER_QUERY_ID}.out"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT mt_mutate_pause_after_alter_lock" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT alter_settings_pause_before_metadata_write" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT alter_settings_throw_before_metadata_write" 2>/dev/null || true
    wait || true
    rm -f "$ALTER_OUTPUT"
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC" 2>/dev/null || true
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC"
$CLICKHOUSE_CLIENT --query "CREATE TABLE $TABLE (id UInt64, value String) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT --query "INSERT INTO $TABLE VALUES (1, 'a')"

# The mutation passes the `ALTER` lock check and parks before it creates its entry.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT mt_mutate_pause_after_alter_lock"
$CLICKHOUSE_CLIENT --query "ALTER TABLE $TABLE UPDATE value = 'b' WHERE id = 1 SETTINGS mutations_sync = 1" &
MUTATION_PID=$!
timeout 60 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT mt_mutate_pause_after_alter_lock PAUSE"

# The settings `ALTER` starts now. If the mutation did not hold the lock, it would apply the setting in memory
# and park right before its durable metadata write; with the lock held it waits for the mutation instead.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT alter_settings_pause_before_metadata_write"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT alter_settings_throw_before_metadata_write"
$CLICKHOUSE_CLIENT --query-id="$ALTER_QUERY_ID" --query "ALTER TABLE $TABLE MODIFY SETTING persist_mutation_author = 1 SETTINGS lock_acquire_timeout = 120" > "$ALTER_OUTPUT" 2>&1 &
ALTER_PID=$!

for _ in {1..600}
do
    if [[ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$ALTER_QUERY_ID'")" -gt 0 ]]
    then
        break
    fi
    sleep 0.1
done

# Resume the mutation: it creates its entry and releases the lock, which lets the `ALTER` proceed.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT mt_mutate_pause_after_alter_lock"
wait "$MUTATION_PID"

# The `ALTER` reaches the metadata write, hits the injected failure and rolls the setting back.
timeout 60 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT alter_settings_pause_before_metadata_write PAUSE"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT alter_settings_pause_before_metadata_write"
wait "$ALTER_PID" || true
grep -oF 'FAULT_INJECTED' "$ALTER_OUTPUT" | head -n 1

$CLICKHOUSE_CLIENT --query "SELECT 'author is empty', author = '' FROM system.mutations WHERE database = currentDatabase() AND table = '$TABLE'"
$CLICKHOUSE_CLIENT --query "SELECT 'mutated value', value FROM $TABLE"
$CLICKHOUSE_CLIENT --query "SELECT 'setting persisted', engine_full LIKE '%persist_mutation_author%' FROM system.tables WHERE database = currentDatabase() AND name = '$TABLE'"
