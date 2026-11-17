#!/usr/bin/env bash

# With `persist_ttl_merge_times`, `merge_with_ttl_timeout` must survive a table restart and regular merges.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_ttl_merge_timeout;
    CREATE TABLE t_ttl_merge_timeout (key UInt64, d DateTime)
    ENGINE = MergeTree ORDER BY key
    TTL d
    SETTINGS persist_ttl_merge_times = 1, merge_with_ttl_timeout = 86400, max_number_of_merges_with_ttl_in_pool = 100;
"

# One expired and one never-expiring row (so the part is rewritten, not dropped); wait until it is merged.
function insert_and_merge()
{
    local key=$1
    $CLICKHOUSE_CLIENT -q "INSERT INTO t_ttl_merge_timeout VALUES ($key, now() - 1000), ($key + 1, '2100-01-01 00:00:00')"
    $CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE t_ttl_merge_timeout SETTINGS optimize_throw_if_noop = 0"

    for _ in {1..600}; do
        local pending
        pending=$($CLICKHOUSE_CLIENT -q "
            SELECT (SELECT count() FROM t_ttl_merge_timeout WHERE d < now())
                + (SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 't_ttl_merge_timeout')")
        [ "$pending" -eq 0 ] && break
        sleep 0.1
    done
}

function ttl_delete_merges()
{
    $CLICKHOUSE_CLIENT -q "
        SYSTEM FLUSH LOGS part_log;
        SELECT '$1', count() FROM system.part_log
        WHERE database = currentDatabase() AND table = 't_ttl_merge_timeout'
            AND event_type = 'MergeParts' AND merge_reason = 'TTLDeleteMerge' AND error = 0;
    "
}

insert_and_merge 1
ttl_delete_merges 'first TTL merge'

# A new storage object starts with no TTL merge times in memory.
$CLICKHOUSE_CLIENT -q "DETACH TABLE t_ttl_merge_timeout"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE t_ttl_merge_timeout"

# The partition is still postponed, so the expired row is removed by a regular merge.
insert_and_merge 10
ttl_delete_merges 'after restart'

# The regular merge carried the time of the TTL merge over to the merged part.
$CLICKHOUSE_CLIENT -q "DETACH TABLE t_ttl_merge_timeout"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE t_ttl_merge_timeout"

insert_and_merge 20
ttl_delete_merges 'after regular merge and restart'

# The postponement is counted with the current value of the setting.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_ttl_merge_timeout MODIFY SETTING merge_with_ttl_timeout = 0"

insert_and_merge 30
ttl_delete_merges 'after timeout'

$CLICKHOUSE_CLIENT -q "SELECT key FROM t_ttl_merge_timeout ORDER BY key"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_ttl_merge_timeout"
