#!/usr/bin/env bash

# `merge_with_ttl_timeout` postpones the next TTL merge of a partition. With `persist_ttl_merge_times`, the
# postponement must survive a restart of the table, and the regular merges of the partition, because the time of the
# last TTL merge is stored in `ttl.txt` of the parts. Without it, the time is kept only in memory, and after a restart
# every partition gets a TTL merge at once.

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

# Insert one expired row and one row that never expires, so the TTL merge rewrites the part instead of dropping it.
# A background merge may pick the part before `OPTIMIZE`, so wait until the expired row is gone and no merge runs.
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
