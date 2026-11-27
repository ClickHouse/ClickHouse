#!/usr/bin/env bash

# A part that is known to hold rows whose rows TTL computed to exactly the epoch (`has_epoch_timestamps`)
# must still be cleaned up by TTL merges with `ttl_only_drop_parts = 1`, which disables the row-delete
# TTL selector. The `TTLDrop` merge reads such a part instead of dropping it as a whole: the expired rows
# are removed and the epoch rows survive.
#
# The marker can also be stale: an ordinary `DELETE` copies the TTL infos without recalculating them, so
# the part keeps the marker after the epoch row is gone. Such a part must not be kept forever either.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for table in t_ttl_epoch_stale_marker t_ttl_epoch_real_marker
do
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS $table;
        CREATE TABLE $table (id UInt32, d DateTime('UTC')) ENGINE = MergeTree ORDER BY id TTL d
            SETTINGS ttl_only_drop_parts = 1, merge_with_ttl_timeout = 0, min_bytes_for_full_part_storage = 0;
        SYSTEM STOP TTL MERGES $table;
        INSERT INTO $table VALUES (1, '1970-01-01 00:00:00'), (2, '2000-01-01 00:00:00');
    "
done

$CLICKHOUSE_CLIENT -q "ALTER TABLE t_ttl_epoch_stale_marker DELETE WHERE id = 1 SETTINGS mutations_sync = 2"

# Wait until the TTL merges have removed the expired rows.
function wait_for_rows()
{
    local table=$1
    local expected=$2
    for _ in {1..600}
    do
        [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM $table")" == "$expected" ] && return
        sleep 0.1
    done
    echo "Timed out waiting for $expected rows in $table"
}

$CLICKHOUSE_CLIENT -q "SYSTEM START TTL MERGES t_ttl_epoch_stale_marker"
$CLICKHOUSE_CLIENT -q "SYSTEM START TTL MERGES t_ttl_epoch_real_marker"

wait_for_rows t_ttl_epoch_stale_marker 0
wait_for_rows t_ttl_epoch_real_marker 1

$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_ttl_epoch_stale_marker"
$CLICKHOUSE_CLIENT -q "SELECT id, d FROM t_ttl_epoch_real_marker ORDER BY id"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_ttl_epoch_stale_marker"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_ttl_epoch_real_marker"
