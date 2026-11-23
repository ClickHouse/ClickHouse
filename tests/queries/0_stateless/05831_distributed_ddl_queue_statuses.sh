#!/usr/bin/env bash
# Tags: no-parallel, zookeeper
# Tag no-parallel: the fixtures are system-global Keeper nodes under the shared distributed DDL queue path, so a concurrent reader of system.distributed_ddl_queue sees them too
# Tag zookeeper: the fixtures are planted directly in Keeper

# `system.distributed_ddl_queue` (documented in the DOCS_MD block of
# src/Storages/System/attachSystemTables.cpp) reports one row per (entry, host). The per-host
# status comes from the `active` and `finished` children of the entry node, and the status names
# are the enum of `getStatusEnumsAndValues` in src/Storages/System/StorageSystemDDLWorkerQueue.cpp:
# Inactive, Active, Finished, Removing, Unknown. A real ON CLUSTER query on a one-replica cluster
# finishes immediately, so only a planted entry shows the other states.
#
# `allow_zookeeper_write` is on for the stateless suite, and the DDL worker ignores every child of
# the queue that does not start with `query-`, so these entries are never scheduled, executed or
# cleaned up. This is a shell test because of the cleanup: an entry whose format version the server
# does not support makes every read of the table fail, so the fixtures have to go away on every exit
# path, including a cancellation or an unexpected error in the middle of the test.
#
# Every client call reads from /dev/null: `INSERT ... VALUES` with the rows inline still appends
# whatever stdin holds to the data block, so an inherited stdin makes the insert wait for more rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QUEUE="/clickhouse/task_queue/ddl"
ENTRIES="covgap-ddl-queue-inactive covgap-ddl-queue-mixed covgap-ddl-queue-removing covgap-ddl-queue-hostless"

cleanup()
{
    # 1. Make every fixture parsable again. Done first and unconditionally: a leftover entry with an
    #    unsupported format version fails every read of system.distributed_ddl_queue, not just this test.
    for entry in $ENTRIES
    do
        $CLICKHOUSE_CLIENT -q \
            "INSERT INTO system.zookeeper (name, path, value) VALUES ('$entry', '$QUEUE', 'version: 1\nquery: SELECT 1\nhosts: []\n')" \
            </dev/null >/dev/null 2>&1
    done
    # 2. Remove them. The system.zookeeper sink can create and set but not remove, so this needs
    #    keeper-client. The port is the one the server is connected to; the suite randomly enables
    #    Keeper auth, and then the nodes are not visible to an unauthenticated client, hence best effort.
    local conn
    conn=$($CLICKHOUSE_CLIENT -q "SELECT host, port FROM system.zookeeper_connection WHERE name = 'default' LIMIT 1" </dev/null 2>/dev/null)
    if [ -n "$conn" ]
    then
        local removals=""
        for entry in $ENTRIES
        do
            removals="$removals rmr '$QUEUE/$entry';"
        done
        # shellcheck disable=SC2086
        $CLICKHOUSE_BINARY keeper-client --host "$(echo "$conn" | cut -f1)" --port "$(echo "$conn" | cut -f2)" \
            --connection-timeout 3 --session-timeout 3 --operation-timeout 3 -q "$removals" </dev/null >/dev/null 2>&1
    fi
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (name, path, value) VALUES
    -- No status dir at all: both hosts of the entry are Inactive.
    ('covgap-ddl-queue-inactive', '$QUEUE', 'version: 1\nquery: SELECT 1\nhosts: [\'h1:9001\',\'h2:9002\']\n'),
    -- h1 is in \`finished\` and still in \`active\` (it is reported once, as Finished), h4 is in
    -- \`active\` without being in \`hosts\`, h3 is in \`hosts\` without having started.
    ('covgap-ddl-queue-mixed', '$QUEUE', 'version: 1\nquery: SELECT 1\nhosts: [\'h1:9001\',\'h3:9003\']\n'),
    ('h1:9001', '$QUEUE/covgap-ddl-queue-mixed/finished', '0\n'),
    ('h1:9001', '$QUEUE/covgap-ddl-queue-mixed/active', ''),
    ('h4:9004', '$QUEUE/covgap-ddl-queue-mixed/active', ''),
    -- \`finished\` is there and \`active\` is gone: that is how an entry being removed looks, and its
    -- hosts must not be reported as Inactive.
    ('covgap-ddl-queue-removing', '$QUEUE', 'version: 1\nquery: SELECT 1\nhosts: [\'h5:9005\']\n'),
    ('finished', '$QUEUE/covgap-ddl-queue-removing', ''),
    -- No host is known for this entry at all.
    ('covgap-ddl-queue-hostless', '$QUEUE', 'version: 1\nquery: SELECT 1\nhosts: []\n')" </dev/null

$CLICKHOUSE_CLIENT -q "SELECT entry, entry_version, host, port, status, exception_code
    FROM system.distributed_ddl_queue
    WHERE startsWith(entry, 'covgap-ddl-queue-')
    ORDER BY entry, host, port" </dev/null

# An entry whose format version this server does not support fails the whole read, not just its own rows.
$CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (name, path, value) VALUES
    ('covgap-ddl-queue-hostless', '$QUEUE', 'version: 99\nquery: SELECT 1\nhosts: []\n')" </dev/null
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.distributed_ddl_queue WHERE entry != ''" </dev/null 2>&1 \
    | grep -o -m1 'UNKNOWN_FORMAT_VERSION'

# Put the supported version back, so the table is readable again.
$CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (name, path, value) VALUES
    ('covgap-ddl-queue-hostless', '$QUEUE', 'version: 1\nquery: SELECT 1\nhosts: []\n')" </dev/null
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.distributed_ddl_queue WHERE startsWith(entry, 'covgap-ddl-queue-')" </dev/null
