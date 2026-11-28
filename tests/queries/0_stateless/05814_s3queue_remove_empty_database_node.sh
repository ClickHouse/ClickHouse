#!/usr/bin/env bash
# Tags: no-fasttest, zookeeper
# no-fasttest: S3Queue is not built in the fast test.
# zookeeper: the queue keeps its metadata in Keeper.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Without `keeper_path` a queue keeps its metadata in `<s3queue_default_zookeeper_path>/<database UUID>/<table UUID>`.
# The `<database UUID>` node must be removed when the last queue of the database is dropped.

db="${CLICKHOUSE_DATABASE}_queues"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $db SYNC"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $db ENGINE = Atomic"

function create_queue()
{
    local table=$1
    local extra_settings=${2:-}
    $CLICKHOUSE_CLIENT -q "CREATE TABLE $db.$table (a UInt32)
        ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/data/', 'u', 'p', CSV)
        SETTINGS mode = 'unordered'$extra_settings"
}

function node_exists()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.zookeeper WHERE path = '$(dirname "$1")' AND name = '$(basename "$1")'"
}

create_queue q1
create_queue q2

keeper_path=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.s3_queue_settings WHERE database = '$db' AND table = 'q1' AND name = 'keeper_path'")
database_node=$(dirname "$keeper_path")

echo "two queues: $(node_exists "$database_node")"
$CLICKHOUSE_CLIENT -q "DROP TABLE $db.q1 SYNC"
echo "one queue: $(node_exists "$database_node")"
$CLICKHOUSE_CLIENT -q "DROP TABLE $db.q2 SYNC"
echo "no queues: $(node_exists "$database_node")"

# A queue created after the node was removed creates it again.
create_queue q3
echo "recreated: $(node_exists "$database_node")"
$CLICKHOUSE_CLIENT -q "DROP TABLE $db.q3 SYNC"
echo "dropped again: $(node_exists "$database_node")"

# With an explicit `keeper_path` the parent node belongs to the user and must be kept.
custom_parent="/clickhouse/test_s3queue_custom_parent/${CLICKHOUSE_DATABASE}"
create_queue q4 ", keeper_path = '$custom_parent/q4'"
$CLICKHOUSE_CLIENT -q "DROP TABLE $db.q4 SYNC"
echo "custom keeper_path, table node: $(node_exists "$custom_parent/q4")"
echo "custom keeper_path, parent node: $(node_exists "$custom_parent")"

$CLICKHOUSE_CLIENT -q "DROP DATABASE $db SYNC"
