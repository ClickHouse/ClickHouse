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

for table in q1 q2; do
    $CLICKHOUSE_CLIENT -q "CREATE TABLE $db.$table (a UInt32)
        ENGINE = S3Queue('http://whatever-we-dont-care:9001/root/data/', 'u', 'p', CSV)
        SETTINGS mode = 'unordered'"
done

keeper_path=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.s3_queue_settings WHERE database = '$db' AND table = 'q1' AND name = 'keeper_path'")
database_node=$(dirname "$keeper_path")
database_node_parent=$(dirname "$database_node")
database_node_name=$(basename "$database_node")

function database_node_exists()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.zookeeper WHERE path = '$database_node_parent' AND name = '$database_node_name'"
}

echo "two queues: $(database_node_exists)"
$CLICKHOUSE_CLIENT -q "DROP TABLE $db.q1 SYNC"
echo "one queue: $(database_node_exists)"
$CLICKHOUSE_CLIENT -q "DROP TABLE $db.q2 SYNC"
echo "no queues: $(database_node_exists)"

$CLICKHOUSE_CLIENT -q "DROP DATABASE $db SYNC"
