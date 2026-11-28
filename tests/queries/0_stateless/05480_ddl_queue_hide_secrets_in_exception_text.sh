#!/usr/bin/env bash
# Tags: zookeeper

# `system.distributed_ddl_queue` hides the credentials of a statement in `query`, and it must hide them in
# `exception_text` as well: a host that fails to parse the entry (for example, an older server during a
# rolling upgrade) quotes the text of the query in its syntax error. Simulate such a host by overwriting
# the status of the entry with a failure message that contains the secret.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CANARY="c05480ddlqueuesecret"
TARGET_USER="u05480_target_$CLICKHOUSE_DATABASE"
READER_USER="u05480_reader_$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $TARGET_USER, $READER_USER"
$CLICKHOUSE_CLIENT -q "CREATE USER $TARGET_USER ON CLUSTER test_shard_localhost IDENTIFIED WITH plaintext_password BY '$CANARY'" > /dev/null
$CLICKHOUSE_CLIENT -q "CREATE USER $READER_USER IDENTIFIED WITH plaintext_password BY 'p'"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.distributed_ddl_queue TO $READER_USER"

ENTRY=$($CLICKHOUSE_CLIENT -q "SELECT DISTINCT entry FROM system.distributed_ddl_queue WHERE query LIKE '%$TARGET_USER%'")
QUEUE_PATH=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.zookeeper WHERE path IN ('/clickhouse/task_queue/ddl', '/clickhouse/task_queue/ddl/') AND name = '$ENTRY'" | head -n1)
FINISHED_PATH="${QUEUE_PATH%/}/$ENTRY/finished"
HOST=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.zookeeper WHERE path = '$FINISHED_PATH' LIMIT 1")

$CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (path, name, value) VALUES ('$FINISHED_PATH', '$HOST', '62\nSyntax error: failed at position 1 (IDENTIFIED WITH plaintext_password BY $CANARY)')"

# A reader who may not see secrets gets the code, but not the text.
$CLICKHOUSE_CLIENT --user "$READER_USER" --password p -q "SELECT exception_code, exception_text FROM system.distributed_ddl_queue WHERE entry = '$ENTRY'"

$CLICKHOUSE_CLIENT -q "DROP USER $TARGET_USER, $READER_USER"
