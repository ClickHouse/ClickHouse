#!/usr/bin/env bash
# Tags: zookeeper

# `system.distributed_ddl_queue` must hide the credentials of a statement even when the reader lowers
# `max_query_size` below the length of the stored entry. The entry used to fail to parse then, and the
# raw text with the password was shown instead.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CANARY="c05457ddlqueuesecret"
TARGET_USER="u05457_target_$CLICKHOUSE_DATABASE"
READER_USER="u05457_reader_$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $TARGET_USER, $READER_USER"
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "CREATE USER $TARGET_USER ON CLUSTER test_shard_localhost IDENTIFIED WITH plaintext_password BY '$CANARY'"
$CLICKHOUSE_CLIENT -q "CREATE USER $READER_USER IDENTIFIED WITH plaintext_password BY 'p'"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON system.distributed_ddl_queue TO $READER_USER"

# The reading query itself has to fit into `max_query_size`, so filter on this side.
QUEUE=$($CLICKHOUSE_CLIENT --user "$READER_USER" --password p --max_query_size 64 -q "SELECT query FROM system.distributed_ddl_queue")
echo "$QUEUE" | grep -c "$TARGET_USER" | sed 's/^[1-9][0-9]*$/entry is shown/'
echo "$QUEUE" | grep -c "$CANARY"

$CLICKHOUSE_CLIENT -q "DROP USER $TARGET_USER, $READER_USER"
