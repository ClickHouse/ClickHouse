#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-fasttest, no-random-settings
# no-parallel: `ddl_worker_quote_entry_query` fails every distributed DDL task on this server.
# no-fasttest: the failpoint is compiled out without libfiu.
# no-random-settings: the initiator must throw, and `log_queries` must record that exception.

# An older worker on a mixed-version `ON CLUSTER` target rejects `CREATE CLUSTER` and quotes
# `secret` in its syntax error. The initiator used to copy that text into the exception, which
# `system.errors` and `system.query_log` then store with no `displaySecretsInShowAndSelect` check.
# `05480` only rewrites `system.distributed_ddl_queue` after the query has already finished, so it
# never builds this exception. `ALTER CLUSTER` uses the same status source.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CANARY="c05575ddlclustersecret"
CLUSTER="c05575_${CLICKHOUSE_DATABASE}"
READER="u05575_reader_${CLICKHOUSE_DATABASE}"
QUERY_ID="${CLICKHOUSE_DATABASE}_ddl_cluster_worker_secret"
LOG="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.create.log"

cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ddl_worker_quote_entry_query" >/dev/null 2>&1 || true
    $CLICKHOUSE_CLIENT --query "DROP CLUSTER IF EXISTS ${CLUSTER}" >/dev/null 2>&1 || true
    $CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${READER}" >/dev/null 2>&1 || true
    rm -f "$LOG"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${READER}"
$CLICKHOUSE_CLIENT --query "CREATE USER ${READER} IDENTIFIED WITH plaintext_password BY 'p'"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON system.query_log TO ${READER}"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON system.errors TO ${READER}"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON system.text_log TO ${READER}"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT ddl_worker_quote_entry_query"

# `send_logs_level=none` keeps the worker's own log line out of this client. The exception text
# is still what a caller sees, and it must already be redacted.
if $CLICKHOUSE_CLIENT --send_logs_level=none --log_queries=1 --distributed_ddl_output_mode=throw \
    --query_id "$QUERY_ID" --query "
        CREATE CLUSTER ${CLUSTER} ON CLUSTER test_shard_localhost
            (secret = '${CANARY}', SHARD (host = '127.0.0.1', port = 9000))
    " >"$LOG" 2>&1
then
    echo "CREATE CLUSTER unexpectedly succeeded" >&2
    exit 1
fi

if ! grep -F 'There was an error on' "$LOG" | grep -F '[HIDDEN]' >/dev/null; then
    echo "initiator exception was not redacted" >&2
    cat "$LOG" >&2
    exit 1
fi
if grep -F 'There was an error on' "$LOG" | grep -F "$CANARY" >/dev/null; then
    echo "initiator exception leaked the secret" >&2
    exit 1
fi

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT ddl_worker_quote_entry_query"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log, text_log"

# The reader has `SELECT` on the diagnostic tables and no `displaySecretsInShowAndSelect`.
$CLICKHOUSE_CLIENT --user "$READER" --password p --query "
    SELECT 'query_log',
        countIf(position(concat(exception, query, formatted_query, stack_trace), '${CANARY}') > 0),
        countIf(position(exception, '[HIDDEN]') > 0 AND exception_code = 62)
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '${QUERY_ID}' AND type = 'ExceptionWhileProcessing' AND event_date >= yesterday()
    SETTINGS enable_parallel_replicas = 0
"
$CLICKHOUSE_CLIENT --user "$READER" --password p --query "
    SELECT 'errors',
        countIf(position(last_error_message, '${CANARY}') > 0 OR position(last_error_format_string, '${CANARY}') > 0),
        countIf(position(last_error_message, '[HIDDEN]') > 0)
    FROM system.errors
    WHERE query_id = '${QUERY_ID}' AND name = 'SYNTAX_ERROR'
    SETTINGS enable_parallel_replicas = 0
"
$CLICKHOUSE_CLIENT --user "$READER" --password p --query "
    SELECT 'text_log',
        countIf(position(message, '${CANARY}') > 0),
        countIf(logger_name = 'executeQuery' AND position(message, '[HIDDEN]') > 0) > 0
    FROM system.text_log
    WHERE query_id = '${QUERY_ID}' AND event_date >= yesterday() AND event_time >= now() - 600
    SETTINGS enable_parallel_replicas = 0
"
