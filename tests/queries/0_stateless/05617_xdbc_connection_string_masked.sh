#!/usr/bin/env bash
# Tags: no-asan, no-msan, no-fasttest, no-cpu-aarch64
# Tag no-msan: can't pass because odbc libraries are not instrumented
# Tag no-cpu-aarch64: clickhouse-odbc is not setup for arm

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A bridge request carries the ODBC connection string in the `connection_string` parameter of its URI.
# Wherever that URI is shown, the value must be shown as [HIDDEN], as it is in the query text.

SECRET="xdbcpw${CLICKHOUSE_TEST_UNIQUE_NAME}"
SHAPE="connection_string=[HIDDEN]"

# Reads text from stdin: the secret must be absent and the masked parameter present, so a text that never
# carried the URI fails instead of passing.
assert_shape() {
    local label="$1" secret="$2" shape="$3" text
    text=$(cat)
    if echo "$text" | grep -qF "$secret"; then
        echo "$label: FAIL cleartext"
    elif echo "$text" | grep -qF "$shape"; then
        echo "$label: OK masked"
    else
        echo "$label: FAIL uri absent"
    fi
}

# The bridge is started on first use.
for _ in $(seq 1 10); do
    ${CLICKHOUSE_CLIENT} --query "SELECT * FROM odbc('DSN=no_such_dsn', 'db', 't')" 2>&1 | grep -qF 'Received error from remote server' && break
    sleep 0.1
done

# The bridge cannot connect to the DSN and answers with an error, which quotes the request URI. Grep that
# line: the client also echoes the submitted query, which legitimately contains what the user typed.
${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_tf" --query "SELECT * FROM odbc('DSN=no_such_dsn;PWD=${SECRET}', 'db', 't')" 2>&1 \
    | grep -F 'Received error from remote server' | assert_shape "odbc_table_function" "$SECRET" "$SHAPE"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT exception
    FROM system.query_log
    WHERE event_date >= yesterday()
      AND current_database = currentDatabase()
      AND query_id = '${CLICKHOUSE_TEST_UNIQUE_NAME}_tf'
      AND type != 'QueryStart'" | assert_shape "query_log" "$SECRET" "$SHAPE"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_odbc (a Int32) ENGINE = ODBC('DSN=no_such_dsn;PWD=${SECRET}', 'db', 't')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_odbc VALUES (1)" 2>&1 \
    | grep -F 'Received error from remote server' | assert_shape "odbc_engine_insert" "$SECRET" "$SHAPE"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_odbc"
