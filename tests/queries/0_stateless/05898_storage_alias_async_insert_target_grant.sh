#!/usr/bin/env bash
# An asynchronous insert into an `Alias` table checks the grant on the target table before the data is queued,
# instead of returning success and dropping the data in the background flush.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user_05898_${CLICKHOUSE_DATABASE}"
user_client="$CLICKHOUSE_CLIENT --user $user --password password"

# Prints the error code of a query, or OK.
function result()
{
    local out
    if out=$($user_client --send_logs_level=none "$@" 2>&1); then
        echo "OK"
    else
        echo "$out" | grep -oE '\(([A-Z_]+)\)' | head -n1
    fi
}

$CLICKHOUSE_CLIENT --multiline -q "
    DROP USER IF EXISTS $user;
    CREATE USER $user IDENTIFIED WITH plaintext_password BY 'password';
    CREATE TABLE target (a UInt32) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE alias ENGINE = Alias(currentDatabase(), 'target');
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.alias TO $user;
"

echo "--- no grant on the target table"
result -q "INSERT INTO alias SETTINGS async_insert = 0 VALUES (1)"
result -q "INSERT INTO alias SETTINGS async_insert = 1, wait_for_async_insert = 0 VALUES (2)"
result -q "INSERT INTO alias SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES (3)"

echo "--- with the grant"
$CLICKHOUSE_CLIENT -q "GRANT INSERT ON ${CLICKHOUSE_DATABASE}.target TO $user"
result -q "INSERT INTO alias SETTINGS async_insert = 0 VALUES (4)"
result -q "INSERT INTO alias SETTINGS async_insert = 1, wait_for_async_insert = 0 VALUES (5)"
result -q "INSERT INTO alias SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES (6)"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH ASYNC INSERT QUEUE"
$CLICKHOUSE_CLIENT -q "SELECT * FROM target ORDER BY a"

$CLICKHOUSE_CLIENT -q "DROP USER $user"
