#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The client's own timeout to wait for the server must follow `receive_timeout` changed by `SET`
# and by the query `SETTINGS` clause, not only the value from the command line.
# https://github.com/ClickHouse/ClickHouse/issues/4831
# A huge `interactive_delay` stops the server from sending progress packets while the query runs,
# so the client waits for the whole duration of `sleep`, as it does for e.g. a long `OPTIMIZE`.

CLIENT="$CLICKHOUSE_CLIENT --receive_timeout 1 --interactive_delay 1000000000"

echo "SET"
$CLIENT --query "SET receive_timeout = 60; SELECT sleep(3)"

echo "SETTINGS"
$CLIENT --query "SELECT sleep(3) SETTINGS receive_timeout = 60"

# The value from the `SETTINGS` clause applies only to its query, then the client waits for 1 second again.
echo "SETTINGS then default"
$CLIENT --query "SELECT sleep(3) SETTINGS receive_timeout = 60; SELECT sleep(3)" 2>&1 | grep -o -m1 "Timeout exceeded while receiving data from server"
