#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Tag no-parallel: the failpoint is global for the server
# Tag no-fasttest: tests that run alone are kept out of the fast test

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT sleep_on_receive_external_table_data"
}
trap cleanup EXIT

# The failpoint delays every block of an external table by a second.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT sleep_on_receive_external_table_data"

python3 "$CUR_DIR"/05877_external_tables_max_execution_time_query_settings.python
