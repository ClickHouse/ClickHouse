#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Tag no-parallel: the failpoint is global for the server
# Tag no-fasttest: tests that run alone are kept out of the fast test

# The server writes all the blocks of an external table with one pipeline,
# and the time it spends receiving external tables is limited by `max_execution_time`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BIG_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_big.tsv"
SMALL_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_small.tsv"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT sleep_on_receive_external_table_data"
    rm -f "${BIG_FILE:?}" "${SMALL_FILE:?}"
}
trap cleanup EXIT

# The big table takes several blocks, the small ones take one block each.
seq 1 200000 > "$BIG_FILE"
echo 1 > "$SMALL_FILE"

$CLICKHOUSE_CLIENT \
    --external --file="$BIG_FILE" --name=big --structure='x UInt64' \
    --external --file="$SMALL_FILE" --name=small1 --structure='x UInt64' \
    --external --file="$SMALL_FILE" --name=small2 --structure='x UInt64' \
    -q "SELECT count(), sum(x) FROM (SELECT x FROM big UNION ALL SELECT x FROM small1 UNION ALL SELECT x FROM small2)"

# The failpoint delays every block of an external table by a second, and every table is at least one block,
# so receiving four tables takes longer than `max_execution_time`.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT sleep_on_receive_external_table_data"

$CLICKHOUSE_CLIENT --max_execution_time 2 \
    --external --file="$SMALL_FILE" --name=small1 --structure='x UInt64' \
    --external --file="$SMALL_FILE" --name=small2 --structure='x UInt64' \
    --external --file="$SMALL_FILE" --name=small3 --structure='x UInt64' \
    --external --file="$SMALL_FILE" --name=small4 --structure='x UInt64' \
    -q "SELECT count() FROM small1" 2>&1 | grep -o -F "Timeout exceeded while reading external table data" | uniq
