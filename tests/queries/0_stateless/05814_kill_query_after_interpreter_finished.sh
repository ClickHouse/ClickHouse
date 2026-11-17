#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

holder_id="${CLICKHOUSE_DATABASE}_holder"
victim_id="${CLICKHOUSE_DATABASE}_victim"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '${holder_id}' SYNC FORMAT Null" 2>/dev/null
}
trap cleanup EXIT

function wait_for()
{
    # $1: condition over system.processes
    for _ in {1..600}; do
        [ "$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE $1")" = "1" ] && return 0
        sleep 0.1
    done
    echo "timed out waiting for: $1"
    return 1
}

# $1: name of the case, $2: how the victim is killed ("kill" or "timeout")
function run_case()
{
    echo "-- $1"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_kill; CREATE TABLE t_kill (x UInt8) ENGINE = Memory; INSERT INTO t_kill VALUES (1)"

    # Holds the share lock of `t_kill` until it is killed.
    ${CLICKHOUSE_CLIENT} --query_id "${holder_id}" --max_block_size 1 \
        --query "INSERT INTO t_kill SELECT sleepEachRow(1) FROM numbers(100000)" >/dev/null 2>&1 &
    wait_for "query_id = '${holder_id}' AND read_rows > 0"

    local victim_settings=()
    [ "$2" = "timeout" ] && victim_settings=(--max_execution_time 1)
    # Needs the exclusive lock, so it waits inside its interpreter.
    ${CLICKHOUSE_CLIENT} --query_id "${victim_id}" "${victim_settings[@]}" \
        --query "TRUNCATE TABLE t_kill" > "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_victim.out" 2>&1 &
    local victim_pid=$!
    wait_for "query_id = '${victim_id}'"

    [ "$2" = "kill" ] && ${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '${victim_id}' ASYNC FORMAT Null"
    wait_for "query_id = '${victim_id}' AND is_cancelled"

    ${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '${holder_id}' SYNC FORMAT Null"
    wait ${victim_pid}

    # The query did its work, so it must report success.
    grep -o -m1 -E 'QUERY_WAS_CANCELLED|TIMEOUT_EXCEEDED' "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_victim.out" || echo "OK"
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_kill"
}

run_case "KILL QUERY" kill
run_case "max_execution_time" timeout
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_kill"

# A PARALLEL WITH cancelled in clickhouse-local stops its combined pipeline from the interactive cancel callback,
# so it must still report the cancellation, not success.
echo "-- PARALLEL WITH in clickhouse-local"
pw_file="${CLICKHOUSE_TEST_UNIQUE_NAME}_pw.tsv"  # relative: clickhouse-local resolves file() against its cwd, which is ours
pw_err="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_pw.err"
rm -f "${pw_file}"
# The finite insert writes the marker file and finishes; the endless one keeps the combined pipeline running.
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${pw_file}', 'TSV', 'x UInt64') SELECT number FROM numbers(10)
    PARALLEL WITH
    INSERT INTO FUNCTION null('x UInt64') SELECT number FROM system.numbers
    SETTINGS max_rows_to_read = 0, max_bytes_to_read = 0" > /dev/null 2> "${pw_err}" &
local_pid=$!
for _ in {1..600}; do [ -s "${pw_file}" ] && break; sleep 0.1; done
kill -INT ${local_pid}
for _ in {1..600}; do kill -0 ${local_pid} 2>/dev/null || break; sleep 0.1; done
if kill -0 ${local_pid} 2>/dev/null; then
    kill -9 ${local_pid}
    echo "HUNG"
fi
wait ${local_pid} 2>/dev/null
grep -o -m1 'QUERY_WAS_CANCELLED' "${pw_err}" || echo "no error"
rm -f "${pw_file}"
