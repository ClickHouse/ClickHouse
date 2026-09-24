#!/usr/bin/env bash

native_cancel_cleanup_client()
{
    if [[ -n "${CLIENT_PID:-}" ]]; then
        kill -KILL "$CLIENT_PID" 2>/dev/null || true
        wait "$CLIENT_PID" 2>/dev/null || true
        CLIENT_PID=""
    fi
}

native_cancel_wait_for_failpoint()
{
    local failpoint="$1"
    local failure_message="$2"
    local client_error_file="${3:-}"

    if timeout 60 $CLICKHOUSE_CLIENT --query \
        "SYSTEM WAIT FAILPOINT $failpoint PAUSE" > /dev/null 2>&1; then
        return 0
    fi

    echo "$failure_message"
    if [[ -n "$client_error_file" ]]; then
        cat "$client_error_file"
    fi
    return 1
}

native_cancel_wait_for_process()
{
    local query_id="$1"
    local condition="$2"
    local failure_message="$3"
    local client_error_file="${4:-}"

    for _ in {1..200}; do
        if [[ "$($CLICKHOUSE_CLIENT --query "
            SELECT count() FROM system.processes
            WHERE query_id='$query_id' AND ($condition)")" == 1 ]]; then
            return 0
        fi
        if [[ -n "${CLIENT_PID:-}" ]] && ! kill -0 "$CLIENT_PID" 2>/dev/null; then
            break
        fi
        sleep 0.1
    done

    echo "$failure_message"
    if [[ -n "$client_error_file" ]]; then
        cat "$client_error_file"
    fi
    return 1
}

native_cancel_wait_for_query_log()
{
    local query_id="$1"
    local condition="$2"

    for _ in {1..100}; do
        $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
        if [[ "$($CLICKHOUSE_CLIENT --query "
            SELECT $condition
            FROM system.query_log WHERE current_database = currentDatabase()
                AND query_id='$query_id' AND type != 'QueryStart'")" == 1 ]]; then
            return 0
        fi
        sleep 0.1
    done

    return 1
}
