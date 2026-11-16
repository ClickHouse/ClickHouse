#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An AggregateFunction(any, Dynamic) state whose value is nested $1 levels deep through JSON and Dynamic.
# The depth comes from the data, not from the declared type.
state() { echo "concat('01', repeat('3000800820000000010161', $1), '300080082000000000')"; }

# The constant is sent to the remote server as a scalar, and the materialized value is sent back by it.
# Both are serialized or deserialized on the small stack of the remote query coroutine. A shallow value must succeed,
# 1000 and 2000 levels may succeed or report TOO_DEEP_RECURSION depending on the build, and 4000 levels must report it.
settings="SETTINGS async_socket_for_remote = 1, async_query_sending_for_remote = 1, optimize_const_name_size = 256"

check()
{
    local label=$1 expect=$2 query=$3
    local out
    out=$($CLICKHOUSE_CLIENT -q "$query FORMAT Null $settings" 2>&1)
    if [[ ( -z "$out" && "$expect" != throw ) || ( "$out" == *TOO_DEEP_RECURSION* && "$expect" != ok ) ]]; then
        echo "$label: ok"
    else
        echo "$label: $out"
    fi
}

for depth in 5 1000 2000 4000; do
    # 5 levels fit on every build, so a false positive of the coroutine stack check shows up here.
    # 4000 levels exceed the coroutine stack on every build, so this one is always rejected.
    case $depth in
        5) expect=ok ;;
        4000) expect=throw ;;
        *) expect=any ;;
    esac
    check "send $depth" "$expect" "SELECT CAST(unhex($(state $depth)), 'AggregateFunction(any, Dynamic)') FROM remote('127.0.0.2', system.one)"
    check "receive $depth" "$expect" "SELECT CAST(unhex(materialize($(state $depth))), 'AggregateFunction(any, Dynamic)') FROM remote('127.0.0.2', system.one)"
done

$CLICKHOUSE_CLIENT -q "SELECT count() FROM remote('127.0.0.2', system.one)"
