#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An AggregateFunction(any, Dynamic) state whose value is nested $1 levels deep through JSON and Dynamic.
# The depth comes from the data, not from the declared type.
state() { echo "concat('01', repeat('3000800820000000010161', $1), '300080082000000000')"; }

# The constant is sent to the remote server as a scalar, and the materialized value is sent back by it.
# Both are serialized or deserialized on the small stack of the remote query coroutine, so they must
# either succeed or report TOO_DEEP_RECURSION, depending on the build.
settings="SETTINGS async_socket_for_remote = 1, async_query_sending_for_remote = 1, optimize_const_name_size = 256"

check()
{
    local label=$1 must_throw=$2 query=$3
    local out
    out=$($CLICKHOUSE_CLIENT -q "$query FORMAT Null $settings" 2>&1)
    if [[ "$out" == *TOO_DEEP_RECURSION* || ( -z "$out" && "$must_throw" == 0 ) ]]; then
        echo "$label: ok"
    else
        echo "$label: $out"
    fi
}

for depth in 1000 2000 4000; do
    # 4000 levels exceed the coroutine stack on every build, so this one is always rejected.
    must_throw=$([[ $depth == 4000 ]] && echo 1 || echo 0)
    check "send $depth" "$must_throw" "SELECT CAST(unhex($(state $depth)), 'AggregateFunction(any, Dynamic)') FROM remote('127.0.0.2', system.one)"
    check "receive $depth" 0 "SELECT CAST(unhex(materialize($(state $depth))), 'AggregateFunction(any, Dynamic)') FROM remote('127.0.0.2', system.one)"
done

$CLICKHOUSE_CLIENT -q "SELECT count() FROM remote('127.0.0.2', system.one)"
