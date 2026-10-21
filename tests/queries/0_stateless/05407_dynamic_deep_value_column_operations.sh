#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Each repetition of 1E2B0801 is a value of type Array(Dynamic(max_types=8)) with one element, so the
# nesting depth of the Dynamic value comes from the data, not from the declared type.
nested_value() { echo "format(RowBinary, 'd Dynamic', unhex(concat(repeat('1E2B0801', $1), '00')))"; }

$CLICKHOUSE_CLIENT -q "SELECT arraySort([d, d]) FROM $(nested_value 3)"

# Depending on the build and the stack size, a deep value is either processed or rejected with
# TOO_DEEP_RECURSION, but it must never crash the server.
for depth in 1000 4000 8000 11000 14000; do
    for query in "SELECT [d]" "SELECT arraySort([d, d])" "SELECT any(d)" "SELECT arrayJoin([d, d])" "SELECT d, arrayJoin([1, 2])"; do
        out=$($CLICKHOUSE_CLIENT -q "$query FROM $(nested_value $depth) FORMAT Null" 2>&1)
        [[ -z "$out" || "$out" == *TOO_DEEP_RECURSION* ]] || echo "$query, depth $depth: $out"
    done
done

$CLICKHOUSE_CLIENT -q "SELECT 'server is alive'"
