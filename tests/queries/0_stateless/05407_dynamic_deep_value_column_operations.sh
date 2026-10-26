#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Each repetition of 1E2B0801 is a value of type Array(Dynamic(max_types=8)) with one element, so the
# nesting depth of the Dynamic value comes from the data, not from the declared type. The second row is NULL.
nested_value() { echo "format(RowBinary, 'd Dynamic', unhex(concat(repeat('1E2B0801', $1), '0000')))"; }

$CLICKHOUSE_CLIENT -q "SELECT arraySort([d, d]) FROM $(nested_value 3)"

# Depending on the build and the stack size, a deep value is either processed or rejected with
# TOO_DEEP_RECURSION, but it must never crash the server or use memory quadratic in the depth.
for depth in 1000 9000; do
    table=$(nested_value $depth)
    for query in "SELECT [d] FROM $table" "SELECT arraySort([d, d]) FROM $table" "SELECT d, arrayJoin([1, 2]) FROM $table" "SELECT d FROM $table WHERE d IS NOT NULL"; do
        out=$($CLICKHOUSE_CLIENT -q "$query FORMAT Null" 2>&1)
        [[ -z "$out" || "$out" == *TOO_DEEP_RECURSION* ]] || echo "$query, depth $depth: $out"
    done
done

$CLICKHOUSE_CLIENT -q "SELECT 'server is alive'"
