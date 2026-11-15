#!/usr/bin/env bash
# Tags: no-fasttest
# The fast-test build is unoptimized and re-runs a new test several times under a 60s cap, while
# materializing a deeply nested Dynamic value whose operation does not trip the recursion guard takes
# superlinear time. The optimized stateless configurations run it within budget.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Each repetition of 1E2B0801 is a value of type Array(Dynamic(max_types=8)) with one element, so the
# nesting depth of the Dynamic value comes from the data, not from the declared type. The second row is NULL.
nested_value() { echo "format(RowBinary, 'd Dynamic', unhex(concat(repeat('1E2B0801', $1), '0000')))"; }

$CLICKHOUSE_CLIENT -q "SELECT arraySort([d, d]) FROM $(nested_value 3)"

# Depending on the build and the stack size, a deep value is either processed or rejected with
# TOO_DEEP_RECURSION, but it must never crash the server or use memory quadratic in the depth.
# Parsing a value takes time quadratic in its depth (every level has a longer type name), so the deepest value,
# which exhausts the stack in the release build, is only used for the operations that recurse into the nested
# columns: `index`, `compareAt`, and `filter`.
check_query() {
    out=$($CLICKHOUSE_CLIENT -q "$1 FORMAT Null" 2>&1)
    [[ -z "$out" || "$out" == *TOO_DEEP_RECURSION* ]] || echo "$1: $out"
}
table=$(nested_value 1000)
for query in "SELECT [d] FROM $table" "SELECT arraySort([d, d]) FROM $table" "SELECT d, arrayJoin([1, 2]) FROM $table" "SELECT d FROM $table WHERE d IS NOT NULL"; do
    check_query "$query"
done
table=$(nested_value 9000)
for query in "SELECT [d] FROM $table" "SELECT arraySort([d, d]) FROM $table" "SELECT d FROM $table WHERE d IS NOT NULL"; do
    check_query "$query"
done

# Writing a part and merging parts go through the dynamic structure helpers of the column.
# Writing a part takes time quadratic in the nesting depth (every level adds substreams with longer names),
# so a depth that exhausts the stack takes tens of seconds; use a small depth that checks the normal path
# and stays far from the stack limit in sanitizer builds, where depth 300 already hits it.
# The substream names of a deep value exceed the file name length limit of the filesystem in a wide part,
# so they must be replaced with hashes regardless of the randomized MergeTree settings.
$CLICKHOUSE_CLIENT -q "CREATE TABLE deep_dynamic (d Dynamic) ENGINE = MergeTree ORDER BY tuple() SETTINGS replace_long_file_name_to_hash = 1, max_file_name_length = 127"
table=$(nested_value 64)
for query in "INSERT INTO deep_dynamic SELECT d FROM $table" "INSERT INTO deep_dynamic SELECT d FROM $table" "OPTIMIZE TABLE deep_dynamic FINAL"; do
    out=$($CLICKHOUSE_CLIENT -q "$query" 2>&1)
    [[ -z "$out" ]] || echo "$query: $out"
done
$CLICKHOUSE_CLIENT -q "SELECT count(), countIf(d IS NULL) FROM deep_dynamic"
$CLICKHOUSE_CLIENT -q "DROP TABLE deep_dynamic"

$CLICKHOUSE_CLIENT -q "SELECT 'server is alive'"
