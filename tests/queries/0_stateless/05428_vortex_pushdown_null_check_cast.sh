#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A malformed string read as a nullable number becomes NULL after the scan. A null check pushed
# into the scan sees the original non-null string, so it must not be pushed for such a conversion,
# or the result would depend on `input_format_vortex_filter_push_down`.
DATA_FILE=$CUR_DIR/test_$CLICKHOUSE_TEST_UNIQUE_NAME.vortex

$CLICKHOUSE_LOCAL -q "SELECT arrayJoin(['bad', '1']) AS s, toInt32(number) AS i FROM numbers(1) FORMAT Vortex" > "$DATA_FILE"

for push_down in 0 1; do
    echo "push_down = $push_down"
    for predicate in "s IS NULL" "NOT isNotNull(s)" "s IS NOT NULL" "isNotNull(s)"; do
        $CLICKHOUSE_LOCAL -q "
            SELECT '$predicate', count()
            FROM file('$DATA_FILE', 'Vortex', 's Nullable(UInt64)')
            WHERE $predicate
            SETTINGS input_format_vortex_filter_push_down = $push_down"
    done
    # An integer widened into a nullable integer keeps its null-ness; the null check is pushed.
    $CLICKHOUSE_LOCAL -q "
        SELECT 'i IS NULL', count()
        FROM file('$DATA_FILE', 'Vortex', 'i Nullable(Int64)')
        WHERE i IS NULL
        SETTINGS input_format_vortex_filter_push_down = $push_down"
done

rm -f "$DATA_FILE"
