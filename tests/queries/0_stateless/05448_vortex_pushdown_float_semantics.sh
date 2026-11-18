#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Vortex compares floating-point values in their total order, ClickHouse as IEEE 754 does: `-0.0`
# equals `0`, and every comparison with `NaN` but `notEquals` is false, also under `NOT`. The result
# must not depend on `input_format_vortex_filter_push_down`.
DATA_FILE=$CUR_DIR/test_$CLICKHOUSE_TEST_UNIQUE_NAME.vortex

$CLICKHOUSE_LOCAL -q "
    SELECT x AS f64, toFloat32(x) AS f32
    FROM (SELECT arrayJoin([-0., 0., nan, -nan, 1.5, -1.5, 1.]) AS x)
    FORMAT Vortex" > "$DATA_FILE"

for column in f64 f32; do
    for predicate in \
        "$column = 0" "$column = -0." "0 = $column" "$column != 0" "$column >= 0" "$column > 0" "$column <= 0" "$column < 0" \
        "$column >= -0." "$column <= -0." "$column = 1.5" "$column != 1.5" \
        "NOT ($column > 1)" "NOT ($column < 1)" "NOT ($column = 0)" "NOT ($column != 0)" "NOT ($column >= 0)" \
        "$column IN (0, 1.5)" "$column NOT IN (0, 1.5)" "$column IN (1.5, 1)" "$column NOT IN (1.5, 1)" \
        "$column = 0 OR $column = 1" "NOT ($column = 0 OR $column > 1)"
    do
        $CLICKHOUSE_LOCAL --param_predicate="$predicate" -q "
            SELECT
                {predicate:String},
                (SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE $predicate SETTINGS input_format_vortex_filter_push_down = 0),
                (SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE $predicate SETTINGS input_format_vortex_filter_push_down = 1)
            FORMAT TSVRaw"
    done
done

rm -f "$DATA_FILE"
