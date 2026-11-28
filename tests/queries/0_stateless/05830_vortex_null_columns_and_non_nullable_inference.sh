#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${WORKING_DIR}"

# A `Null` field and a struct of `Null` fields are decoded as constants; reading them as other types
# converts them.
$CLICKHOUSE_LOCAL -q "SELECT NULL AS x, tuple(NULL) AS t FROM numbers(3) FORMAT Vortex" > "$WORKING_DIR/null.vortex"
$CLICKHOUSE_LOCAL -q "DESC file('$WORKING_DIR/null.vortex', Vortex)"
$CLICKHOUSE_LOCAL -q "SELECT x, t, toTypeName(x), toTypeName(t) FROM file('$WORKING_DIR/null.vortex', Vortex, 'x Nullable(UInt64), t Tuple(Nullable(UInt64))')"
$CLICKHOUSE_LOCAL -q "SELECT x, t FROM file('$WORKING_DIR/null.vortex', Vortex, 'x UInt64, t Tuple(UInt64)')"

# `schema_inference_make_columns_nullable = 0` removes `Nullable` from the nested types too.
$CLICKHOUSE_LOCAL -q "
    SELECT
        [if(number % 2 = 0, NULL, number)] AS a,
        tuple(if(number % 2 = 0, NULL, number), toNullable('s'))::Tuple(x Nullable(UInt64), y Nullable(String)) AS t,
        [NULL] AS n
    FROM numbers(2) FORMAT Vortex" > "$WORKING_DIR/nested.vortex"
for mode in 0 1 auto; do
    echo "schema_inference_make_columns_nullable = $mode"
    $CLICKHOUSE_LOCAL -q "DESC file('$WORKING_DIR/nested.vortex', Vortex) SETTINGS schema_inference_make_columns_nullable = '$mode'"
done
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$WORKING_DIR/nested.vortex', Vortex) SETTINGS schema_inference_make_columns_nullable = 0"

rm -rf "${WORKING_DIR}"
