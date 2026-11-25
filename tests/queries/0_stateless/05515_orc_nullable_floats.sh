#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: ORC is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.orc"
trap 'rm -f "$FILE"' EXIT

# Nullable floats with different patterns of nulls, and special values, at the top level and nested.
QUERY="
    SELECT
        number,
        multiIf(k = 0, NULL, k = 1, nan, k = 2, inf, k = 3, -inf, k = 4, -0., k = 5, toFloat32(1e-42), toFloat32(number) / 7)::Nullable(Float32) AS f32,
        multiIf(k2 = 0, NULL, k2 = 1, nan, k2 = 2, -inf, k2 = 3, 5e-324, k2 = 4, -0., number / 3.)::Nullable(Float64) AS f64,
        if(intDiv(number, 1000) % 2 = 0, NULL, number / 2.)::Nullable(Float64) AS runs,
        NULL::Nullable(Float32) AS all_null,
        (number * 1.5)::Nullable(Float64) AS no_null,
        arrayMap(x -> if(x % 3 = 0, NULL, toFloat32(x) / 4), range(number % 5))::Array(Nullable(Float32)) AS arr
    FROM (SELECT number, cityHash64(number) % 9 AS k, cityHash64(number, 1) % 7 AS k2 FROM numbers(10000))"
STRUCTURE='number UInt64, f32 Nullable(Float32), f64 Nullable(Float64), runs Nullable(Float64), all_null Nullable(Float32), no_null Nullable(Float64), arr Array(Nullable(Float32))'
DIGEST="sum(cityHash64(number, isNull(f32), reinterpretAsUInt32(ifNull(f32, 0)), isNull(f64), reinterpretAsUInt64(ifNull(f64, 0)),
    isNull(runs), ifNull(runs, 0), isNull(all_null), no_null, arrayMap(x -> (isNull(x), reinterpretAsUInt32(ifNull(x, 0))), arr)))"

$CLICKHOUSE_LOCAL -q "$QUERY INTO OUTFILE '$FILE' TRUNCATE FORMAT ORC"
$CLICKHOUSE_LOCAL -q "
    SELECT (SELECT $DIGEST FROM ($QUERY)) = (SELECT $DIGEST FROM file('$FILE', 'ORC', '$STRUCTURE'));
    SELECT countIf(f32 IS NULL), countIf(f64 IS NULL), countIf(runs IS NULL), countIf(all_null IS NULL), countIf(no_null IS NULL) FROM file('$FILE', 'ORC', '$STRUCTURE')"
