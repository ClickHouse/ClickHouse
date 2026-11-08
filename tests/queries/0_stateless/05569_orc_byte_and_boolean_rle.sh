#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: ORC is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.orc"
trap 'rm -f "$FILE"' EXIT

# Booleans and bytes with literal and repeated runs and different patterns of nulls, at the top level and nested.
# The number of rows is not a multiple of 8, so the last byte of each boolean stream is partial.
QUERY="
    SELECT
        number::Int64 AS id,
        (number % 3 = 0)::Bool AS b,
        (intDiv(number, 500) % 2 = 1)::Bool AS rb,
        if(cityHash64(number) % 5 = 0, NULL, number % 7 < 3)::Nullable(Bool) AS nb,
        toInt8(intDiv(number, 37) % 5) AS i8,
        toInt8(number % 100) AS l8,
        if(number % 3 = 0, NULL, toInt8(number))::Nullable(Int8) AS ni8,
        if(intDiv(number, 1000) % 2 = 0, NULL, toInt8(intDiv(number, 100) % 3))::Nullable(Int8) AS runs,
        NULL::Nullable(Int8) AS all_null,
        arrayMap(x -> if(x % 3 = 0, NULL, x % 2 = 0), range(number % 5))::Array(Nullable(Bool)) AS arr,
        if(number % 7 = 0, NULL, tuple(if(number % 11 = 0, NULL, number % 2 = 0), if(number % 13 = 0, NULL, toInt8(number % 50))))::Nullable(Tuple(b Nullable(Bool), i Nullable(Int8))) AS t
    FROM numbers(30011)"
STRUCTURE='id Int64, b Bool, rb Bool, nb Nullable(Bool), i8 Int8, l8 Int8, ni8 Nullable(Int8), runs Nullable(Int8), all_null Nullable(Int8), arr Array(Nullable(Bool)), t Nullable(Tuple(b Nullable(Bool), i Nullable(Int8)))'
DIGEST="sum(cityHash64(id, b, rb, isNull(nb), ifNull(nb, false), i8, l8, isNull(ni8), ifNull(ni8, 0), isNull(runs), ifNull(runs, 0),
    isNull(all_null), arrayMap(x -> (isNull(x), ifNull(x, false)), arr), toString(t)))"

$CLICKHOUSE_LOCAL --output_format_orc_row_index_stride 1000 -q "$QUERY INTO OUTFILE '$FILE' TRUNCATE FORMAT ORC"

# Batch sizes that end the batches inside a byte of the boolean streams and inside the runs.
for batch_size in 1 3 8 13 1000 100000; do
    $CLICKHOUSE_LOCAL -q "
        SELECT $batch_size, (SELECT $DIGEST FROM ($QUERY)) = (SELECT $DIGEST FROM file('$FILE', 'ORC', '$STRUCTURE') SETTINGS input_format_orc_row_batch_size = $batch_size)"
done

# Row groups before id 12345 are skipped (count = rows of the kept groups), so the reader seeks into the middle of the streams.
$CLICKHOUSE_LOCAL -q "
    SELECT count() FROM file('$FILE', 'ORC', '$STRUCTURE') WHERE indexHint(id >= 12345) SETTINGS input_format_orc_filter_push_down = 1;
    SELECT (SELECT $DIGEST FROM ($QUERY) WHERE id >= 12345)
        = (SELECT $DIGEST FROM file('$FILE', 'ORC', '$STRUCTURE') WHERE id >= 12345 SETTINGS input_format_orc_filter_push_down = 1);
    SELECT countIf(nb IS NULL), countIf(ni8 IS NULL), countIf(runs IS NULL), countIf(all_null IS NULL), countIf(t IS NULL),
        sum(arrayCount(x -> x IS NULL, arr))
    FROM file('$FILE', 'ORC', '$STRUCTURE')"
