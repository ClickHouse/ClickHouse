#!/usr/bin/env bash

CURDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$CURDIR"/../shell_config.sh

export CLICKHOUSE_USER_FILES="$CURDIR/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$CLICKHOUSE_USER_FILES"
mkdir -p "$CLICKHOUSE_USER_FILES"

PARQUET_FILE="10307_decimal512_pipeline.parquet"
ARROW_FILE="10307_decimal512_pipeline.arrow"

$CLICKHOUSE_CLIENT --query="INSERT INTO FUNCTION file('$PARQUET_FILE', 'Parquet', 'd Decimal512(4)') SETTINGS engine_file_truncate_on_insert=1 SELECT toDecimal512(number, 4) FROM numbers(4)"

$CLICKHOUSE_CLIENT --query="INSERT INTO FUNCTION file('$ARROW_FILE', 'Arrow', 'd Decimal512(4)') SETTINGS engine_file_truncate_on_insert=1 SELECT d FROM file('$PARQUET_FILE', 'Parquet', 'd Decimal512(4)')"

echo "-- cross-format decimal512 pipeline"
$CLICKHOUSE_CLIENT --query="
WITH arr AS (
    SELECT groupArray(d) AS values
    FROM file('$ARROW_FILE', 'Arrow', 'd Decimal512(4)')
)
SELECT
    arrayMap(x -> toString(x), arrayCumSum(arraySort(values))) AS cum_sorted,
    arraySort(mapValues(mapAdd(map('sum', sum_val), map('sum', toDecimal512('1.0000', 4))))) AS totals
FROM (
    SELECT values, arrayReduce('sum', values) AS sum_val FROM arr
);
"

rm -rf "$CLICKHOUSE_USER_FILES"
