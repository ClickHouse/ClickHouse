#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: the Delta Lake kernel (Rust) is not built in the fast test
# no-msan: the Delta Lake kernel (Rust) is not built with MSan

# Reads golden Delta tables of delta-kernel-rs (see data_delta_lake/kernel_golden/README.md)
# with decimals of various precision, scale and Parquet encoding.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

GOLDEN="$CUR_DIR/data_delta_lake/kernel_golden"

# All queries run in one clickhouse-local launch, because a launch is slow in sanitizer builds.
QUERIES=""

# Prints the number of rows, the number of rows missing in the Delta table and the number of unexpected rows.
function check()
{
    local table="$1"
    local delta="deltaLakeLocal('$GOLDEN/$table/delta')"
    local expected="file('$GOLDEN/$table/expected/*.parquet', Parquet)"
    QUERIES+="
        SELECT '$table', (SELECT count() FROM $delta),
            (SELECT count() FROM (SELECT * FROM $expected EXCEPT ALL SELECT * FROM $delta)),
            (SELECT count() FROM (SELECT * FROM $delta EXCEPT ALL SELECT * FROM $expected));"
}

for table in \
    basic-decimal-table \
    basic-decimal-table-legacy \
    decimal-various-scale-precision \
    124-decimal-decode-bug \
    125-iterator-bug
do
    check "$table"
done

# These tables are large, so only the Delta table is stored and it is compared with
# the aggregates of the expected data from delta-kernel-rs (which are identical for all of them).
for table in \
    parquet-decimal-dictionaries \
    parquet-decimal-dictionaries-v2 \
    parquet-decimal-type
do
    QUERIES+="
        SELECT '$table', count(), sum(cityHash64(*)), min(tuple(*)), max(tuple(*))
        FROM deltaLakeLocal('$GOLDEN/$table/delta');"
done

QUERIES+="
    SELECT 'Values';
    DESCRIBE TABLE deltaLakeLocal('$GOLDEN/decimal-various-scale-precision/delta') SETTINGS describe_compact_output = 1;
    SELECT * FROM deltaLakeLocal('$GOLDEN/decimal-various-scale-precision/delta') ORDER BY ALL;
    SELECT * FROM deltaLakeLocal('$GOLDEN/basic-decimal-table/delta') ORDER BY ALL;
    SELECT * FROM deltaLakeLocal('$GOLDEN/basic-decimal-table-legacy/delta') ORDER BY ALL;
    SELECT * FROM deltaLakeLocal('$GOLDEN/124-decimal-decode-bug/delta');
    SELECT col1, count() FROM deltaLakeLocal('$GOLDEN/parquet-decimal-dictionaries/delta') GROUP BY col1 ORDER BY col1;
    SELECT * FROM deltaLakeLocal('$GOLDEN/parquet-decimal-type/delta') WHERE id IN (0, 1, 12345, 99997) ORDER BY id;
"

$CLICKHOUSE_LOCAL -q "$QUERIES"
