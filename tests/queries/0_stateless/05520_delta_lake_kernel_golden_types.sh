#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: the Delta Lake kernel (Rust) is not built in the fast test
# no-msan: the Delta Lake kernel (Rust) is not built with MSan

# Reads golden Delta tables of delta-kernel-rs (see data_delta_lake/kernel_golden/README.md)
# with various data types and compares them with the expected data written by Spark.

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

function schema()
{
    QUERIES+="
        SELECT '$1';
        DESCRIBE TABLE deltaLakeLocal('$GOLDEN/$1/delta') SETTINGS describe_compact_output = 1;"
}

for table in \
    data-reader-primitives \
    data-reader-array-primitives \
    data-reader-array-complex-objects \
    data-reader-map \
    data-reader-nested-struct \
    data-reader-nullable-field-invalid-schema-key \
    data-reader-escaped-chars \
    data-reader-date-types-America \
    data-reader-date-types-Asia \
    data-reader-date-types-Etc \
    data-reader-date-types-Iceland \
    data-reader-date-types-Jst \
    data-reader-date-types-Pst \
    data-reader-date-types-utc \
    data-reader-timestamp_ntz \
    data-reader-timestamp_ntz-id-mode \
    data-reader-timestamp_ntz-name-mode \
    kernel-timestamp-int96 \
    kernel-timestamp-pst \
    kernel-timestamp-timestamp_micros \
    kernel-timestamp-timestamp_millis \
    parquet-all-types \
    parquet-all-types-legacy-format
do
    check "$table"
done

QUERIES+="SELECT 'Schemas';"
for table in \
    data-reader-primitives \
    data-reader-map \
    data-reader-nested-struct \
    data-reader-timestamp_ntz \
    data-reader-timestamp_ntz-id-mode \
    kernel-timestamp-int96 \
    parquet-all-types
do
    schema "$table"
done

QUERIES+="
    SELECT 'Values';
    SELECT * FROM deltaLakeLocal('$GOLDEN/data-reader-date-types-Asia/delta');
    SELECT * FROM deltaLakeLocal('$GOLDEN/data-reader-timestamp_ntz/delta') ORDER BY id;
    SELECT * FROM deltaLakeLocal('$GOLDEN/kernel-timestamp-int96/delta') ORDER BY ALL;
    SELECT * FROM deltaLakeLocal('$GOLDEN/data-reader-escaped-chars/delta') ORDER BY ALL;
    SELECT * FROM deltaLakeLocal('$GOLDEN/data-reader-map/delta') ORDER BY i LIMIT 2;
    SELECT * FROM deltaLakeLocal('$GOLDEN/data-reader-nested-struct/delta') ORDER BY b LIMIT 2;
"

$CLICKHOUSE_LOCAL --session_timezone UTC -q "$QUERIES"
