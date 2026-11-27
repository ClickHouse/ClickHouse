#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: the Delta Lake kernel (Rust) is not built in the fast test
# no-msan: the Delta Lake kernel (Rust) is not built with MSan

# Reads golden Delta tables of delta-kernel-rs (see data_delta_lake/kernel_golden/README.md)
# with deletion vectors: partitioned with a checkpoint, with column mapping, and the log replay
# cases where the same file is added with different deletion vectors.

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
    local settings="$2"
    local delta="(SELECT * FROM deltaLakeLocal('$GOLDEN/$table/delta') SETTINGS $settings)"
    local expected="file('$GOLDEN/$table/expected/*.parquet', Parquet)"
    QUERIES+="
        SELECT '$table', \$\$${settings}\$\$, (SELECT count() FROM $delta),
            (SELECT count() FROM (SELECT * FROM $expected EXCEPT ALL SELECT * FROM $delta)),
            (SELECT count() FROM (SELECT * FROM $delta EXCEPT ALL SELECT * FROM $expected));"
}

# Prints the number of rows matching the filter in the Delta table and in the expected data.
function filter()
{
    local table="$1"
    local predicate="$2"
    QUERIES+="
        SELECT '$table', \$\$${predicate}\$\$,
            (SELECT count() FROM deltaLakeLocal('$GOLDEN/$table/delta') WHERE $predicate),
            (SELECT count() FROM file('$GOLDEN/$table/expected/*.parquet', Parquet) WHERE $predicate);"
}

for table in \
    dv-partitioned-with-checkpoint \
    dv-with-columnmapping \
    log-replay-dv-key-cases
do
    check "$table" "max_threads = 0"
    check "$table" "max_threads = 1, max_block_size = 3"
    check "$table" "max_threads = 4, max_block_size = 1, input_format_parquet_filter_push_down = 0"
done

for table in \
    dv-partitioned-with-checkpoint \
    dv-with-columnmapping
do
    filter "$table" "part = 3"
    filter "$table" "part IN (0, 9)"
    filter "$table" "col1 >= 30"
    filter "$table" "col1 = 7"
    filter "$table" "col2 LIKE '%1%'"
    filter "$table" "part = 5 AND col1 > 20"
done

filter log-replay-dv-key-cases "id <= 10"
filter log-replay-dv-key-cases "id > 40"

QUERIES+="
    SELECT 'Values';
    SELECT part, count(), sum(col1), groupArraySorted(100)(col1) FROM deltaLakeLocal('$GOLDEN/dv-partitioned-with-checkpoint/delta') GROUP BY part ORDER BY part;
    SELECT part, count(), sum(col1), groupArraySorted(100)(col1) FROM deltaLakeLocal('$GOLDEN/dv-with-columnmapping/delta') GROUP BY part ORDER BY part;
    SELECT groupArraySorted(100)(id) FROM deltaLakeLocal('$GOLDEN/log-replay-dv-key-cases/delta');
"

$CLICKHOUSE_LOCAL -q "$QUERIES"
