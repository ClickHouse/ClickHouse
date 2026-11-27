#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: the Delta Lake kernel (Rust) is not built in the fast test
# no-msan: the Delta Lake kernel (Rust) is not built with MSan

# Reads golden Delta tables of delta-kernel-rs (see data_delta_lake/kernel_golden/README.md)
# with per-file statistics and partitions, and checks that filters on every column type return
# the same rows as the expected data, and how many files are read after data skipping.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

GOLDEN="$CUR_DIR/data_delta_lake/kernel_golden"

# All queries run in one clickhouse-local launch, because a launch is slow in sanitizer builds.
# After each query, the cumulative value of `DeltaLakeScannedFiles` is printed on a separate line,
# and the difference to the previous value (the number of files read by the query) is computed by awk.
QUERIES=""

# Prints the table, the filter, the number of matching rows in the Delta table and in the expected data,
# and the number of data files read.
function filter()
{
    local table="$1"
    local predicate="$2"
    QUERIES+="
        SELECT '$table', \$\$${predicate}\$\$,
            (SELECT count() FROM deltaLakeLocal('$GOLDEN/$table/delta') WHERE $predicate),
            (SELECT count() FROM file('$GOLDEN/$table/expected/*.parquet', Parquet) WHERE $predicate);
        SELECT 'files', sum(value) FROM system.events WHERE event = 'DeltaLakeScannedFiles';"
}

for table in \
    data-skipping-basic-stats-all-types \
    data-skipping-basic-stats-all-types-checkpoint \
    data-skipping-basic-stats-all-types-columnmapping-id \
    data-skipping-basic-stats-all-types-columnmapping-name
do
    filter "$table" "1"
    filter "$table" "as_int = 0"
    filter "$table" "as_int = 1"
    filter "$table" "as_int > 0"
    filter "$table" "as_int < 0"
    filter "$table" "as_int IS NULL"
    filter "$table" "as_int IS NOT NULL"
    filter "$table" "as_long = 0"
    filter "$table" "as_long >= 1"
    filter "$table" "as_byte = 0"
    filter "$table" "as_byte != 0"
    filter "$table" "as_short = 0"
    filter "$table" "as_short <= -1"
    filter "$table" "as_float = 0"
    filter "$table" "as_float > 0.5"
    filter "$table" "as_double = 0"
    filter "$table" "as_double < -0.5"
    filter "$table" "as_string = '0'"
    filter "$table" "as_string = '1'"
    filter "$table" "as_string > '0'"
    filter "$table" "as_date = '2000-01-01'"
    filter "$table" "as_date > '2000-01-01'"
    filter "$table" "as_timestamp = '2000-01-01 08:00:00'"
    filter "$table" "as_timestamp > '2000-01-01 08:00:00'"
    filter "$table" "as_big_decimal = 0"
    filter "$table" "as_big_decimal = 1"
    filter "$table" "as_int = 0 AND as_string = '1'"
    filter "$table" "as_int = 1 OR as_string = '0'"
done

table=data-skipping-change-stats-collected-across-versions
filter "$table" "1"
filter "$table" "col1 = 0"
filter "$table" "col1 > 0"
filter "$table" "col2 = 0"
filter "$table" "col2 > 0"
filter "$table" "col2 IS NULL"

table=data-skipping-partition-and-data-column
filter "$table" "1"
filter "$table" "part = 0"
filter "$table" "part = 1"
filter "$table" "part = 2"
filter "$table" "id = 0"
filter "$table" "id = 2"
filter "$table" "part = 1 AND id = 0"
filter "$table" "part = 1 AND id = 2"
filter "$table" "part = 0 OR id = 1"

$CLICKHOUSE_LOCAL --session_timezone UTC -q "$QUERIES" | awk -F '\t' '
    $1 == "files" { print row "\t" ($2 - files); files = $2; next }
    { row = $0 }'
