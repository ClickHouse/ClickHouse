#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# File statistics with NaN, +Infinity, -Infinity and -0.0 as min/max values, encoded as Databricks
# writes them (`"NaN"`, `"Infinity"` strings and plain numbers). Data skipping based on these
# statistics must not drop matching rows: for every predicate, the count over the Delta table must
# be equal to the count over the same Parquet files read directly.
# The transaction logs come from the Trino Delta Lake connector tests (`statistics/*`), see
# `data_delta_lake/trino/README.md`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_delta_lake/trino/statistics"

PREDICATES=(
    "val > 0"
    "val >= 0"
    "val < 0"
    "val <= 0"
    "val = 0"
    "val != 0"
    "val > 1"
    "val < -1"
    "isNaN(val)"
    "NOT isNaN(val)"
    "isInfinite(val)"
    "val = inf"
    "val = -inf"
    "val IS NULL"
    "NOT (val > 0)"
    "val BETWEEN -1 AND 1"
)

# The expected count is computed with `countIf` over all rows, because a filter on `file` can also skip
# data by the statistics in the Parquet files.
# The Delta table is read with the filter pushdown into the Parquet reader disabled, so that only data
# skipping by the statistics in the Delta log is tested: the page filter of the Parquet reader drops rows
# with NaN (e.g. `val != 0` over a page with the values 0 and NaN finds nothing), also when reading the
# Parquet files directly.
QUERY=""
for TABLE in nan positive_infinity negative_infinity negative_zero infinity_nan negative_infinity_nan zero_infinity zero_nan zero_negative_infinity nan_multi_file
do
    for PREDICATE in "${PREDICATES[@]}"
    do
        QUERY+="SELECT '$TABLE', '$PREDICATE',
            (SELECT count() FROM deltaLakeLocal('$DATA/$TABLE') WHERE $PREDICATE
                SETTINGS input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0),
            (SELECT countIf($PREDICATE) FROM file('$DATA/$TABLE/*.parquet', Parquet));
"
    done
done

$CLICKHOUSE_LOCAL -q "$QUERY"
