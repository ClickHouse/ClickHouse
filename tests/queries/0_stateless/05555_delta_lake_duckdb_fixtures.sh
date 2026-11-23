#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: `deltaLakeLocal` needs the delta-kernel-rs library, which fast test does not build.
# no-msan: delta-kernel-rs is Rust code that is not instrumented by MSan.

# Delta fixtures adopted from the duckdb-delta test suite (see data_delta_lake/duckdb/README.md).
# The expected values are the ones asserted by duckdb-delta in test/sql/inlined/*.test and
# test/sql/issues/partition_equality_filter__issue_303.test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_delta_lake/duckdb"

$CLICKHOUSE_LOCAL --session_timezone UTC -q "
SELECT '-- spark_int96_timestamp: INT96 timestamps at the top level and inside a struct, an array and a map';
DESCRIBE deltaLakeLocal('$DATA/spark_int96_timestamp');
SELECT writer, toUnixTimestamp64Micro(c), toUnixTimestamp64Micro(s.c), toUnixTimestamp64Micro(l[1]), toUnixTimestamp64Micro(m['k'])
FROM deltaLakeLocal('$DATA/spark_int96_timestamp') ORDER BY writer;
SELECT writer FROM deltaLakeLocal('$DATA/spark_int96_timestamp')
WHERE c = toDateTime64('2026-01-02 03:04:05.123456', 6, 'UTC') AND s.c = toDateTime64('2026-01-02 03:04:05.123456', 6, 'UTC') ORDER BY writer;

SELECT '-- ict_timetravel: in-commit timestamps, one row added per commit';
SELECT count() FROM deltaLakeLocal('$DATA/ict_timetravel/delta_lake');
SELECT count() FROM deltaLakeLocal('$DATA/ict_timetravel/delta_lake') SETTINGS delta_lake_snapshot_version = 0;
SELECT count() FROM deltaLakeLocal('$DATA/ict_timetravel/delta_lake') SETTINGS delta_lake_snapshot_version = 1;

SELECT '-- issue_303_partitioned: filters on an integer partition column';
DESCRIBE deltaLakeLocal('$DATA/issue_303_partitioned/delta_lake');
SELECT count() FROM deltaLakeLocal('$DATA/issue_303_partitioned/delta_lake');
SELECT count() FROM deltaLakeLocal('$DATA/issue_303_partitioned/delta_lake') WHERE part = 42;
SELECT count() FROM deltaLakeLocal('$DATA/issue_303_partitioned/delta_lake') WHERE part IN (42);
SELECT count() FROM deltaLakeLocal('$DATA/issue_303_partitioned/delta_lake') WHERE part IN (31, 42);
SELECT count() FROM deltaLakeLocal('$DATA/issue_303_partitioned/delta_lake') WHERE part > 40;
SELECT count() FROM deltaLakeLocal('$DATA/issue_303_partitioned/delta_lake') WHERE part = 31;

SELECT '-- char_varchar: char(n) and varchar(n) columns, including inside an array and as a partition column';
DESCRIBE deltaLakeLocal('$DATA/char_varchar/basic');
DESCRIBE deltaLakeLocal('$DATA/char_varchar/nested');
DESCRIBE deltaLakeLocal('$DATA/char_varchar/partitioned');
SELECT count() FROM deltaLakeLocal('$DATA/char_varchar/basic');
"

echo "-- types that cannot be read are refused"
for t in geography geometry interval_day_time interval_year_month user_defined; do
    echo "$t: $($CLICKHOUSE_LOCAL -q "SELECT * FROM deltaLakeLocal('$DATA/unsupported_types/$t')" 2>&1 | grep -o -m1 'DELTA_KERNEL_ERROR')"
done

# A catalog-managed table must be read through its catalog, not by path.
echo "-- catalog_managed: $($CLICKHOUSE_LOCAL -q "SELECT * FROM deltaLakeLocal('$DATA/catalog_managed/delta_lake')" 2>&1 | grep -o -m1 'DELTA_KERNEL_ERROR')"

# The Variant type is not supported by the Delta Lake reader yet.
echo "-- variant: $($CLICKHOUSE_LOCAL -q "SELECT * FROM deltaLakeLocal('$DATA/variant/basic')" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED')"
