#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: the Delta Lake kernel (Rust) is not built in the fast test
# no-msan: the Delta Lake kernel (Rust) is not built with MSan

# Reads golden Delta tables of delta-kernel-rs (see data_delta_lake/kernel_golden/README.md)
# at every version with `delta_lake_snapshot_version`. The golden tables are generated as a sequence
# of stages of the same table (e.g. `snapshot-data0` is the version 0 of `snapshot-data1`, and so on),
# so the expected data of each version is the expected data of the corresponding stage.
# The tables include schema changes, partitioning changes, deletes, repartitioning and vacuum.

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
    local version="$2"
    local stage="$3"
    local delta="(SELECT * FROM deltaLakeLocal('$GOLDEN/$table/delta') SETTINGS delta_lake_snapshot_version = $version)"
    local expected="file('$GOLDEN/$stage/expected/*.parquet', Parquet)"
    QUERIES+="
        SELECT '$table', $version, '$stage', (SELECT count() FROM $delta),
            (SELECT count() FROM (SELECT * FROM $expected EXCEPT ALL SELECT * FROM $delta)),
            (SELECT count() FROM (SELECT * FROM $delta EXCEPT ALL SELECT * FROM $expected));"
}

function schema()
{
    QUERIES+="
        SELECT '$1', $2;
        DESCRIBE TABLE deltaLakeLocal('$GOLDEN/$1/delta') SETTINGS describe_compact_output = 1, delta_lake_snapshot_version = $2;"
}

check time-travel-start-start20-start40 0 time-travel-start
check time-travel-start-start20-start40 1 time-travel-start-start20
check time-travel-start-start20-start40 2 time-travel-start-start20-start40
check time-travel-start-start20-start40 -1 time-travel-start-start20-start40

check time-travel-schema-changes-b 0 time-travel-schema-changes-a
check time-travel-schema-changes-b 1 time-travel-schema-changes-b

check time-travel-partition-changes-b 0 time-travel-partition-changes-a
check time-travel-partition-changes-b 1 time-travel-partition-changes-b

check snapshot-repartitioned 0 snapshot-data0
check snapshot-repartitioned 1 snapshot-data1
check snapshot-repartitioned 2 snapshot-data2
check snapshot-repartitioned 3 snapshot-data3
check snapshot-repartitioned 4 snapshot-data2-deleted
check snapshot-repartitioned 5 snapshot-repartitioned

# The same table after vacuum: only the latest version is readable.
check snapshot-vacuumed 5 snapshot-repartitioned
check snapshot-vacuumed -1 snapshot-vacuumed

QUERIES+="SELECT 'Schemas';"
schema time-travel-schema-changes-b 0
schema time-travel-schema-changes-b 1
schema time-travel-partition-changes-b 0
schema time-travel-partition-changes-b 1

QUERIES+="
    SELECT 'Values';
    SELECT * FROM deltaLakeLocal('$GOLDEN/time-travel-schema-changes-b/delta') ORDER BY ALL LIMIT 3 SETTINGS delta_lake_snapshot_version = 0;
    SELECT * FROM deltaLakeLocal('$GOLDEN/time-travel-schema-changes-b/delta') ORDER BY ALL LIMIT 3 SETTINGS delta_lake_snapshot_version = 1;
    SELECT * FROM deltaLakeLocal('$GOLDEN/time-travel-schema-changes-b/delta') ORDER BY ALL DESC LIMIT 3 SETTINGS delta_lake_snapshot_version = 1;
    SELECT part5, count(), sum(id) FROM deltaLakeLocal('$GOLDEN/time-travel-partition-changes-b/delta') WHERE part5 = 3 GROUP BY part5 SETTINGS delta_lake_snapshot_version = 0;
    SELECT part2, count(), sum(id) FROM deltaLakeLocal('$GOLDEN/time-travel-partition-changes-b/delta') GROUP BY part2 ORDER BY part2 SETTINGS delta_lake_snapshot_version = 1;
    SELECT part2, count(), sum(id) FROM deltaLakeLocal('$GOLDEN/time-travel-partition-changes-b/delta') WHERE part2 = 1 GROUP BY part2 SETTINGS delta_lake_snapshot_version = 1;
"

$CLICKHOUSE_LOCAL -q "$QUERIES"

# Older versions of the vacuumed table refer to deleted files.
$CLICKHOUSE_LOCAL -q "
    SELECT count() FROM deltaLakeLocal('$GOLDEN/snapshot-vacuumed/delta') SETTINGS delta_lake_snapshot_version = 3" 2>&1 | grep -o -m1 'FILE_DOESNT_EXIST'
# A version which does not exist.
$CLICKHOUSE_LOCAL -q "
    SELECT count() FROM deltaLakeLocal('$GOLDEN/time-travel-start-start20-start40/delta') SETTINGS delta_lake_snapshot_version = 3" 2>&1 | grep -o -m1 'DELTA_KERNEL_ERROR'
