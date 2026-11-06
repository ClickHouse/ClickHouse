#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: the Delta Lake kernel (Rust) is not built in the fast test
# no-msan: the Delta Lake kernel (Rust) is not built with MSan

# Reads golden Delta tables of delta-kernel-rs (see data_delta_lake/kernel_golden/README.md)
# with various log histories: updates, deletes, merges, restores, classic, multi-part and V2 checkpoints,
# a corrupted `_last_checkpoint`, a log consisting only of checkpoints, protocol and metadata changes,
# special characters in file names.

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
    basic-with-inserts-deletes-checkpoint \
    basic-with-inserts-merge \
    basic-with-inserts-overwrite-restore \
    basic-with-inserts-updates \
    basic-with-vacuum-protocol-check-feature \
    corrupted-last-checkpoint-kernel \
    multi-part-checkpoint \
    only-checkpoint-files \
    v2-checkpoint-json \
    v2-checkpoint-parquet \
    log-replay-latest-metadata-protocol \
    deltalog-getChanges
do
    check "$table"
done

# These tables are empty: all their files were removed.
for table in \
    log-replay-special-characters \
    log-replay-special-characters-a
do
    QUERIES+="SELECT '$table', count() FROM deltaLakeLocal('$GOLDEN/$table/delta');"
done

QUERIES+="
    SELECT 'Values';
    SELECT * FROM deltaLakeLocal('$GOLDEN/basic-with-inserts-updates/delta') ORDER BY ALL LIMIT 5;
    SELECT count(), sum(id), min(id), max(id) FROM deltaLakeLocal('$GOLDEN/basic-with-inserts-merge/delta');
    SELECT count(), sum(id) FROM deltaLakeLocal('$GOLDEN/multi-part-checkpoint/delta');
    SELECT * FROM deltaLakeLocal('$GOLDEN/v2-checkpoint-parquet/delta') ORDER BY ALL;
    DESCRIBE TABLE deltaLakeLocal('$GOLDEN/log-replay-latest-metadata-protocol/delta') SETTINGS describe_compact_output = 1;
    SELECT * FROM deltaLakeLocal('$GOLDEN/log-replay-latest-metadata-protocol/delta') ORDER BY ALL LIMIT 3;
"

QUERIES+="
    SELECT 'Virtual columns';
    SELECT _file LIKE '%.parquet', count() FROM deltaLakeLocal('$GOLDEN/v2-checkpoint-json/delta') GROUP BY 1;
    SELECT count(DISTINCT _path) > 1 FROM deltaLakeLocal('$GOLDEN/multi-part-checkpoint/delta');
"

$CLICKHOUSE_LOCAL -q "$QUERIES"
