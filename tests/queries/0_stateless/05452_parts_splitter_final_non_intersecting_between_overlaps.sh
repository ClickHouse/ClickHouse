#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `FINAL` reads the marks of a merged part between two other parts inside its key range without merging.

CLICKHOUSE_CLIENT_TEST_LOGS=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=test/g')

SETTINGS="split_parts_ranges_into_intersecting_and_non_intersecting_final = 1, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0"

function print_split()
{
    $CLICKHOUSE_CLIENT_TEST_LOGS -q "SELECT count() FROM t FINAL SETTINGS $SETTINGS" 2>&1 >/dev/null \
        | grep -o -E "(Non intersecting|Intersecting) ranges in data parts .*"
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM t FINAL SETTINGS $SETTINGS"
}

# One part of level 1 with keys 0..99999 in 1000 granules of 100 rows, and parts with newer versions of the keys inside its range.
function create_table()
{
    $CLICKHOUSE_CLIENT -m -q "
        DROP TABLE IF EXISTS t;
        CREATE TABLE t (key UInt64, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY key
        SETTINGS index_granularity = 100, index_granularity_bytes = '10Mi';
        SYSTEM STOP MERGES t;
        SET optimize_on_insert = 1, max_insert_threads = 1, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;
        INSERT INTO t SELECT number, 1 FROM numbers(100000);
        INSERT INTO t SELECT $1 + number, 2 FROM numbers($2);
        INSERT INTO t SELECT $3 + number, 2 FROM numbers($4);
    "
}

echo "Two parts in granules 200 and 800, granules 201..798 between them are not merged"
create_table 20000 100 80000 100
print_split

echo "Two parts in granule 200, no granule between them"
create_table 20000 50 20050 50
print_split

$CLICKHOUSE_CLIENT -q "DROP TABLE t"
