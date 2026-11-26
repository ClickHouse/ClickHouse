#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Boundary cases that previously wrapped around an unsigned integer (which UBSan now reports).

echo "-- non-adaptive index granularity of UInt64 max"
$CLICKHOUSE_LOCAL -m -q "
CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY n
SETTINGS index_granularity = 18446744073709551615, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO t VALUES (1);
SELECT * FROM t;
"

echo "-- the number of processing units of parallel parsing and formatting"
$CLICKHOUSE_LOCAL -q "SELECT * FROM format(CSV, 'x UInt8', '1\n') SETTINGS max_threads = 1, max_parsing_threads = 18446744073709551615, input_format_parallel_parsing = 1, max_memory_usage = 0, max_memory_usage_for_user = 0"
$CLICKHOUSE_LOCAL -q "SELECT 1 SETTINGS max_threads = 18446744073709551615, output_format_parallel_formatting = 1 FORMAT CSV"

echo "-- POLYGON_INDEX_EACH dictionary with an empty multipolygon"
$CLICKHOUSE_LOCAL -m -q "
CREATE TABLE p (key Array(Array(Array(Tuple(Float64, Float64)))), name String) ENGINE = Memory;
INSERT INTO p VALUES ([], 'empty');
CREATE DICTIONARY d (key Array(Array(Array(Tuple(Float64, Float64)))), name String)
PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE 'p')) LIFETIME(0) LAYOUT(POLYGON_INDEX_EACH());
SELECT dictHas('d', (5., 5.));
"
