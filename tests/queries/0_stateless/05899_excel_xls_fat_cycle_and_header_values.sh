#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_excel"

echo "--- a chain of sectors with a short cycle is corrupted, even if the stream ends before the cycle repeats"
# `fat_cycle.xls` is `durations.xls` with the chain of the workbook stream changed from `0 -> 1 -> ... -> 7` to `0 -> 1 -> 2 -> 3 -> 0`.
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA/fat_cycle.xls', XLS)" 2>&1 | grep -o -m1 "a chain of sectors has a cycle"

echo "--- input_format_excel_header accepts only 0, 1 and auto"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$DATA/sample.xls', XLS) SETTINGS input_format_excel_header = 2" 2>&1 | grep -o -m1 "must be 0, 1 or 'auto', got 2"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA/sample.xls', XLS) SETTINGS input_format_excel_header = 0"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA/sample.xls', XLS) SETTINGS input_format_excel_header = 1"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA/sample.xls', XLS) SETTINGS input_format_excel_header = 'auto'"
