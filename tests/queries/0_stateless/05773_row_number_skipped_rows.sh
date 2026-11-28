#!/usr/bin/env bash
# Tags: no-fasttest
# Test that _row_number keeps the physical row number of each emitted row when some
# rows are skipped by `input_format_allow_errors_num` (CSV) or by
# `format_regexp_skip_unmatched` (Regexp), with serial parsing.
# https://github.com/ClickHouse/ClickHouse/issues/131756

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FILE_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}_row_number_skipped.csv"

# 5 lines, line 1 ('garbage') is bad for both readers below and gets skipped.
${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${FILE_NAME}', RawBLOB) SELECT '1\ngarbage\n2\n3\n4'"

echo "--- CSV: _row_number when a row is skipped by input_format_allow_errors_num"
${CLICKHOUSE_CLIENT} --query "SELECT _row_number, x FROM file('${FILE_NAME}', CSV, 'x UInt32') SETTINGS input_format_parallel_parsing = 0, input_format_allow_errors_num = 1"

echo "--- Regexp: _row_number when a line is skipped by format_regexp_skip_unmatched"
${CLICKHOUSE_CLIENT} --query "SELECT _row_number, x FROM file('${FILE_NAME}', Regexp, 'x UInt32') SETTINGS input_format_parallel_parsing = 0, format_regexp = '^([0-9]+)$', format_regexp_escaping_rule = 'Raw', format_regexp_skip_unmatched = 1"
