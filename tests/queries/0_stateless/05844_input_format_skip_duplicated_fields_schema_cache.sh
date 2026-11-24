#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A schema inferred with input_format_skip_duplicated_fields = 1 must not be reused by a query that disables it.
# The file is aged: the schema cache ignores an entry that is not older than the file.
F="${CLICKHOUSE_TEST_UNIQUE_NAME}.jsonl"
echo '{"url":"a","url":1,"name":"x"}' > "$F"
touch -d "2000-01-01 00:00:00" "$F"

$CLICKHOUSE_LOCAL -m -q "
    DESC file('$F') SETTINGS input_format_skip_duplicated_fields = 1;
    DESC file('$F') SETTINGS input_format_skip_duplicated_fields = 0;" 2>&1 | grep -oE 'Nullable\(String\)|CANNOT_EXTRACT_TABLE_STRUCTURE'

# Nor by a query with another input_format_column_name_matching_mode.
echo '{"a":1,"A":"x"}' > "$F"
touch -d "2000-01-01 00:00:00" "$F"

$CLICKHOUSE_LOCAL -m -q "
    DESC file('$F') SETTINGS input_format_skip_duplicated_fields = 1, input_format_column_name_matching_mode = 'ignore_case';
    DESC file('$F') SETTINGS input_format_skip_duplicated_fields = 1, input_format_column_name_matching_mode = 'auto';" | cut -f1

rm -f "$F"
