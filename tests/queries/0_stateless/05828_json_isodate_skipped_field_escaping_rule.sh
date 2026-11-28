#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The MongoDB shell `ISODate("...")` wrapper is accepted by a declared `DateTime64` with the `JSON`
# escaping rule, so a skipped field (an unknown column, `${:JSON}` in `Template`, or the optimized
# `count()`) must accept it too.

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.txt"
ROW_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_row.txt"
trap 'rm -f "$DATA_FILE" "$ROW_FILE"' EXIT

printf '1,ISODate("2024-05-29T23:16:12.256Z")\n2,new ISODate("2024-05-29T23:16:13Z")\n' > "$DATA_FILE"
printf '${id:JSON},${:JSON}' > "$ROW_FILE"

CUSTOM_SETTINGS="format_custom_escaping_rule = 'JSON', format_custom_field_delimiter = ',', format_custom_row_after_delimiter = '\n', input_format_custom_detect_header = 0"

echo "CustomSeparated, declared"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_FILE', CustomSeparated, 'id UInt8, ts DateTime64(3, \'UTC\')') SETTINGS $CUSTOM_SETTINGS"

echo "CustomSeparated, skipped unknown field"
$CLICKHOUSE_LOCAL -q "SELECT * FROM format(CustomSeparatedWithNames, 'id UInt8', \$\$\"id\",\"ts\"
1,ISODate(\"2024-05-29T23:16:12.256Z\")
\$\$) SETTINGS $CUSTOM_SETTINGS, input_format_skip_unknown_fields = 1"

echo "CustomSeparated, count"
for optimize in 0 1; do
    $CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA_FILE', CustomSeparated, 'id UInt8, ts DateTime64(3)') SETTINGS $CUSTOM_SETTINGS, optimize_count_from_files = $optimize"
done

echo "Template, skipped field"
printf '1,ISODate("2024-05-29T23:16:12.256Z")' | $CLICKHOUSE_LOCAL --input-format Template --structure 'id UInt8' --format_template_row "$ROW_FILE" -q "SELECT * FROM table"
