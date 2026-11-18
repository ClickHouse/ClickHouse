#!/usr/bin/env bash

# The format string for the result set of the `Template` input format must contain `${data}`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$dir"
printf 'prefix ${data}' > "$dir/resultset_ok.format"
printf 'prefix' > "$dir/resultset_without_data.format"
printf '${x:CSV}' > "$dir/row.format"

printf 'prefix 1' | $CLICKHOUSE_LOCAL --input-format Template --structure 'x UInt64' \
    --format_template_resultset "$dir/resultset_ok.format" --format_template_row "$dir/row.format" -q "SELECT * FROM table"

printf 'prefix' | $CLICKHOUSE_LOCAL --input-format Template --structure 'x UInt64' \
    --format_template_resultset "$dir/resultset_without_data.format" --format_template_row "$dir/row.format" -q "SELECT * FROM table" 2>&1 \
    | grep -o -m1 'INVALID_TEMPLATE_FORMAT'

rm -rf "$dir"
