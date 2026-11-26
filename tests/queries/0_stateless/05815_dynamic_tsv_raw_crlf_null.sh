#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A null marker in a TabSeparatedRaw field keeps its trailing CRLF terminator when read, so the
# Dynamic null check must consume it. The null must be detected with input_format_null_as_default=0,
# the same as Nullable(String), while a non-null value keeps its raw bytes including the carriage return.

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.tsv"
SETTINGS="input_format_tsv_crlf_end_of_line=1, input_format_null_as_default=0"

printf '1\t\\N\r\n2\thello\r\n' > "$DATA_FILE"

echo 'Dynamic null detection'
$CLICKHOUSE_LOCAL -q "SELECT id, isNull(d), dynamicType(d) FROM file('$DATA_FILE', TabSeparatedRaw, 'id UInt32, d Dynamic') ORDER BY id SETTINGS $SETTINGS"
echo 'Nullable(String) oracle'
$CLICKHOUSE_LOCAL -q "SELECT id, isNull(d) FROM file('$DATA_FILE', TabSeparatedRaw, 'id UInt32, d Nullable(String)') ORDER BY id SETTINGS $SETTINGS"
echo 'Non-null value keeps the carriage return'
$CLICKHOUSE_LOCAL -q "SELECT hex(d::String) FROM file('$DATA_FILE', TabSeparatedRaw, 'id UInt32, d Dynamic') WHERE id = 2 SETTINGS $SETTINGS"

echo 'Dynamic, custom null representation'
printf '1\tmissing\r\n2\tmissing_suffix\r\n' > "$DATA_FILE"
$CLICKHOUSE_LOCAL -q "SELECT id, isNull(d), dynamicType(d) FROM file('$DATA_FILE', TabSeparatedRaw, 'id UInt32, d Dynamic') ORDER BY id SETTINGS $SETTINGS, format_tsv_null_representation='missing'"

rm -f "$DATA_FILE"
