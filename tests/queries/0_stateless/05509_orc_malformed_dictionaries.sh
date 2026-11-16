#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: ORC is not built in fasttest, the test uses S3.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./orc_malformed.lib
. "$CUR_DIR"/orc_malformed.lib

DATA="$CUR_DIR/data_orc"

# A dictionary without entries referenced by the non-null values.
orc_check empty "$DATA/malformed_dictionary_empty.orc" 's Nullable(String)' "SELECT count(), countIf(s IS NULL) FROM SOURCE" \
    'ORC dictionary index 0 at row 0 is out of range \[0, 0\)'

# A valid empty dictionary of a column with only nulls.
FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_nulls.orc"
trap 'rm -f "$FILE"' EXIT
$CLICKHOUSE_LOCAL -q "SELECT CAST(NULL, 'Nullable(String)') AS s FROM numbers(5) INTO OUTFILE '$FILE' TRUNCATE FORMAT ORC SETTINGS output_format_orc_dictionary_key_size_threshold = 1"
orc_check nulls "$FILE" 's Nullable(String)' "SELECT count(), countIf(s IS NULL) FROM SOURCE"
