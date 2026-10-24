#!/usr/bin/env bash
# The cached schema and row count of a CSV file must not be reused after `input_format_csv_allow_cr_end_of_line`
# changes: it decides whether a bare CR ends a row.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.csv"
trap 'rm -f "${DATA_FILE}"' EXIT
printf '1\r2\r3\n' > "${DATA_FILE}"
# A cache entry is dropped when the file is not older than the entry, so make the file old.
touch -d '2020-01-01' "${DATA_FILE}"

SETTINGS="input_format_csv_detect_header = 0, schema_inference_use_cache_for_file = 1, use_cache_for_count_from_files = 1, optimize_count_from_files = 1"

# One process, so the second query of each pair can hit what the first one cached.
${CLICKHOUSE_LOCAL} --query "
    SELECT 1, count() FROM file('${DATA_FILE}', 'CSV', 'x String') SETTINGS ${SETTINGS}, input_format_csv_allow_cr_end_of_line = 1;
    SELECT 0, count() FROM file('${DATA_FILE}', 'CSV', 'x String') SETTINGS ${SETTINGS}, input_format_csv_allow_cr_end_of_line = 0;
    DESC file('${DATA_FILE}', 'CSV') SETTINGS ${SETTINGS}, input_format_csv_allow_cr_end_of_line = 1;
    DESC file('${DATA_FILE}', 'CSV') SETTINGS ${SETTINGS}, input_format_csv_allow_cr_end_of_line = 0;" | cut -f1,2
