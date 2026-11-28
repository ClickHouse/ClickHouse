#!/usr/bin/env bash
# The cached schema and row count of a JSONCompactEachRow file must not be reused after
# `input_format_json_compact_allow_variable_number_of_columns` has changed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -f "${DATA_FILE}"_*' EXIT

SETTINGS="schema_inference_use_cache_for_file = 1, use_cache_for_count_from_files = 1, optimize_count_from_files = 1"
SETTING=input_format_json_compact_allow_variable_number_of_columns

# check <format> <file content>: queries with the setting enabled, then with it disabled, in one process,
# so that the second pair of queries can hit what the first one cached. The file is aged with `touch -d`, because
# a cache entry is dropped when the file is not older than the entry.
check()
{
    local format=$1 file="${DATA_FILE}_$1"
    printf "$2" > "${file}"
    touch -d '2020-01-01' "${file}"
    echo "-- ${format}"
    ${CLICKHOUSE_LOCAL} -m --query "
        SELECT count() FROM file('${file}', '${format}', 'x String') SETTINGS ${SETTINGS}, ${SETTING} = 1 FORMAT Null;
        DESC file('${file}', '${format}') SETTINGS ${SETTINGS}, ${SETTING} = 1 FORMAT Null;
        SELECT count() FROM file('${file}', '${format}', 'x String') SETTINGS ${SETTINGS}, ${SETTING} = 0;
        DESC file('${file}', '${format}') SETTINGS ${SETTINGS}, ${SETTING} = 0;" 2>&1 \
        | grep -E '^[0-9]+$|^c[0-9]+	|CANNOT_EXTRACT_TABLE_STRUCTURE|INCORRECT_DATA' | sed -E 's/.*(CANNOT_EXTRACT_TABLE_STRUCTURE|INCORRECT_DATA).*/\1/' | cut -f1,2
}

check JSONCompactEachRow '[1,2]\n[1]\n[1,2,3]\n'
check JSONCompactStringsEachRow '["1","2"]\n["1"]\n["1","2","3"]\n'
check JSONCompactEachRowWithNames '["c1","c2"]\n[1,2]\n[1]\n'
# The header and the types row decide the schema of WithNamesAndTypes, and count() does not depend on the setting.
check JSONCompactEachRowWithNamesAndTypes '["c1","c2"]\n["String","String"]\n["a","b"]\n["c"]\n'
