#!/usr/bin/env bash
# The cached schema and row count of a CustomSeparated file must not be reused after a setting that decides
# how the rows are read has changed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -f "${DATA_FILE}"_*' EXIT

SETTINGS="input_format_custom_detect_header = 0, format_custom_escaping_rule = 'Escaped', format_custom_field_delimiter = '\t', format_custom_row_after_delimiter = '\n', schema_inference_use_cache_for_file = 1, use_cache_for_count_from_files = 1, optimize_count_from_files = 1"

# check <setting> <format> <file content> [structure]: queries with the setting enabled, then with it disabled, in one process,
# so that the second pair of queries can hit what the first one cached. The file is aged with `touch -d`, because
# a cache entry is dropped when the file is not older than the entry.
check()
{
    local setting=$1 format=$2 file="${DATA_FILE}_$1_$2" structure=${4:-x String}
    printf "$3" > "${file}"
    touch -d '2020-01-01' "${file}"
    echo "-- ${setting} ${format}"
    ${CLICKHOUSE_LOCAL} -m --ignore-error --query "
        SELECT count() FROM file('${file}', '${format}', '${structure}') SETTINGS ${SETTINGS}, ${setting} = 1 FORMAT Null;
        DESC file('${file}', '${format}') SETTINGS ${SETTINGS}, ${setting} = 1 FORMAT Null;
        SELECT count() FROM file('${file}', '${format}', '${structure}') SETTINGS ${SETTINGS}, ${setting} = 0;
        DESC file('${file}', '${format}') SETTINGS ${SETTINGS}, ${setting} = 0;" 2>&1 \
        | grep -E '^[0-9]+$|^c[0-9]+	|CANNOT_EXTRACT_TABLE_STRUCTURE|CANNOT_PARSE_INPUT_ASSERTION_FAILED' | sed -E 's/.*(CANNOT_EXTRACT_TABLE_STRUCTURE|CANNOT_PARSE_INPUT_ASSERTION_FAILED).*/\1/' | cut -f1,2
}

check input_format_custom_allow_variable_number_of_columns CustomSeparated '1\t2\n3\n4\t5\t6\n'
check input_format_custom_skip_trailing_empty_lines CustomSeparated '1\t2\n\n\n'
check input_format_custom_allow_variable_number_of_columns CustomSeparatedWithNames 'c1\tc2\n1\n2\t3\n'
# The header and the types row decide the schema of WithNamesAndTypes, but the rows are still read according to the setting.
check input_format_custom_allow_variable_number_of_columns CustomSeparatedWithNamesAndTypes 'c1\tc2\nString\tString\nfull\trow\nshort\n' 'c1 String, c2 String'
