#!/usr/bin/env bash
# The cached schema and row count of a CSV or TSV file must not be reused after a setting that decides how the text
# is split into rows and values has changed. The settings of the CSV escaping rule also matter to the formats that
# read their fields with it (CustomSeparated, Template).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -f "${DATA_FILE}"_*' EXIT

SETTINGS="input_format_csv_detect_header = 0, input_format_tsv_detect_header = 0, input_format_custom_detect_header = 0, format_custom_escaping_rule = 'CSV', format_custom_field_delimiter = ',', format_template_rows_between_delimiter = '', schema_inference_use_cache_for_file = 1, use_cache_for_count_from_files = 1, optimize_count_from_files = 1"

# check <setting> <format> <file content> [more settings]: queries with the setting enabled, then with it disabled,
# in one process, so that the second pair of queries can hit what the first one cached. The file is aged with
# `touch -d`, because a cache entry is dropped when the file is not older than the entry.
# The output is what the queries with the setting disabled return: the row count, the schema and the error name.
check()
{
    local setting=$1 format=$2 file="${DATA_FILE}_$1" more=""
    if [ -n "$4" ]; then more=", $4"; fi
    printf "$3" > "${file}"
    touch -d '2020-01-01' "${file}"
    echo "-- ${setting} ${format}"
    ${CLICKHOUSE_LOCAL} -m --ignore-error --query "
    SELECT count() FROM file('${file}', '${format}', 'x String') SETTINGS ${SETTINGS}${more}, ${setting} = 1 FORMAT Null;
    DESC file('${file}', '${format}') SETTINGS ${SETTINGS}${more}, ${setting} = 1 FORMAT Null;
    SELECT count() FROM file('${file}', '${format}', 'x String') SETTINGS ${SETTINGS}${more}, ${setting} = 0;
    DESC file('${file}', '${format}') SETTINGS ${SETTINGS}${more}, ${setting} = 0;" 2>&1 \
    | grep -E $'^[0-9]+$|^c[0-9]+\t|CANNOT_EXTRACT_TABLE_STRUCTURE' | sed -E 's/.*(CANNOT_EXTRACT_TABLE_STRUCTURE).*/\1/' | cut -f1,2
}

check input_format_csv_allow_cr_end_of_line CSV '1\r2\r3\n'
check format_csv_allow_single_quotes CSV "'a,b',c\n"
check format_csv_allow_double_quotes CSV '"a,b",c\n'
check input_format_csv_trim_whitespaces CSV ' 1 , 2 \n'
check input_format_csv_allow_variable_number_of_columns CSV '1,2\n1\n'
check input_format_csv_skip_trailing_empty_lines CSV '1\n\n\n'
check input_format_csv_allow_whitespace_or_tab_as_delimiter CSV ' "a,b",c\n'
check input_format_tsv_crlf_end_of_line TSV '1\t2\r\n3\t4\r\n'
check input_format_tsv_allow_variable_number_of_columns TSV '1\t2\n1\n'
check input_format_tsv_skip_trailing_empty_lines TSV '1\n\n\n'

# The settings of the CSV escaping rule are part of the key of every format that uses that rule.
check format_csv_allow_single_quotes CustomSeparated "'a,b',c\n"
check format_csv_allow_double_quotes CustomSeparated '"a,b",c\n'
check input_format_csv_allow_cr_end_of_line CustomSeparated '1\r2\r3\r' "format_custom_row_after_delimiter = '\r', format_custom_row_between_delimiter = ''"
check format_csv_allow_single_quotes CustomSeparatedWithNamesAndTypes "'a,b',c\nString,String\nx,y\n"
check format_csv_allow_double_quotes CustomSeparatedWithNamesAndTypes '"a,b",c\nString,String\nx,y\n'

printf '${c1:CSV}\n' > "${DATA_FILE}_row_lf"
printf '${c1:CSV}\r' > "${DATA_FILE}_row_cr"
check input_format_csv_trim_whitespaces Template '1  \n' "format_template_row = '${DATA_FILE}_row_lf'"
check input_format_csv_allow_cr_end_of_line Template '1\r2\r3\r' "format_template_row = '${DATA_FILE}_row_cr'"
