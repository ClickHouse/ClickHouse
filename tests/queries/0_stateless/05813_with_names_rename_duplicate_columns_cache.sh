#!/usr/bin/env bash
# `input_format_with_names_rename_duplicate_columns` changes whether the first row is consumed as a header and which
# names are inferred, so it must be a part of the keys of the schema cache and of the cached number of rows.
# Each order of the two modes runs in its own process, and the cached result of the second query is compared
# with a read of the same file without the cache.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$DIR"
trap 'rm -rf "$DIR"' EXIT

# Files must be older than the queries, otherwise their schemas and row counts are not cached.
printf 'a,a\nfoo,bar\n' > "$DIR/data.csv"
printf 'a\ta\nfoo\tbar\n' > "$DIR/data.tsv"
printf 'a\ta\nfoo\tbar\n' > "$DIR/data.custom"
printf 'a\ta\nString\tString\nfoo\tbar\n' > "$DIR/types.custom"
printf '["a","a"]\n["String","String"]\n["foo","bar"]\n' > "$DIR/types.json"
printf '\x02\x01a\x01a\x06String\x06String\x03foo\x03bar' > "$DIR/types.rowbinary"
touch -t 200001010000 "$DIR"/*

CSV_SETTINGS="input_format_skip_unknown_fields = 1, input_format_csv_allow_variable_number_of_columns = 1"
TSV_SETTINGS="input_format_skip_unknown_fields = 1, input_format_tsv_allow_variable_number_of_columns = 1"
CUSTOM_SETTINGS="input_format_skip_unknown_fields = 1, input_format_custom_allow_variable_number_of_columns = 1, input_format_custom_detect_header = 1, format_custom_escaping_rule = 'CSV', format_custom_field_delimiter = '\t', format_custom_row_after_delimiter = '\n'"

# The number of rows with a header detected automatically: 1 with renaming (the header is consumed), 2 without it.
count_rows()
{
    local format=$1 file=$2 settings=$3 first=$4 second=$5
    local query="SELECT count() FROM file('$file', '$format', 'a_1 String') SETTINGS $settings"
    ${CLICKHOUSE_LOCAL} --multiquery --query "
        $query, input_format_with_names_rename_duplicate_columns = $first;
        $query, input_format_with_names_rename_duplicate_columns = $second;
        $query, input_format_with_names_rename_duplicate_columns = $second, use_cache_for_count_from_files = 0;"
}

for case in "CSV $DIR/data.csv $CSV_SETTINGS" "TSV $DIR/data.tsv $TSV_SETTINGS" "CustomSeparated $DIR/data.custom $CUSTOM_SETTINGS"
do
    read -r format file settings <<< "$case"
    for order in "1 0" "0 1"
    do
        echo "--- count $format, renaming $order ---"
        read -r first second <<< "$order"
        count_rows "$format" "$file" "$settings" "$first" "$second"
    done
done

# The schema: names are renamed with the setting, and duplicates are rejected without it.
describe()
{
    local format=$1 file=$2 settings=$3 first=$4 second=$5
    local query="DESC file('$file', '$format') SETTINGS $settings"
    ${CLICKHOUSE_LOCAL} --multiquery --ignore-error --query "
        $query, input_format_with_names_rename_duplicate_columns = $first;
        $query, input_format_with_names_rename_duplicate_columns = $second;" 2>&1 | grep -E '^(a|Code: 636)' | sed -E 's/^Code: 636.*/rejected/' | cut -f1,2
}

for case in "CSVWithNames $DIR/data.csv $CSV_SETTINGS" \
    "CustomSeparatedWithNamesAndTypes $DIR/types.custom $CUSTOM_SETTINGS" \
    "JSONCompactEachRowWithNamesAndTypes $DIR/types.json input_format_skip_unknown_fields=1" \
    "RowBinaryWithNamesAndTypes $DIR/types.rowbinary input_format_skip_unknown_fields=1"
do
    read -r format file settings <<< "$case"
    for order in "1 0" "0 1"
    do
        echo "--- DESC $format, renaming $order ---"
        read -r first second <<< "$order"
        describe "$format" "$file" "$settings" "$first" "$second"
    done
done
