#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# An `INSERT` into an Iceberg table with a sort order writes every data file sorted by it and tags it with `sort_order_id`.
# Reverting the sort in the sink makes the sortedness and tag lines fail.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 1000-row blocks, 2500-row file limit.
INSERT_SETTINGS="--allow_insert_into_iceberg=1 --iceberg_insert_max_rows_in_data_file=2500 --max_threads=1 --max_insert_threads=1
    --max_block_size=1000 --max_insert_block_size=1000 --min_insert_block_size_rows=0 --min_insert_block_size_bytes=0"

TABLES=()

# $1: table suffix, $2: columns, $3: PARTITION BY / ORDER BY clauses
create_table()
{
    local table="t_${CLICKHOUSE_DATABASE}_$1"
    TABLES+=("${table}")
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE ${table} ($2) ENGINE = IcebergLocal('${USER_FILES_PATH}/${table}/', 'Parquet') $3"
}

# $1: table suffix, $2: the key a file is sorted by, as an expression sortable by `arraySort`, $3: the sort function
report()
{
    local table="t_${CLICKHOUSE_DATABASE}_$1"
    local sorted tagged
    sorted=$(${CLICKHOUSE_CLIENT} --max_threads=1 --query "
        SELECT count(), countIf(sorted) FROM (SELECT groupArray($2) AS keys, keys = $3(keys) AS sorted FROM ${table} GROUP BY _file)
        FORMAT CSV")
    tagged=$(${CLICKHOUSE_CLIENT} --query "
        SELECT countIf(sort_order_id = 1) FROM system.iceberg_files
        WHERE database = currentDatabase() AND table = '${table}' AND content = 'DATA'")
    echo "$1: files,sorted=${sorted} tagged=${tagged}"
}

# $1: table suffix, $2: columns, $3: clauses, $4: INSERT SELECT, $5: key, $6: sort function, $7: extra settings
run_case()
{
    create_table "$1" "$2" "$3"
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} ${INSERT_SETTINGS} $7 --query "INSERT INTO t_${CLICKHOUSE_DATABASE}_$1 $4"
    report "$1" "$5" "$6"
}

RANDOM_X="cityHash64(number) % 100000"

run_case descending "x Int64" "ORDER BY x" "SELECT 10000 - number FROM numbers(10000)" "x" "arraySort"
# Three out-of-order blocks below the file limit become one sorted file.
run_case small "x Int64" "ORDER BY x" "SELECT 2400 - number FROM numbers(2400)" "x" "arraySort"
run_case partitioned "x Int64, p Int32" "PARTITION BY p ORDER BY x" \
    "SELECT ${RANDOM_X}, number % 3 FROM numbers(10000)" "x" "arraySort"
run_case two_columns "a Int32, b Int32" "ORDER BY (a, b)" \
    "SELECT cityHash64(number) % 10, cityHash64(number, 1) % 1000 FROM numbers(10000)" "(a, b)" "arraySort"
run_case desc "x Int64" "ORDER BY x DESC" "SELECT ${RANDOM_X} FROM numbers(10000)" "x" "arrayReverseSort"
# A small byte limit ends files before the row limit; they are still sorted and tagged.
run_case byte_limit "x Int64" "ORDER BY x" "SELECT ${RANDOM_X} FROM numbers(10000)" "x" "arraySort" "--iceberg_insert_max_bytes_in_data_file=12000"
# `CREATE TABLE` writes `nulls-last` into the sort order for `ASC` and `nulls-first` for `DESC`.
run_case nullable "x Nullable(Int64)" "ORDER BY x" \
    "SELECT if(number % 7 = 0, NULL, ${RANDOM_X}) FROM numbers(10000)" "(isNull(x), ifNull(x, 0))" "arraySort"
run_case nullable_desc "x Nullable(Int64)" "ORDER BY x DESC" \
    "SELECT if(number % 7 = 0, NULL, ${RANDOM_X}) FROM numbers(10000)" "(isNull(x), ifNull(x, 0))" "arrayReverseSort"
# Partitions together fill the sort buffer before any file is full: the largest pending file is written early.
run_case sort_buffer_limit "x Int64, p Int32" "PARTITION BY p ORDER BY x" \
    "SELECT ${RANDOM_X}, number % 3 FROM numbers(10000)" "x" "arraySort" "--iceberg_insert_max_sort_buffer_bytes=50000"
# Floating-point and UUID keys are sorted but not tagged: Iceberg orders them differently from ClickHouse.
run_case float "x Float64" "ORDER BY x" "SELECT ${RANDOM_X} / 7 FROM numbers(10000)" "x" "arraySort"
run_case uuid "x UUID" "ORDER BY x" "SELECT generateUUIDv4(number) FROM numbers(10000)" "x" "arraySort"

# Sort orders `CREATE TABLE` cannot write: $1: table suffix, $2: columns, $3: ORDER BY clause, $4: sed script for the
# metadata, $5: rows to insert. The table is reattached to reread the metadata.
run_edited_case()
{
    create_table "$1" "$2" "$3"
    local table="t_${CLICKHOUSE_DATABASE}_$1"
    sed -i -e "$4" "${USER_FILES_PATH}/${table}/metadata/v1.metadata.json"
    ${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${table}; ATTACH TABLE ${table}"
    # shellcheck disable=SC2086
    ${CLICKHOUSE_CLIENT} ${INSERT_SETTINGS} --use_iceberg_metadata_files_cache=0 --query "INSERT INTO ${table} $5"
}

# Iceberg truncates `binary` by bytes, ClickHouse by UTF-8 characters: such files are not tagged. Both rows have one
# leading byte, so Iceberg orders them by `y`; ClickHouse orders them by the character. The transform is upper-cased:
# Iceberg transform names are case-insensitive.
run_edited_case truncated_binary "b String, y Int32" "ORDER BY (icebergTruncate(1, b), y)" \
    's/"type" *: *"string"/"type" : "binary"/; s/"truncate\[1\]"/"TRUNCATE[1]"/' \
    "SELECT unhex(h), y FROM values('h String, y Int32', ('C3A9', 1), ('C3AA', 0))"
report truncated_binary "y" "arraySort"
# Iceberg orders by `void(a)`, a constant, and then by `b`; ClickHouse evaluates it as `tuple(a)`: not tagged.
run_edited_case void_transform "a Int32, b Int32" "ORDER BY (a, b)" '0,/"identity"/s//"void"/' \
    "SELECT a, b FROM values('a Int32, b Int32', (0, 1), (1, 0))"
report void_transform "b" "arraySort"

for table in "${TABLES[@]}"; do
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}"
    rm -rf "${USER_FILES_PATH:?}/${table}"
done
