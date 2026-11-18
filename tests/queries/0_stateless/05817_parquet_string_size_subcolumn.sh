#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: writes and reads Parquet files

# `length`, `empty` and `notEmpty` of a `String` column of a Parquet file read the `size` subcolumn,
# which the Parquet reader decodes from the lengths of the values, without the values.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05817_parquet_string_size_XXXXXX")
trap 'rm -rf "${DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL} --input_format_parquet_use_native_reader_v3=1 --optimize_move_to_prewhere=1)
ON=(--optimize_functions_to_subcolumns=1)
OFF=(--optimize_functions_to_subcolumns=0)

DATA="SELECT
    repeat('x', number % 17) AS s,
    if(number % 5 = 0, NULL, repeat('y', number % 7)) AS n,
    tuple(repeat('z', number % 3), if(number % 4 = 0, NULL, toString(number))) AS t,
    toString(number % 10) AS low
    FROM numbers(10000)"
STRUCTURE="s String, n Nullable(String), t Tuple(a String, b Nullable(String)), low String"

# Dictionary-encoded and PLAIN-encoded strings.
"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DIR}/dict.parquet', Parquet, '${STRUCTURE}') ${DATA}
    SETTINGS output_format_parquet_row_group_size = 3000, engine_file_truncate_on_insert = 1"
"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DIR}/plain.parquet', Parquet, '${STRUCTURE}') ${DATA}
    SETTINGS output_format_parquet_row_group_size = 3000, output_format_parquet_max_dictionary_size = 0, engine_file_truncate_on_insert = 1"

echo "-- results identical with and without the optimization"
for file in dict plain; do
    queries=(
        "SELECT sum(length(s)), countIf(empty(s)), countIf(notEmpty(s)) FROM file('${DIR}/${file}.parquet', Parquet, '${STRUCTURE}')"
        "SELECT sum(length(n)), countIf(empty(n)), countIf(notEmpty(n)), countIf(isNull(length(n))) FROM file('${DIR}/${file}.parquet', Parquet, '${STRUCTURE}')"
        "SELECT sum(length(t.a)), sum(length(t.b)), countIf(empty(t.b)) FROM file('${DIR}/${file}.parquet', Parquet, '${STRUCTURE}')"
        "SELECT count(), avg(length(s)) FROM file('${DIR}/${file}.parquet', Parquet, '${STRUCTURE}') WHERE notEmpty(s)"
        "SELECT length(low) AS l, count() FROM file('${DIR}/${file}.parquet', Parquet, '${STRUCTURE}') WHERE notEmpty(low) GROUP BY l ORDER BY l"
        "SELECT count() FROM file('${DIR}/${file}.parquet', Parquet, '${STRUCTURE}') WHERE length(s) > 10 AND notEmpty(n)"
        "SELECT sum(length(s)) FROM file('${DIR}/${file}.parquet', Parquet, '${STRUCTURE}') WHERE length(s) % 3 = 0"
        "SELECT s, length(s) FROM file('${DIR}/${file}.parquet', Parquet, '${STRUCTURE}') WHERE notEmpty(s) ORDER BY s LIMIT 3"
        "SELECT s.size, n.size, t.b.size FROM file('${DIR}/${file}.parquet', Parquet, '${STRUCTURE}') ORDER BY s.size, n.size, t.b.size LIMIT 5"
        "SELECT s.size, s FROM file('${DIR}/${file}.parquet', Parquet, '${STRUCTURE}') ORDER BY s LIMIT 2"
        "SELECT length(s), notEmpty(n) FROM file('${DIR}/${file}.parquet', Parquet, '${STRUCTURE}') ORDER BY low DESC, s LIMIT 3 SETTINGS query_plan_optimize_lazy_materialization = 1"
    )
    for query in "${queries[@]}"; do
        diff <("${LOCAL[@]}" "${ON[@]}" --query "${query}") <("${LOCAL[@]}" "${OFF[@]}" --query "${query}") && echo "OK"
    done
done

echo "-- the reading step reads only the sizes"
"${LOCAL[@]}" "${ON[@]}" --query "
    EXPLAIN actions = 1 SELECT sum(length(s)), countIf(notEmpty(n)) FROM file('${DIR}/dict.parquet', Parquet, '${STRUCTURE}')" \
    | grep -A1 "ReadFromFile" | grep -o "Output: .*"

echo "-- a column with DEFAULT that is missing in the file"
"${LOCAL[@]}" "${ON[@]}" --query "
    CREATE TABLE t_default (s String, d String DEFAULT 'abc') ENGINE = File(Parquet, '${DIR}/dict.parquet');
    SELECT sum(length(d)), countIf(empty(d)) FROM t_default;
    SELECT count() FROM t_default WHERE notEmpty(d);
" --input_format_parquet_allow_missing_columns=1

echo "-- a hive partition column"
mkdir -p "${DIR}/hive/p=abcd"
cp "${DIR}/dict.parquet" "${DIR}/hive/p=abcd/data.parquet"
"${LOCAL[@]}" "${ON[@]}" --use_hive_partitioning=1 --query "
    SELECT sum(length(p)), countIf(notEmpty(p)), sum(length(s)) FROM file('${DIR}/hive/*/*.parquet')"
