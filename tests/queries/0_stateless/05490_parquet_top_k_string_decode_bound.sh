#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: writes and reads Parquet files, and uses S3 (MinIO)

# ORDER BY <String column> LIMIT over Parquet with a PREWHERE that rejects the empty string: the values beyond the
# top-K threshold are decoded as '' without copying them, and PREWHERE drops their rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05490_parquet_topk_bound_XXXXXX")
DATA_DIR="${CLICKHOUSE_DATABASE}_05490"
S3_P="http://localhost:11111/test/${CLICKHOUSE_TEST_UNIQUE_NAME}_p.parquet"
trap 'rm -rf "${DIR}" "${USER_FILES_PATH:?}/${DATA_DIR}"' EXIT

# Each clickhouse-local call runs a whole group of queries: starting the process is the slow part.
LOCAL=(${CLICKHOUSE_LOCAL}
    --query_plan_max_limit_for_top_k_optimization=1000
    --input_format_parquet_filter_push_down=1
    --input_format_parquet_preserve_order=0
    --input_format_parquet_dictionary_filter_push_down=1048576
    --use_query_condition_cache=0
    --use_columns_cache=0
    --optimize_move_to_prewhere=1
    --apply_string_filters_during_scan=1
    --max_block_size=65409)
ST="max_threads = 1, max_parsing_threads = 1"
MT="max_threads = 4, max_parsing_threads = 4"
ON="use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 0"
OFF="use_top_k_dynamic_filtering = 0, use_top_k_dynamic_filtering_for_variable_length_types = 0"

# 20 row groups of 1000 rows; every row group holds values from the whole range, so neither the statistics nor the
# dictionary can skip it. `s` is unique and never empty, `e` is '' in one of ten rows, `t` repeats each value 4 times,
# `r` has its 100 smallest values once and 300 others in every row group (a dictionary small enough for the
# `ColumnReplicated` decoding of a single-column PREWHERE),
# `k` is unique. p.parquet is PLAIN, d.parquet dictionary-encoded, dl.parquet and db.parquet DELTA_LENGTH_BYTE_ARRAY and
# DELTA_BYTE_ARRAY.
"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DIR}/p.parquet', Parquet)
    SELECT leftPad(toString((number * 7919) % 20000), 6, '0') AS s, if(number % 10 = 3, '', s) AS e,
        leftPad(toString(intDiv((number * 7919) % 20000, 4)), 6, '0') AS t,
        leftPad(toString(if((number * 7919) % 20000 < 100, (number * 7919) % 20000, 100 + (number * 7919) % 300)), 6, '0') AS r,
        number AS k
    FROM numbers(20000)
    SETTINGS output_format_parquet_row_group_size = 1000, output_format_parquet_max_dictionary_size = 0, engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION file('${DIR}/d.parquet', Parquet) SELECT * FROM file('${DIR}/p.parquet')
    SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION s3('${S3_P}', 'test', 'testtest', 'RawBLOB') SELECT * FROM file('${DIR}/p.parquet', 'RawBLOB')
    SETTINGS s3_truncate_on_insert = 1;"

python3 -c "
import pyarrow.parquet as pq
table = pq.read_table('${DIR}/p.parquet')
for name, encoding in (('dl', 'DELTA_LENGTH_BYTE_ARRAY'), ('db', 'DELTA_BYTE_ARRAY')):
    pq.write_table(table, '${DIR}/' + name + '.parquet', use_dictionary=False, row_group_size=1000,
                   column_encoding={'s': encoding, 'e': encoding, 't': encoding})
"

P="file('${DIR}/p.parquet')"
D="file('${DIR}/d.parquet')"
DL="file('${DIR}/dl.parquet')"
DB="file('${DIR}/db.parquet')"
# `d` is not in the files: it is computed from `s` for every row the source returns.
TABLES="CREATE TABLE tp (s String, d UInt8 DEFAULT intDiv(100, length(s))) ENGINE = File(Parquet, '${DIR}/p.parquet');
    CREATE TABLE td (s String, d UInt8 DEFAULT intDiv(100, length(s))) ENGINE = File(Parquet, '${DIR}/d.parquet');"

# Runs all of ${queries} without the optimization, then with it with one thread and with 4 threads, and checks every
# result against the one without the optimization.
compare() {
    local script="${TABLES} " mode query
    for mode in "${ST}, ${OFF}" "${ST}, ${ON}" "${MT}, ${ON}"; do
        script+="SET ${mode}; "
        for query in "${queries[@]}"; do
            script+="${query}; SELECT '---'; "
        done
    done
    "${LOCAL[@]}" --query "${script}" | python3 -c "
import sys
queries = sys.argv[1:]
results, lines = [], []
for line in sys.stdin.read().split('\n'):
    if line == '---':
        results.append(lines)
        lines = []
    else:
        lines.append(line)
mode = lambda i: results[i * len(queries):(i + 1) * len(queries)]
off = mode(0)
for checked in (mode(1), mode(2)):
    failed = [query for i, query in enumerate(queries)
              if not (i < len(off) and off[i] and i < len(checked) and checked[i] == off[i])]
    print('\n'.join('FAIL: ' + query for query in failed) if failed else 'OK')
" "${queries[@]}"
}

echo "-- results identical with and without the optimization"
for F in "${P}" "${D}" "${DL}" "${DB}"; do
    queries=(
        "SELECT s FROM ${F} WHERE s != '' ORDER BY s LIMIT 5"
        "SELECT s FROM ${F} WHERE s != '' ORDER BY s DESC LIMIT 5 OFFSET 3"
        "SELECT r FROM ${F} WHERE r != '' ORDER BY r DESC LIMIT 5 OFFSET 3"
        "SELECT s FROM ${F} PREWHERE s != '' ORDER BY s DESC LIMIT 5"
        "SELECT s FROM ${F} WHERE notEmpty(s) ORDER BY s LIMIT 5 OFFSET 7"
        "SELECT e FROM ${F} WHERE e != '' ORDER BY e LIMIT 5"
        "SELECT e FROM ${F} WHERE e > '' ORDER BY e DESC LIMIT 5"
        "SELECT t, k FROM ${F} WHERE t != '' ORDER BY t, k LIMIT 6"
        "SELECT t, k FROM ${F} WHERE t != '' ORDER BY t DESC, k LIMIT 6"
        "SELECT s, k FROM ${F} WHERE s != '' ORDER BY s LIMIT 5"
        "SELECT s, k FROM ${F} WHERE s != '' AND k % 3 != 0 ORDER BY s LIMIT 5"
        "SELECT e FROM file('${DIR}/p.parquet', Parquet, 'e Nullable(String), k UInt64') WHERE e != '' ORDER BY e LIMIT 5"
        "SELECT e FROM file('${DIR}/p.parquet', Parquet, 'e LowCardinality(String), k UInt64') WHERE e != '' ORDER BY e LIMIT 5"
        "SELECT e FROM file('${DIR}/d.parquet', Parquet, 'e LowCardinality(Nullable(String)), k UInt64') WHERE e != '' ORDER BY e LIMIT 5"
        "SELECT s FROM ${F} WHERE s != '' ORDER BY s LIMIT 5 SETTINGS use_top_k_dynamic_filtering_for_variable_length_types = 1"
        "SELECT x FROM (SELECT intDiv(1, length(s)) AS x, s FROM ${F}) WHERE s != '' ORDER BY s LIMIT 5 SETTINGS query_plan_filter_push_down = 0"
        "SELECT s, d FROM tp WHERE s != '' ORDER BY s LIMIT 5 SETTINGS optimize_move_to_prewhere = 0, query_plan_optimize_lazy_materialization = 0"
        "SELECT s, d FROM td WHERE s != '' ORDER BY s LIMIT 5 SETTINGS query_plan_optimize_lazy_materialization = 0"
        "SELECT s, d FROM tp PREWHERE length(s) < 1000 WHERE s != '' ORDER BY s LIMIT 5 SETTINGS query_plan_optimize_lazy_materialization = 0"
        "SELECT s FROM ${F} PREWHERE throwIf(length(s) = 0) = 0 AND s != '' ORDER BY s LIMIT 5"
    )
    compare
done
queries=("SELECT s FROM s3('${S3_P}', 'test', 'testtest', 'Parquet') WHERE s != '' ORDER BY s LIMIT 5")
compare

# One thread: the row groups are started one by one, and the threshold is set by the first ones.
echo "-- the values beyond the threshold are not copied"
script="${TABLES} SET ${ST}, ${ON}; "
checks=()
# $1: name, $2: query, $3: Python condition on the number of values it replaced
check() {
    script+="$2 FORMAT Null; SELECT sum(value) FROM system.events WHERE event = 'ParquetTopKStringValuesReplaced'; "
    checks+=("$1" "$3")
}
check "PLAIN" "SELECT s FROM ${P} WHERE s != '' ORDER BY s LIMIT 5" "replaced > 0"
check "PLAIN, PREWHERE" "SELECT s FROM ${P} PREWHERE s != '' ORDER BY s DESC LIMIT 5" "replaced > 0"
check "PLAIN, ties" "SELECT t, k FROM ${P} WHERE t != '' ORDER BY t, k LIMIT 6" "replaced > 0"
check "dictionary" "SELECT s FROM ${D} WHERE s != '' AND k % 3 != 0 ORDER BY s LIMIT 5" "replaced > 0"
check "dictionary, PREWHERE" "SELECT s FROM ${D} PREWHERE s != '' AND k % 3 != 0 ORDER BY s DESC LIMIT 5" "replaced > 0"
check "dictionary, single column" "SELECT r FROM ${D} WHERE r != '' ORDER BY r LIMIT 5" "replaced > 0"
check "DELTA_LENGTH_BYTE_ARRAY" "SELECT s FROM ${DL} WHERE s != '' ORDER BY s LIMIT 5" "replaced > 0"
check "DELTA_BYTE_ARRAY" "SELECT s FROM ${DB} WHERE s != '' ORDER BY s LIMIT 5" "replaced > 0"
check "DEFAULT, PREWHERE moved" "SELECT s, d FROM tp WHERE s != '' ORDER BY s LIMIT 5 SETTINGS query_plan_optimize_lazy_materialization = 0" "replaced > 0"
# PREWHERE does not drop the replaced values before something else reads them.
check "no PREWHERE" "SELECT s, d FROM tp WHERE s != '' ORDER BY s LIMIT 5 SETTINGS optimize_move_to_prewhere = 0, query_plan_optimize_lazy_materialization = 0" "replaced == 0"
check "length in PREWHERE" "SELECT s, d FROM tp PREWHERE length(s) < 1000 WHERE s != '' ORDER BY s LIMIT 5 SETTINGS query_plan_optimize_lazy_materialization = 0" "replaced == 0"
check "length in the same PREWHERE" "SELECT s FROM ${P} PREWHERE length(s) > 3 AND s != '' ORDER BY s LIMIT 5" "replaced == 0"
check "throwIf" "SELECT s FROM ${P} PREWHERE throwIf(length(s) = 0) = 0 AND s != '' ORDER BY s LIMIT 5" "replaced == 0"
"${LOCAL[@]}" --query "${script}" | python3 -c "
import sys
before = 0
for (name, condition), line in zip(zip(sys.argv[1::2], sys.argv[2::2]), sys.stdin):
    replaced, before = int(line) - before, int(line)
    print(name + ':', condition + ':', eval(condition))
" "${checks[@]}"

echo "-- a single-column filter is moved to PREWHERE"
"${LOCAL[@]}" --query "
    SELECT count() FROM (EXPLAIN actions = 1 SELECT s FROM ${P} WHERE s != '' ORDER BY s LIMIT 5) WHERE explain ILIKE '%Prewhere filter column%';
    SELECT count() FROM (EXPLAIN actions = 1 SELECT s FROM ${P} WHERE s != '' ORDER BY s LIMIT 5 SETTINGS use_top_k_dynamic_filtering = 0)
        WHERE explain ILIKE '%Prewhere filter column%';
    SELECT count() FROM (EXPLAIN actions = 1 SELECT s FROM ${P} WHERE s != '' ORDER BY s LIMIT 5 SETTINGS use_columns_cache = 1)
        WHERE explain ILIKE '%Prewhere filter column%';"

# The column written to the columns cache keeps its values; the second query is served from the cache. The cache trusts
# the version of a file with an mtime in the past.
echo "-- columns cache"
cp "${DIR}/p.parquet" "${DIR}/pc.parquet"
touch -d '2020-01-01 00:00:00' "${DIR}/pc.parquet"
"${LOCAL[@]}" --query "
    SET ${ST}, ${ON}, use_columns_cache = 1;
    SELECT s FROM file('${DIR}/pc.parquet') WHERE s != '' ORDER BY s LIMIT 5 FORMAT Null;
    SELECT sum(value) FROM system.events WHERE event = 'ParquetTopKStringValuesReplaced';
    SELECT count(), sum(length(s)) FROM file('${DIR}/pc.parquet') WHERE s != '';
    SELECT count() > 0 FROM system.events WHERE event = 'ColumnsCacheHits';
    SELECT count(), sum(length(s)) FROM file('${DIR}/pc.parquet') WHERE s != '' SETTINGS use_columns_cache = 0;"

# The query condition cache of a server, which trusts the version of a file with an mtime in the past: a row group whose
# values were all replaced by '' still holds rows that match the filter.
echo "-- query condition cache"
mkdir -p "${USER_FILES_PATH}/${DATA_DIR}"
cp "${DIR}/p.parquet" "${USER_FILES_PATH}/${DATA_DIR}/p.parquet"
touch -d '2020-01-01 00:00:00' "${USER_FILES_PATH}/${DATA_DIR}/p.parquet"
QCC="${ST}, use_query_condition_cache = 1, query_plan_max_limit_for_top_k_optimization = 1000,
    input_format_parquet_filter_push_down = 1, input_format_parquet_preserve_order = 0, use_columns_cache = 0,
    max_block_size = 65409, optimize_move_to_prewhere = 1, enable_parallel_replicas = 0"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_05490 (s String, k UInt64) ENGINE = File(Parquet, '${DATA_DIR}/p.parquet');
    SELECT s FROM t_05490 WHERE s != '' ORDER BY s DESC LIMIT 5 SETTINGS ${QCC}, ${ON};
    SELECT count(), sum(length(s)) FROM t_05490 WHERE s != '' SETTINGS ${QCC}, use_top_k_dynamic_filtering = 0;"
