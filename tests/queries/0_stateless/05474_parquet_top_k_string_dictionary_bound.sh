#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: writes and reads Parquet files

# ORDER BY <String column> LIMIT over Parquet with a filter that rejects the empty string: the minimum statistic of
# every row group is '', so the statistics cannot skip them, but the dictionary of a chunk whose data pages are all
# dictionary-encoded bounds the values that can pass the filter.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05474_parquet_topk_dict_XXXXXX")
DATA_DIR="${CLICKHOUSE_DATABASE}_05474"
trap 'rm -rf "${DIR}" "${USER_FILES_PATH:?}/${DATA_DIR}"' EXIT

# Each clickhouse-local call runs a whole group of queries: starting the process is the slow part.
LOCAL=(${CLICKHOUSE_LOCAL}
    --query_plan_max_limit_for_top_k_optimization=1000
    --input_format_parquet_filter_push_down=1
    --input_format_parquet_preserve_order=0
    --input_format_parquet_dictionary_filter_push_down=1048576
    --use_query_condition_cache=0
    --max_block_size=65409)
ST="max_threads = 1, max_parsing_threads = 1"
MT="max_threads = 4, max_parsing_threads = 4"
ON="use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 0"
OPT_IN="use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 1"
OFF="use_top_k_dynamic_filtering = 0, use_top_k_dynamic_filtering_for_variable_length_types = 0"

# 20 row groups of 1000 rows. `e` is '' in nine of ten rows, otherwise a value of a range of k that is disjoint
# between row groups (the smallest in row group 11): every row group has the minimum ''. `n` is `e` with a NULL in
# row groups 0 and 10, whose chunks then have no range to refine.
# long.parquet: `p` has the same 300-byte prefix everywhere, `q` differs within the first 256 bytes, the maximum of
# `r` is 300 0xFF bytes, `u` is the same as `r` but too long for the statistics except in row group 0, `w` is one
# 1000-byte value.
"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DIR}/t.parquet', Parquet)
    SELECT if(number % 10 = 3, leftPad(toString(k), 6, '0'), '') AS e, number AS v, if(number % 10000 = 5, NULL, e) AS n
    FROM (SELECT (number % 1000) + 1000 * ((intDiv(number, 1000) * 7 + 3) % 20) AS k, number FROM numbers(20000))
    SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION file('${DIR}/long.parquet', Parquet)
    SELECT if(number % 10 = 3, concat(repeat('x', 300), leftPad(toString(k), 6, '0')), '') AS p,
        if(number % 10 = 3, concat(leftPad(toString(k), 6, '0'), repeat('y', 300)), '') AS q,
        multiIf(number % 10 = 3, leftPad(toString(k), 6, '0'), number % 10 = 7, repeat(unhex('FF'), 300), '') AS r,
        multiIf(number % 10 = 3, leftPad(toString(k), 6, '0'), number % 10 = 7 AND number >= 1000, repeat(unhex('FF'), 5000), '') AS u,
        if(number % 10 = 3, repeat('z', 1000), '') AS w, number AS v
    FROM (SELECT (number % 1000) + 1000 * ((intDiv(number, 1000) * 7 + 3) % 20) AS k, number FROM numbers(20000))
    SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1;"

T="file('${DIR}/t.parquet')"
L="file('${DIR}/long.parquet')"

# Runs all of ${queries} without the optimization, then with it with one thread and with 4 threads, and checks every
# result against the one without the optimization.
compare() {
    local script="" mode query
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
queries=(
    "SELECT e FROM ${T} WHERE e != '' ORDER BY e LIMIT 5"
    "SELECT e FROM ${T} WHERE notEmpty(e) ORDER BY e LIMIT 5 OFFSET 3"
    "SELECT e, v FROM ${T} WHERE e != '' AND v % 2 = 1 ORDER BY e LIMIT 5"
    "SELECT e, v FROM ${T} WHERE e != '' OR v = 10007 ORDER BY e, v LIMIT 5"
    "SELECT e FROM ${T} WHERE e != '' ORDER BY e DESC LIMIT 5"
    "SELECT n, v FROM ${T} WHERE n != '' ORDER BY n LIMIT 5"
    "SELECT n, v FROM ${T} WHERE n != '' ORDER BY n NULLS FIRST LIMIT 5"
    "SELECT e FROM ${T} PREWHERE e != '' ORDER BY e LIMIT 5"
    "SELECT e FROM ${T} PREWHERE e != '' AND e LIKE '%1%' ORDER BY e LIMIT 5 SETTINGS apply_string_filters_during_scan = 1"
    "SELECT e, v FROM ${T} WHERE indexHint(e != '') ORDER BY e, v DESC LIMIT 5"
    "SELECT p, v FROM ${L} WHERE p != '' ORDER BY p LIMIT 5"
    "SELECT q, v FROM ${L} WHERE q != '' ORDER BY q LIMIT 5"
    "SELECT hex(r), v FROM ${L} WHERE r != '' ORDER BY r LIMIT 5"
    "SELECT hex(u), v FROM ${L} WHERE u != '' ORDER BY u LIMIT 5"
    "SELECT w, v FROM ${L} WHERE w != '' ORDER BY w, v LIMIT 5"
)
compare

# One thread: the row groups are started one by one, and the threshold is set by the first ones.
echo "-- the row groups that hold no value passing the filter below the threshold are skipped"
script="SET ${ST}, ${ON}; "
checks=()
# $1: name, $2: query, $3: Python condition on the number of row groups it skipped
check() {
    script+="$2 FORMAT Null; SELECT sum(value) FROM system.events WHERE event = 'ParquetTopKSkippedRowGroups'; "
    checks+=("$1" "$3")
}
check "!=" "SELECT e FROM ${T} WHERE e != '' ORDER BY e LIMIT 5" "skipped >= 10"
check "notEmpty" "SELECT e FROM ${T} WHERE notEmpty(e) ORDER BY e LIMIT 5" "skipped >= 10"
check "> ''" "SELECT e FROM ${T} WHERE e > '' ORDER BY e LIMIT 5" "skipped >= 10"
check "'' <" "SELECT e FROM ${T} WHERE '' < e ORDER BY e LIMIT 5" "skipped >= 10"
check "LIKE" "SELECT e FROM ${T} WHERE e LIKE '%1%' ORDER BY e LIMIT 5" "skipped >= 10"
check "another conjunct" "SELECT e, v FROM ${T} WHERE e != '' AND v % 2 = 1 ORDER BY e LIMIT 5" "skipped >= 10"
check "PREWHERE" "SELECT e FROM ${T} PREWHERE e != '' ORDER BY e LIMIT 5" "skipped >= 10"
check "long values" "SELECT q FROM ${L} WHERE q != '' ORDER BY q LIMIT 5" "skipped >= 10"
check "0xFF maximum" "SELECT hex(r) FROM ${L} WHERE r != '' ORDER BY r LIMIT 5" "skipped >= 10"
# The filter does not reject every row with '', or the chunks have no statistics.
check "OR" "SELECT e, v FROM ${T} WHERE e != '' OR v = 10007 ORDER BY e, v LIMIT 5" "skipped == 0"
check "indexHint" "SELECT e, v FROM ${T} WHERE indexHint(e != '') ORDER BY e, v DESC LIMIT 5" "skipped == 0"
check "no statistics" "SELECT hex(u) FROM ${L} WHERE u != '' ORDER BY u LIMIT 5" "skipped == 0"
"${LOCAL[@]}" --query "${script}" | python3 -c "
import sys
before = 0
for (name, condition), line in zip(zip(sys.argv[1::2], sys.argv[2::2]), sys.stdin):
    skipped, before = int(line) - before, int(line)
    print(name + ':', condition + ':', eval(condition))
" "${checks[@]}"

# The query condition cache of a server, which trusts the version of a file with an mtime in the past: the row groups
# skipped by a query with the filter are not skipped by the same query without it.
echo "-- query condition cache"
mkdir -p "${USER_FILES_PATH}/${DATA_DIR}"
cp "${DIR}/t.parquet" "${USER_FILES_PATH}/${DATA_DIR}/t.parquet"
touch -d '2020-01-01 00:00:00' "${USER_FILES_PATH}/${DATA_DIR}/t.parquet"
QCC="${ST}, ${OPT_IN}, use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 1,
    query_plan_max_limit_for_top_k_optimization = 1000, input_format_parquet_filter_push_down = 1,
    input_format_parquet_preserve_order = 0, input_format_parquet_dictionary_filter_push_down = 1048576,
    max_block_size = 65409, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, enable_parallel_replicas = 0"
# The second query reads the entries of the first one.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_05474 (e String, v UInt64) ENGINE = File(Parquet, '${DATA_DIR}/t.parquet');
    SELECT e, v FROM t_05474 WHERE e != '' ORDER BY e, v DESC LIMIT 5 SETTINGS ${QCC};
    SELECT e, v FROM t_05474 WHERE e != '' ORDER BY e, v DESC LIMIT 5 SETTINGS ${QCC};
    SELECT e, v FROM t_05474 ORDER BY e, v DESC LIMIT 5 SETTINGS ${QCC};"
