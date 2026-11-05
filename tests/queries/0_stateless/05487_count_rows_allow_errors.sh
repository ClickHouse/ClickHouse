#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `count()` with `optimize_count_from_files` must honour `input_format_allow_errors_num`/`_ratio` and give the
# same result as a full read (`optimize_count_from_files = 0`). Issue #70925.

unique_name=${CLICKHOUSE_TEST_UNIQUE_NAME}
tmp_dir=${USER_FILES_PATH}/${unique_name}
mkdir -p "${tmp_dir}"
rm -rf "${tmp_dir:?}"/*

html='<html>\n<body>oops</body>\n</html>\n'
mixed='{"x":1}\n{"x":"zz"}\nnot json\n{"x":2}\n'
tsv='x\n1\nzz\n2\n'

printf '%b' "${html}" > "${tmp_dir}/html.json"
printf '%b' "${mixed}" > "${tmp_dir}/mixed.json"
printf '%b' "${tsv}" > "${tmp_dir}/data.tsv"
# A separate file for every sequence that uses the count cache, so that one sequence does not fill the cache for another.
printf '%b' "${tsv}" > "${tmp_dir}/cache_count.tsv"
printf '%b' "${html}" > "${tmp_dir}/cache_count.json"
printf '%b' "${tsv}" > "${tmp_dir}/cache_read.tsv"
printf '%b' "${html}" > "${tmp_dir}/cache_read.json"
# A cached number of rows is ignored if the file was modified in the same second it was cached.
touch -t 202001010000 "${tmp_dir}"/*

data()
{
    echo "file('${unique_name}/$1', '$2', 'x UInt8')"
}

# The fast path itself, without the count cache.
# `optimize_trivial_count_query` is randomized in tests, and without it `count()` reads the rows instead of counting them.
SETTINGS="use_cache_for_count_from_files = 0, optimize_trivial_count_query = 1"

count_with_errors()
{
    local name=$1 file=$2 format=$3 errors=$4
    for optimize in 1 0
    do
        ${CLICKHOUSE_CLIENT} -q "SELECT '${name}', ${optimize}, count() FROM $(data "${file}" "${format}") SETTINGS ${errors}, optimize_count_from_files = ${optimize}, ${SETTINGS}"
    done
}

count_with_errors 'json html ratio' html.json JSONEachRow 'input_format_allow_errors_ratio = 1'
count_with_errors 'json html num' html.json JSONEachRow 'input_format_allow_errors_num = 3'
count_with_errors 'json mixed num' mixed.json JSONEachRow 'input_format_allow_errors_num = 2'
count_with_errors 'json mixed ratio' mixed.json JSONEachRow 'input_format_allow_errors_ratio = 0.7'
count_with_errors 'tsv num' data.tsv TSVWithNames 'input_format_allow_errors_num = 1'
count_with_errors 'tsv ratio' data.tsv TSVWithNames 'input_format_allow_errors_ratio = 0.6'

# Without allowed errors the fast path is still used: it does not validate rows, and fails on unparsable framing.
${CLICKHOUSE_CLIENT} -q "SELECT 'tsv no errors allowed', count() FROM $(data data.tsv TSVWithNames) SETTINGS optimize_count_from_files = 1, ${SETTINGS}"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM $(data html.json JSONEachRow) SETTINGS optimize_count_from_files = 1, ${SETTINGS}" 2>&1 | grep -o -m1 'INCORRECT_DATA'

# The count cache stores the number of rows in a file regardless of the structure and of the allowed errors.
# It must not answer a count with allowed errors, and a read with allowed errors must not fill it.
# Each sequence runs in one client call, so that it shares the cache with `clickhouse-local` as well.
SETTINGS="use_cache_for_count_from_files = 1, optimize_count_from_files = 1, optimize_trivial_count_query = 1"

# The cache is filled by a count without allowed errors.
${CLICKHOUSE_CLIENT} -q "
    SELECT 'cache count tsv plain', count() FROM $(data cache_count.tsv TSVWithNames) SETTINGS ${SETTINGS};
    SELECT 'cache count tsv num', count() FROM $(data cache_count.tsv TSVWithNames) SETTINGS input_format_allow_errors_num = 1, ${SETTINGS};
    SELECT 'cache count tsv ratio', count() FROM $(data cache_count.tsv TSVWithNames) SETTINGS input_format_allow_errors_ratio = 0.6, ${SETTINGS};
    SELECT 'cache count tsv plain', count() FROM $(data cache_count.tsv TSVWithNames) SETTINGS ${SETTINGS};
"

# A count with allowed errors.
${CLICKHOUSE_CLIENT} --send_logs_level=fatal -q "
    SELECT 'cache count json ratio', count() FROM $(data cache_count.json JSONEachRow) SETTINGS input_format_allow_errors_ratio = 1, ${SETTINGS};
    SELECT 'cache count json plain', count() FROM $(data cache_count.json JSONEachRow) SETTINGS ${SETTINGS};
" 2>&1 | grep -o -E "^cache .*|INCORRECT_DATA"

# A full read with allowed errors.
${CLICKHOUSE_CLIENT} -q "
    SELECT 'cache read tsv num', sum(x) FROM $(data cache_read.tsv TSVWithNames) SETTINGS input_format_allow_errors_num = 1, ${SETTINGS};
    SELECT 'cache read tsv plain', count() FROM $(data cache_read.tsv TSVWithNames) SETTINGS ${SETTINGS};
"
${CLICKHOUSE_CLIENT} --send_logs_level=fatal -q "
    SELECT 'cache read json ratio', count() FROM $(data cache_read.json JSONEachRow) SETTINGS input_format_allow_errors_ratio = 1, optimize_count_from_files = 0, use_cache_for_count_from_files = 1, optimize_trivial_count_query = 1;
    SELECT 'cache read json plain', count() FROM $(data cache_read.json JSONEachRow) SETTINGS ${SETTINGS};
" 2>&1 | grep -o -E "^cache .*|INCORRECT_DATA"

rm -rf "${tmp_dir:?}"
