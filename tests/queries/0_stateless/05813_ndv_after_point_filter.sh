#!/usr/bin/env bash

# With `query_plan_estimate_ndv_after_point_filter`, the statistics estimate of a filtered read caps the number of distinct
# values of a column at the number of values the whole filter restricts it to: 1 after `=`, k after `IN` of k values.
# - T1: `=` and `IN` on one column, `=` on two columns, and an `OR` of points on one column.
# - T2: a range, an `OR` across two columns and a negation keep the number of distinct values of the table.
# - T3: a `GROUP BY` over the filtered column is estimated from the capped number.
# - T4: all of T1 with the setting off.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT_TRACE=${CLICKHOUSE_CLIENT/"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"/"--send_logs_level=trace"}

# The trace line is printed by join reordering, whose settings are randomized.
PINS="--query_plan_optimize_join_order_randomize=0 --query_plan_optimize_join_order_limit=10
    --query_plan_optimize_join_order_algorithm=greedy --query_plan_join_swap_table=auto
    --use_hash_table_stats_for_join_reordering=0 --collect_hash_table_stats_during_joins=0
    --enable_join_runtime_filters=0 --enable_parallel_replicas=0 --enable_join_transitive_predicates=0
    --query_plan_propagate_predicate_across_join=0 --use_statistics=1 --materialize_statistics_on_insert=1
    --min_marks_per_part_to_load_statistics=0
    --explain_query_plan_default=legacy"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t (k UInt64, g UInt64 STATISTICS(uniq), s String STATISTICS(uniq)) ENGINE = MergeTree ORDER BY k
        SETTINGS auto_statistics_types = '';
    INSERT INTO t SELECT number, number % 100, toString(intDiv(number, 100) % 50) FROM numbers(10000)
        SETTINGS materialize_statistics_on_insert = 1;
    CREATE TABLE dim (k UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO dim SELECT number FROM numbers(100);
"

# Prints the numbers of distinct values of `g` and `s` in the trace line of the read of `t`. Usage: ndv <filter> [client flags].
ndv()
{
    local filter="$1"
    shift
    echo "${filter}: $(${CLICKHOUSE_CLIENT_TRACE} ${PINS} "$@" --query "
        EXPLAIN SELECT t.g, t.s FROM t JOIN dim ON t.k = dim.k WHERE ${filter}" 2>&1 >/dev/null \
        | grep -oE 'Estimated statistics for [A-Za-z]+ t: .*' | head -n 1 \
        | grep -oE '__table1\.[gs]: [0-9]+' | sed 's/__table1\.//' | sort | tr '\n' ' ' | sed 's/ $//')"
}

echo "T1"
for filter in "t.g = 7" "t.g IN (1, 2, 3)" "t.g = 7 AND t.s = '3'" "t.g = 7 OR t.g = 8"; do
    ndv "${filter}"
done

echo "T2"
for filter in "t.g < 10" "t.g = 7 OR t.s = '3'" "t.g != 7"; do
    ndv "${filter}"
done

echo "T3"
for flag in 1 0; do
    echo "setting ${flag}: $(${CLICKHOUSE_CLIENT_TRACE} ${PINS} --query_plan_estimate_ndv_after_point_filter=${flag} --query "
        EXPLAIN SELECT a.g FROM (SELECT g, s, count() AS c FROM t WHERE g = 7 GROUP BY g, s) AS a JOIN dim ON a.g = dim.k" 2>&1 >/dev/null \
        | grep -oE 'Estimated statistics for [A-Za-z]+ a: [0-9]+ rows' | head -n 1 | grep -oE '[0-9]+ rows')"
done

echo "T4"
for filter in "t.g = 7" "t.g IN (1, 2, 3)" "t.g = 7 AND t.s = '3'" "t.g = 7 OR t.g = 8"; do
    ndv "${filter}" --query_plan_estimate_ndv_after_point_filter=0
done
