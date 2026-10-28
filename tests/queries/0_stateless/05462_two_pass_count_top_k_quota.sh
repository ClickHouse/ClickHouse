#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The passes of `query_plan_two_pass_count_top_k` read more rows than the original plan, so a quota on the rows or bytes
# read disables the optimization; a quota on anything else does not.

# CREATE/DROP QUOTA can log a benign "Re-chose quotas" warning under sanitizers.
CLICKHOUSE_CLIENT+=" --allow_repeated_settings --send_logs_level=fatal"

USER="u_05462_${CLICKHOUSE_DATABASE}"
QUOTA="q_05462_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_two_pass_quota;
    CREATE TABLE t_two_pass_quota (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_two_pass_quota SELECT number % 100, toString(number % 7) FROM numbers(10000);
    DROP USER IF EXISTS ${USER};
    CREATE USER ${USER};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_two_pass_quota TO ${USER};
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${USER};
"

function applied()
{
    # The test profile sets read, sort and speed limits, which disable the optimization as well.
    $CLICKHOUSE_CLIENT --user "${USER}" --query "
        SELECT countIf(explain LIKE '%TwoPassCountTopK%') FROM (
            EXPLAIN SELECT k, s, count() FROM ${CLICKHOUSE_DATABASE}.t_two_pass_quota GROUP BY k, s ORDER BY count() DESC LIMIT 10)
        SETTINGS query_plan_two_pass_count_top_k = 1, query_plan_two_pass_count_top_k_min_rows = 0, enable_parallel_replicas = 0,
            optimize_aggregation_in_order = 0, serialize_query_plan = 0,
            max_rows_to_read = 0, max_bytes_to_read = 0, max_bytes_to_read_leaf = 0, max_rows_to_group_by = 0, max_rows_to_sort = 0,
            max_bytes_to_sort = 0, max_execution_speed = 0, max_execution_speed_bytes = 0, max_estimated_execution_time = 0"
}

for limit in "MAX queries = 1000000" "MAX read_rows = 1000000000" "MAX read_bytes = 1000000000000" "MAX execution_time = 1000"; do
    $CLICKHOUSE_CLIENT --query "DROP QUOTA IF EXISTS ${QUOTA}; CREATE QUOTA ${QUOTA} FOR INTERVAL 1 YEAR ${limit} TO ${USER}"
    echo "${limit}: $(applied)"
done
$CLICKHOUSE_CLIENT --query "DROP QUOTA ${QUOTA}"
echo "no quota: $(applied)"

$CLICKHOUSE_CLIENT --query "DROP USER ${USER}; DROP TABLE t_two_pass_quota"
