#!/usr/bin/env bash
# Tags: no-parallel, no-parallel-replicas
# no-parallel: a concurrent drop or rename of a MergeTree table without a UUID clears the whole query condition cache
# no-parallel-replicas: reads with parallel replicas neither record nor use match counts

# The exact counts of matching rows per granule that the query condition cache keeps for a count()
# are keyed by the PREWHERE condition only. A row policy filters rows ahead of PREWHERE, so a read
# under a row policy must neither use counts recorded without it nor record counts of its own.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# User and policy names are server-global, suffix them with the (unique) test database to avoid clashes.
user_hide="u_qcc_count_hide_${CLICKHOUSE_DATABASE}"
user_show="u_qcc_count_show_${CLICKHOUSE_DATABASE}"
policy_hide="p_qcc_count_hide_${CLICKHOUSE_DATABASE}"
policy_show="p_qcc_count_show_${CLICKHOUSE_DATABASE}"

query="SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_qcc_count_policy WHERE s LIKE '%match%'
    SETTINGS use_query_condition_cache = 1, optimize_move_to_prewhere = 1, optimize_use_projections = 1, optimize_use_implicit_projections = 1, enable_parallel_replicas = 0, apply_string_filters_during_scan = 1"

run() # query id suffix, then client options
{
    local id="${CLICKHOUSE_DATABASE}_qcc_count_$1"
    shift
    ${CLICKHOUSE_CLIENT} "$@" --query_id "${id}" --query "${query}"
}

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user_hide}, ${user_show}"

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE IF EXISTS t_qcc_count_policy;
CREATE TABLE t_qcc_count_policy (k UInt64, s String)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 1000, index_granularity_bytes = '10Mi', add_minmax_index_for_numeric_columns = 0;
SYSTEM STOP MERGES t_qcc_count_policy;
INSERT INTO t_qcc_count_policy SELECT number, if(number % 3 = 0, 'match', 'x') FROM numbers(100000);
"

echo "-- without a row policy (the second count is answered from the cache)"
run 1
run 2

${CLICKHOUSE_CLIENT} --multiquery --query "
CREATE USER ${user_hide};
CREATE USER ${user_show};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_qcc_count_policy TO ${user_hide}, ${user_show};
CREATE ROW POLICY ${policy_hide} ON ${CLICKHOUSE_DATABASE}.t_qcc_count_policy USING k < 50000 TO ${user_hide};
CREATE ROW POLICY ${policy_show} ON ${CLICKHOUSE_DATABASE}.t_qcc_count_policy USING 1 TO ${user_show};
"

echo "-- a row policy that hides half of the rows"
run 3 --user "${user_hide}"
run 4 --user "${user_hide}"
echo "-- a row policy that hides nothing"
run 5 --user "${user_show}"
run 6 --user "${user_show}"

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP ROW POLICY ${policy_hide} ON ${CLICKHOUSE_DATABASE}.t_qcc_count_policy;
DROP ROW POLICY ${policy_show} ON ${CLICKHOUSE_DATABASE}.t_qcc_count_policy;
"

echo "-- without a row policy again: the counts recorded before are still right"
run 7
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_qcc_count_policy WHERE s LIKE '%match%' SETTINGS use_query_condition_cache = 0"

echo "-- counted from the cache"
${CLICKHOUSE_CLIENT} --multiquery --query "
SYSTEM FLUSH LOGS query_log;
SELECT replaceOne(query_id, '${CLICKHOUSE_DATABASE}_', ''),
    multiIf(NOT has(projections, tables[1] || '._exact_count_projection'), 'read', read_rows = 1, 'counted', 'partly counted')
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 1200 AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND query_id LIKE '${CLICKHOUSE_DATABASE}\\_qcc\\_count\\_%'
ORDER BY query_id;
"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user_hide}, ${user_show}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_qcc_count_policy"
