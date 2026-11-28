#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression: a row-policy filter is analyzed separately from the query tree checked for plan cache
# eligibility, so a non-deterministic function hidden from the raw policy AST must still make the
# plan uncacheable:
#   - a niladic function spelled without parentheses (`currentRequestURL IN ('')`): a hit warmed
#     over the native protocol would keep the folded `true` for an HTTP request;
#   - an `ALIAS` column that expands to `now`, read only by the policy.
# The plan cache is a single, server-wide cache inspected via `SYSTEM DROP QUERY PLAN CACHE` and
# exact `QueryPlanCacheHits` counts, and the test creates a global user, so it runs in isolation
# (see 04489 for the full rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05620_${CLICKHOUSE_DATABASE}"
SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_05620;
    DROP TABLE IF EXISTS e_05620;
    CREATE TABLE t_05620 (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE e_05620 (id UInt64, ts DateTime, cutoff DateTime ALIAS now) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_05620 VALUES (1), (2), (3);
    INSERT INTO e_05620 VALUES (1, '2100-01-01 00:00:00'), (2, '2000-01-01 00:00:00');

    DROP USER IF EXISTS $user;
    CREATE USER $user IDENTIFIED WITH no_password;
    REVOKE ALL ON *.* FROM $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05620 TO $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.e_05620 TO $user;

    CREATE ROW POLICY p_05620_t ON ${CLICKHOUSE_DATABASE}.t_05620 USING currentRequestURL IN ('') TO $user;
    CREATE ROW POLICY p_05620_e ON ${CLICKHOUSE_DATABASE}.e_05620 USING ts > cutoff TO $user;
"

run_user()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT --user="$user" $SETTINGS --query "$1" 2>&1
}

# Number of plan cache hits recorded for the most recent run of a query matching $1.
hits_of_last_run()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['QueryPlanCacheHits']
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND type = 'QueryFinish'
          AND query LIKE '$1%'
        ORDER BY event_time_microseconds DESC
        LIMIT 1"
}

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

echo "-- 1. row policy with a niladic function spelled without parentheses"
Q1="SELECT /* q1 */ id FROM ${CLICKHOUSE_DATABASE}.t_05620 ORDER BY id"
echo "-- native: $(run_user "$Q1" | tr '\n' ' ')"
run_user "$Q1" > /dev/null
echo "-- hits after a native repeat (must be 0): $(hits_of_last_run 'SELECT /* q1 */')"
http_query_id="05620_http_${CLICKHOUSE_DATABASE}_${RANDOM}"
echo "-- HTTP (must be empty): [$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user}&query_id=${http_query_id}&allow_experimental_query_plan_cache=1&enable_query_plan_cache=1" -d "$Q1" | tr '\n' ' ')]"
# The `query_log` entry of an HTTP query is written after the response is sent, so wait for it.
http_hits=""
while clickhouse_test_time_left
do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    http_hits=$($CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['QueryPlanCacheHits']
        FROM system.query_log
        WHERE event_date >= yesterday() AND query_id = '${http_query_id}' AND type = 'QueryFinish'")
    [ -n "$http_hits" ] && break
    sleep 0.1
done
echo "-- hits of the HTTP run (must be 0): ${http_hits}"

echo "-- 2. row policy reading an ALIAS column that expands to now"
Q2="SELECT /* q2 */ id FROM ${CLICKHOUSE_DATABASE}.e_05620 ORDER BY id"
echo "-- result: $(run_user "$Q2" | tr '\n' ' ')"
run_user "$Q2" > /dev/null
echo "-- hits after a repeat (must be 0): $(hits_of_last_run 'SELECT /* q2 */')"

$CLICKHOUSE_CLIENT --query "
    DROP ROW POLICY p_05620_t ON ${CLICKHOUSE_DATABASE}.t_05620;
    DROP ROW POLICY p_05620_e ON ${CLICKHOUSE_DATABASE}.e_05620;
    DROP USER $user;
    DROP TABLE t_05620;
    DROP TABLE e_05620;
"
