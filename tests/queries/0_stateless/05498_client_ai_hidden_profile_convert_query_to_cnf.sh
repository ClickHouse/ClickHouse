#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the AI agent of the client is not compiled in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A user whose settings profile turns on `convert_query_to_cnf`, with which the server optimizes the
# filters of a query with the `CHECK` and `ASSUME` constraints of the tables it reads - and analyzing
# a constraint evaluates its scalar subqueries, stored SQL that the validation of the unconfirmed
# read-only query of the AI agent has not seen. The user connects with `apply_settings_from_server = 0`,
# so that the value never reaches the client. The read-only query must carry `convert_query_to_cnf = 0`.
#
# The session is driven by the script of `05452_client_ai_hidden_profile_parallel_replicas`, which
# makes one read-only tool call through a mock provider as the user from `CLICKHOUSE_AI_TEST_USER`.
user="user_${CLICKHOUSE_DATABASE}"
profile="profile_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user}"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS ${profile}"
$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE ${profile} SETTINGS convert_query_to_cnf = 1"
$CLICKHOUSE_CLIENT -q "CREATE USER ${user} SETTINGS PROFILE '${profile}'"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON *.* TO ${user}"
$CLICKHOUSE_CLIENT -q "GRANT SHOW ON *.* TO ${user}"

CLICKHOUSE_AI_TEST_USER="${user}" python3 "$CUR_DIR"/05452_client_ai_hidden_profile_parallel_replicas.python

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
# The read-only query of the agent is there, and it ran without the conversion.
$CLICKHOUSE_CLIENT -q "
    SELECT
        count() > 0 AS agent_query_logged,
        countIf(Settings['convert_query_to_cnf'] != '0') AS with_cnf
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND user = '${user}' AND type != 'QueryStart'
        AND query LIKE '%''own'' || ''result''%'"

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE ${profile}"
