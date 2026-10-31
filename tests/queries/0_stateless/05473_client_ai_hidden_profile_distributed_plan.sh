#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the AI agent of the client is not compiled in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A user whose settings profile turns on the distributed query plan (`make_distributed_plan`), which
# dispatches the tasks of a query to the stateless workers of the cluster, connecting with
# `apply_settings_from_server = 0` so that the value never reaches the client. Neither the
# unconfirmed read-only query of the AI agent nor the internal queries it asks about the session
# may run with it: every one of them must carry `make_distributed_plan = 0`.
#
# The session is driven by the script of `05452_client_ai_hidden_profile_parallel_replicas`, which
# makes one read-only tool call through a mock provider as the user from `CLICKHOUSE_AI_TEST_USER`.
user="user_${CLICKHOUSE_DATABASE}"
profile="profile_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user}"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS ${profile}"
$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE ${profile} SETTINGS make_distributed_plan = 1"
$CLICKHOUSE_CLIENT -q "CREATE USER ${user} SETTINGS PROFILE '${profile}'"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON *.* TO ${user}"
$CLICKHOUSE_CLIENT -q "GRANT SHOW ON *.* TO ${user}"

CLICKHOUSE_AI_TEST_USER="${user}" python3 "$CUR_DIR"/05452_client_ai_hidden_profile_parallel_replicas.python

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
# The read-only query of the agent is there, and no query of the user ran with the distributed plan.
$CLICKHOUSE_CLIENT -q "
    SELECT
        countIf(query LIKE '%''own'' || ''result''%') > 0 AS agent_query_logged,
        countIf(Settings['make_distributed_plan'] != '0') AS with_distributed_plan
    FROM system.query_log
    WHERE event_date >= yesterday() AND user = '${user}' AND type != 'QueryStart'"

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE ${profile}"
