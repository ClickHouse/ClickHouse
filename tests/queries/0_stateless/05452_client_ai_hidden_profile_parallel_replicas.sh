#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the AI agent of the client is not compiled in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A user whose settings profile turns on parallel replicas with a custom key that advances a series
# in Keeper, connecting with `apply_settings_from_server = 0` so that the values never reach the
# client. The custom key is a filter the server adds to the read of any table, `system.settings` of
# the questions the agent asks about the session before the turn included. The AI agent must not
# evaluate it in any of its queries.
user="user_${CLICKHOUSE_DATABASE}"
profile="profile_${CLICKHOUSE_DATABASE}"
series="series_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user}"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS ${profile}"
$CLICKHOUSE_CLIENT -q "
    CREATE SETTINGS PROFILE ${profile} SETTINGS
        enable_parallel_replicas = 1,
        max_parallel_replicas = 2,
        parallel_replicas_mode = 'custom_key_sampling',
        parallel_replicas_count = 2,
        parallel_replica_offset = 0,
        parallel_replicas_custom_key = 'generateSerialID(''${series}'')'"
$CLICKHOUSE_CLIENT -q "CREATE USER ${user} SETTINGS PROFILE '${profile}'"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON *.* TO ${user}"
$CLICKHOUSE_CLIENT -q "GRANT SHOW ON *.* TO ${user}"

CLICKHOUSE_AI_TEST_USER="${user}" python3 "$CUR_DIR"/05452_client_ai_hidden_profile_parallel_replicas.python

# The first value of a series that was never advanced.
$CLICKHOUSE_CLIENT -q "SELECT 'series:', generateSerialID('${series}')"

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE ${profile}"
