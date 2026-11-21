#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the AI agent of the client is not compiled in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A user whose settings profile sets `select` to an expression that advances a series in Keeper,
# connecting with `apply_settings_from_server = 0` so that the value never reaches the client. The
# AI agent must neither evaluate it in its internal queries nor in its unconfirmed read-only query.
user="user_${CLICKHOUSE_DATABASE}"
profile="profile_${CLICKHOUSE_DATABASE}"
series="series_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user}"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS ${profile}"
$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE ${profile} SETTINGS select = 'generateSerialID(''${series}'')'"
$CLICKHOUSE_CLIENT -q "CREATE USER ${user} SETTINGS PROFILE '${profile}'"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON *.* TO ${user}"
$CLICKHOUSE_CLIENT -q "GRANT SHOW ON *.* TO ${user}"

CLICKHOUSE_AI_TEST_USER="${user}" python3 "$CUR_DIR"/05408_client_ai_hidden_profile_construction_settings.python

# The first value of a series that was never advanced.
$CLICKHOUSE_CLIENT -q "SELECT 'series:', generateSerialID('${series}')"

$CLICKHOUSE_CLIENT -q "DROP USER ${user}"
$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE ${profile}"
