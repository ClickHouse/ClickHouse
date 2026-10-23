#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: copies share a global user with a fixed name.

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS user_05398_victim"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_05398_assertions"
}

cleanup
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "CREATE USER user_05398_victim IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_05398_assertions (value UInt8) ENGINE = Null"
$CLICKHOUSE_CLIENT -q "GRANT INSERT ON t_05398_assertions TO user_05398_victim"

python3 "$CUR_DIR"/05398_user_query_log_rejects_forged_initial_user.python
