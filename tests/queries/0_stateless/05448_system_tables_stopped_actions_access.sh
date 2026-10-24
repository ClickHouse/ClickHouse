#!/usr/bin/env bash

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

alias_database="${CLICKHOUSE_DATABASE}_actions_alias"
lazy_database="${CLICKHOUSE_DATABASE}_actions_lazy"
access_user="${CLICKHOUSE_DATABASE}_actions_user"
setup_complete=0

cleanup()
{
    if (( setup_complete )); then
        ${CLICKHOUSE_CLIENT} --multiquery --query "
            SYSTEM START MERGES ${alias_database}.a;
            SYSTEM START MERGES stopped_actions_target;
        "
    fi
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        DROP DATABASE IF EXISTS ${alias_database};
        DROP DATABASE IF EXISTS ${lazy_database} SYNC;
        DROP TABLE IF EXISTS stopped_actions_target SYNC;
        DROP USER IF EXISTS ${access_user};
    "
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE TABLE stopped_actions_target (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE DATABASE ${alias_database};
    CREATE TABLE ${alias_database}.a ENGINE = Alias('${CLICKHOUSE_DATABASE}', 'stopped_actions_target');
    CREATE USER ${access_user};
    GRANT SHOW TABLES ON ${alias_database}.* TO ${access_user};
    SYSTEM STOP MERGES ${alias_database}.a;
"
setup_complete=1

# The existing registry records the alias, not the target receiving its forwarded lock.
${CLICKHOUSE_CLIENT} --query "
    SELECT name, stopped_actions FROM system.tables
    WHERE (database = '${alias_database}' AND name = 'a')
       OR (database = '${CLICKHOUSE_DATABASE}' AND name = 'stopped_actions_target')
    ORDER BY name;
"
# Reuse the existing target-metadata gate, even with a database-wide grant on the alias.
${CLICKHOUSE_CLIENT} --user="${access_user}" --query "
    SELECT stopped_actions FROM system.tables WHERE database = '${alias_database}' AND name = 'a';
"
${CLICKHOUSE_CLIENT} --query "GRANT SHOW TABLES ON ${CLICKHOUSE_DATABASE}.stopped_actions_target TO ${access_user}"
${CLICKHOUSE_CLIENT} --user="${access_user}" --query "
    SELECT stopped_actions FROM system.tables WHERE database = '${alias_database}' AND name = 'a';
"
${CLICKHOUSE_CLIENT} --multiquery --query "
    SYSTEM START MERGES ${alias_database}.a;
    GRANT SYSTEM MERGES ON ${CLICKHOUSE_DATABASE}.stopped_actions_target TO ${access_user};
"

# Limit the global command to the fixture's target; do not change parallel tests' controls.
${CLICKHOUSE_CLIENT} --user="${access_user}" --query "SYSTEM STOP MERGES"
${CLICKHOUSE_CLIENT} --query "
    SELECT name, stopped_actions FROM system.tables
    WHERE (database = '${alias_database}' AND name = 'a')
       OR (database = '${CLICKHOUSE_DATABASE}' AND name = 'stopped_actions_target')
    ORDER BY name;
"
${CLICKHOUSE_CLIENT} --user="${access_user}" --query "SYSTEM START MERGES"
${CLICKHOUSE_CLIENT} --query "
    SELECT stopped_actions FROM system.tables
    WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'stopped_actions_target';
"

# Observing an unused stand-in must not load the real storage to discover its state.
${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE DATABASE ${lazy_database} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE ${lazy_database}.t (x UInt64) ENGINE = MergeTree ORDER BY x;
    DETACH DATABASE ${lazy_database};
    ATTACH DATABASE ${lazy_database};
    SELECT engine, stopped_actions FROM system.tables WHERE database = '${lazy_database}' AND name = 't';
    SELECT engine FROM system.tables WHERE database = '${lazy_database}' AND name = 't';
"
