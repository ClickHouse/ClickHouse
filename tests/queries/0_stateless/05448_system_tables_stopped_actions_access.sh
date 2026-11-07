#!/usr/bin/env bash
# Tags: no-replicated-database

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
        ${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES stopped_actions_target"
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

# The lock is registered on the table that owns the blocker, so it is reported there and not on the
# alias that forwarded the command.
${CLICKHOUSE_CLIENT} --query "
    SELECT name, stopped_actions FROM system.tables
    WHERE (database = '${alias_database}' AND name = 'a')
       OR (database = '${CLICKHOUSE_DATABASE}' AND name = 'stopped_actions_target')
    ORDER BY name;
"
# The restricted user only sees the alias row, which owns no background activity.
${CLICKHOUSE_CLIENT} --user="${access_user}" --query "
    SELECT name, stopped_actions FROM system.tables WHERE database = '${alias_database}';
"

${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES stopped_actions_target"
${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM MERGES ON ${CLICKHOUSE_DATABASE}.stopped_actions_target TO ${access_user}"

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

# Observing an unused stand-in must not load the real storage to discover its state, and a global
# `SYSTEM START` must not load it either.
${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE DATABASE ${lazy_database} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE ${lazy_database}.t (x UInt64) ENGINE = MergeTree ORDER BY x;
    DETACH DATABASE ${lazy_database};
    ATTACH DATABASE ${lazy_database};
    SELECT engine, stopped_actions FROM system.tables WHERE database = '${lazy_database}' AND name = 't';
    SELECT engine FROM system.tables WHERE database = '${lazy_database}' AND name = 't';
"

# Keep the global command inside the fixture: the grant below is the only one that reaches a lazy
# stand-in, so the command still walks one - which must not load it - without touching the controls
# that parallel tests registered on their own tables.
${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM MERGES ON ${lazy_database}.t TO ${access_user}"
${CLICKHOUSE_CLIENT} --user="${access_user}" --query "SYSTEM START MERGES"
${CLICKHOUSE_CLIENT} --query "
    SELECT engine FROM system.tables WHERE database = '${lazy_database}' AND name = 't';
"

# Once loaded, the stand-in reports the actions registered on the storage it wraps.
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES ${lazy_database}.t"
${CLICKHOUSE_CLIENT} --query "
    SELECT engine, stopped_actions FROM system.tables WHERE database = '${lazy_database}' AND name = 't';
"
${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES ${lazy_database}.t"
${CLICKHOUSE_CLIENT} --query "
    SELECT engine, stopped_actions FROM system.tables WHERE database = '${lazy_database}' AND name = 't';
"

# An alias that points at a lazy stand-in must still register on the storage that owns the blocker,
# even though the stand-in has not been loaded when the command arrives.
${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE TABLE ${lazy_database}.t2 (x UInt64) ENGINE = MergeTree ORDER BY x;
    DETACH DATABASE ${lazy_database};
    ATTACH DATABASE ${lazy_database};
    CREATE TABLE ${alias_database}.lazy_alias ENGINE = Alias('${lazy_database}', 't2');
"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES ${alias_database}.lazy_alias"
${CLICKHOUSE_CLIENT} --query "
    SELECT engine, stopped_actions FROM system.tables WHERE database = '${lazy_database}' AND name = 't2';
"
${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES ${alias_database}.lazy_alias"
${CLICKHOUSE_CLIENT} --query "
    SELECT stopped_actions FROM system.tables WHERE database = '${lazy_database}' AND name = 't2';
"
