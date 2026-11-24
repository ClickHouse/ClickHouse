#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: moves tables between databases with RENAME

# The `owner_table` column of `system.tables` is shown only to users who can see the owner, and an inner table
# moved to another database has no owner there (the owner could be hidden from users who can see that database).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}"
private_db="${CLICKHOUSE_DATABASE_1}"
user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 --multiquery --query "
    DROP USER IF EXISTS ${user};
    DROP DATABASE IF EXISTS ${private_db};
    CREATE DATABASE ${private_db} ENGINE = Atomic;

    CREATE TABLE ${db}.src (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE MATERIALIZED VIEW ${db}.mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM ${db}.src;
    CREATE TABLE ${db}.metrics ENGINE = TimeSeries;

    CREATE TABLE ${private_db}.src (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE MATERIALIZED VIEW ${private_db}.private_mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM ${private_db}.src;
    CREATE TABLE ${private_db}.private_metrics ENGINE = TimeSeries;

    CREATE USER ${user} NOT IDENTIFIED;
    GRANT SELECT ON system.tables TO ${user};
    GRANT SHOW TABLES ON ${db}.* TO ${user};
    REVOKE SHOW TABLES ON ${db}.metrics FROM ${user};
"

# The inner tables of the private view and `TimeSeries` table move to the test database keeping their names and UUIDs.
for name in $(${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.tables WHERE database = '${private_db}' AND startsWith(name, '.inner_id.')"); do
    ${CLICKHOUSE_CLIENT} --query "RENAME TABLE ${private_db}.\`${name}\` TO ${db}.\`${name}\`"
done

# Replaces the UUIDs in the names of the inner tables with the names of their owners.
uuids_to_names=$(${CLICKHOUSE_CLIENT} --query "
    SELECT 's/' || toString(uuid) || '/<' || name || '>/' FROM system.tables
    WHERE (database = '${db}' AND name IN ('mv', 'metrics')) OR (database = '${private_db}' AND name IN ('private_mv', 'private_metrics'))")

list_inner_tables() {
    ${CLICKHOUSE_CLIENT} "$@" --query "
        SELECT name, owner_table FROM system.tables WHERE database = '${db}' AND startsWith(name, '.inner_id.')" \
        | sed -e "${uuids_to_names}" | LC_ALL=C sort
}

echo "--- default user: the moved inner tables have no owner ---"
list_inner_tables

echo "--- no grant on metrics: its name is hidden, and so are the names of the owners in the other database ---"
list_inner_tables --user="${user}"

${CLICKHOUSE_CLIENT} --query "GRANT SHOW TABLES ON ${db}.metrics TO ${user}"
echo "--- grant on metrics ---"
list_inner_tables --user="${user}"

${CLICKHOUSE_CLIENT} --multiquery --query "
    DROP USER ${user};
    DROP DATABASE ${private_db};
"
