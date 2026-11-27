#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A full-definition ATTACH MATERIALIZED VIEW validates the SELECT query like CREATE does.

user="user05836_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF2
DROP USER IF EXISTS $user;
CREATE USER $user;
CREATE TABLE $db.src (a String, b String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE $db.dst (a String, b String) ENGINE = MergeTree ORDER BY tuple();
GRANT SELECT, INSERT ON $db.dst TO $user;
GRANT CREATE VIEW, DROP VIEW ON $db.mv TO $user;
EOF2

attach_mv()
{
    ${CLICKHOUSE_CLIENT} --send_logs_level=none "$@" --query "
        ATTACH MATERIALIZED VIEW $db.mv UUID '$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")' TO $db.dst (a String, b String)
        AS SELECT a, b FROM $db.src" 2>&1 | grep -o -m1 -E "ACCESS_DENIED|THERE_IS_NO_COLUMN"
}

# The SELECT outputs a column which is not in the target table.
${CLICKHOUSE_CLIENT} --send_logs_level=none --query "
    ATTACH MATERIALIZED VIEW $db.mv UUID '$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")' TO $db.dst (a String, b String)
    AS SELECT a, b AS c FROM $db.src" 2>&1 | grep -o -m1 "THERE_IS_NO_COLUMN"

# Privileges on the tables in the SELECT are required, the same as for CREATE.
${CLICKHOUSE_CLIENT} --user "$user" --query "
    CREATE MATERIALIZED VIEW $db.mv TO $db.dst (a String, b String) AS SELECT a, b FROM $db.src" 2>&1 | grep -o -m1 "ACCESS_DENIED"
attach_mv --user "$user"

${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON $db.src TO $user"
attach_mv --user "$user"
${CLICKHOUSE_CLIENT} --query "INSERT INTO $db.src VALUES ('x', 'y')"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT * FROM $db.dst"

${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.mv"
${CLICKHOUSE_CLIENT} --query "DROP USER $user"
