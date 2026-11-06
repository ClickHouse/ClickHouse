#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} --multiquery --query "
    DROP USER IF EXISTS ${user};
    CREATE TABLE ${db}.dst (id UInt32) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${db}.dst SELECT number FROM numbers(1000);
    CREATE TABLE ${db}.buf (id UInt32) ENGINE = Buffer('${db}', 'dst', 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
    INSERT INTO ${db}.buf SELECT number FROM numbers(7);
    CREATE TABLE ${db}.buf_empty (id UInt32) ENGINE = Buffer('${db}', 'dst', 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
    CREATE USER ${user} NOT IDENTIFIED;
    GRANT SELECT ON ${db}.buf TO ${user};
    GRANT DROP TABLE ON ${db}.buf TO ${user};
    GRANT DROP TABLE ON ${db}.buf_empty TO ${user};
"

query="SELECT total_rows FROM system.tables WHERE database = '${db}' AND name = 'buf'"

echo "-- default user"
${CLICKHOUSE_CLIENT} --query "${query}"
echo "-- no privilege on the destination"
${CLICKHOUSE_CLIENT} --user "${user}" --query "${query}"
echo "-- DROP TABLE IF EMPTY, buffer holds rows"
${CLICKHOUSE_CLIENT} --user "${user}" --query "DROP TABLE IF EMPTY ${db}.buf SETTINGS ignore_drop_queries_probability = 0" 2>&1 | grep -o -m1 'TABLE_NOT_EMPTY'
echo "-- DROP TABLE IF EMPTY, empty buffer"
${CLICKHOUSE_CLIENT} --user "${user}" --query "DROP TABLE IF EMPTY ${db}.buf_empty SETTINGS ignore_drop_queries_probability = 0" 2>&1 | grep -o -m1 'TABLE_NOT_EMPTY'
${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.tables WHERE database = '${db}' AND name LIKE 'buf%' ORDER BY name"
${CLICKHOUSE_CLIENT} --query "GRANT SHOW TABLES ON ${db}.dst TO ${user}"
echo "-- SHOW TABLES on the destination"
${CLICKHOUSE_CLIENT} --user "${user}" --query "${query}"

${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
