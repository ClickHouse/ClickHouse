#!/usr/bin/env bash
# Tags: shard

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_NAME="u_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER_NAME}"
$CLICKHOUSE_CLIENT -q "CREATE TABLE front (id Int64) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT -q "CREATE TABLE hidden (id Int64, s String) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT -q "INSERT INTO front VALUES (1), (2)"
$CLICKHOUSE_CLIENT -q "INSERT INTO hidden VALUES (1, 'a')"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER_NAME} IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.front TO ${USER_NAME}"

# A shard that executes a serialized query plan must check access to the tables named in the plan.
for serialize in 0 1
do
    settings="--serialize_query_plan ${serialize} --prefer_localhost_replica 0"

    echo "front only, serialize_query_plan = ${serialize}"
    $CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM remote('127.0.0.2', ${CLICKHOUSE_DATABASE}, front, '${USER_NAME}')" 2>&1 | grep -o -m1 -E '^[0-9]+$|ACCESS_DENIED'

    echo "JOIN hidden, serialize_query_plan = ${serialize}"
    $CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM remote('127.0.0.2', ${CLICKHOUSE_DATABASE}, front, '${USER_NAME}') AS a INNER JOIN ${CLICKHOUSE_DATABASE}.hidden AS b USING (id)" 2>&1 | grep -o -m1 -E '^[0-9]+$|ACCESS_DENIED'
done

$CLICKHOUSE_CLIENT -q "DROP USER ${USER_NAME}"
$CLICKHOUSE_CLIENT -q "DROP TABLE front"
$CLICKHOUSE_CLIENT -q "DROP TABLE hidden"
