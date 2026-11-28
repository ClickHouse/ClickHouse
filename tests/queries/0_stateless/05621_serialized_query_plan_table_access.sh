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

# Seeing the table is not enough: the shard must check `SELECT` on the columns it reads.
front="remote('127.0.0.2', ${CLICKHOUSE_DATABASE}, front, '${USER_NAME}')"
join="${front} AS a INNER JOIN ${CLICKHOUSE_DATABASE}.hidden AS b USING (id)"

$CLICKHOUSE_CLIENT -q "GRANT SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.hidden TO ${USER_NAME}"
for serialize in 0 1
do
    settings="--serialize_query_plan ${serialize} --prefer_localhost_replica 0"

    echo "SHOW COLUMNS, column s, serialize_query_plan = ${serialize}"
    $CLICKHOUSE_CLIENT $settings -q "SELECT b.s FROM ${join}" 2>&1 | grep -o -m1 -E '^[a-z0-9]+$|ACCESS_DENIED'
done
$CLICKHOUSE_CLIENT -q "REVOKE SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.hidden FROM ${USER_NAME}"

$CLICKHOUSE_CLIENT -q "GRANT SELECT(id) ON ${CLICKHOUSE_DATABASE}.hidden TO ${USER_NAME}"
for serialize in 0 1
do
    settings="--serialize_query_plan ${serialize} --prefer_localhost_replica 0"

    echo "SELECT(id), column s, serialize_query_plan = ${serialize}"
    $CLICKHOUSE_CLIENT $settings -q "SELECT b.s FROM ${join}" 2>&1 | grep -o -m1 -E '^[a-z0-9]+$|ACCESS_DENIED'

    echo "SELECT(id), count, serialize_query_plan = ${serialize}"
    $CLICKHOUSE_CLIENT $settings -q "SELECT count() FROM ${join}" 2>&1 | grep -o -m1 -E '^[a-z0-9]+$|ACCESS_DENIED'
done

# A parameterized view is read directly as well.
$CLICKHOUSE_CLIENT -q "CREATE VIEW pv AS SELECT id, s FROM hidden WHERE id = {p:Int64}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.hidden TO ${USER_NAME}"
view_join="${front} AS a INNER JOIN ${CLICKHOUSE_DATABASE}.pv(p = 1) AS b USING (id)"
for grant in "SHOW COLUMNS" "SELECT"
do
    $CLICKHOUSE_CLIENT -q "GRANT ${grant} ON ${CLICKHOUSE_DATABASE}.pv TO ${USER_NAME}"
    for serialize in 0 1
    do
        settings="--serialize_query_plan ${serialize} --prefer_localhost_replica 0"

        echo "parameterized view, ${grant}, serialize_query_plan = ${serialize}"
        $CLICKHOUSE_CLIENT $settings -q "SELECT b.s FROM ${view_join}" 2>&1 | grep -o -m1 -E '^[a-z0-9]+$|ACCESS_DENIED'
    done
done

$CLICKHOUSE_CLIENT -q "DROP USER ${USER_NAME}"
$CLICKHOUSE_CLIENT -q "DROP VIEW pv"
$CLICKHOUSE_CLIENT -q "DROP TABLE front"
$CLICKHOUSE_CLIENT -q "DROP TABLE hidden"
