#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `Replicated` database replays an `ALTER` on every replica with its own name as the current database. The other table
# of a partition command - `MOVE PARTITION ... TO TABLE dst`, `REPLACE PARTITION ... FROM src` - must be in the same
# database: a table of another database is a different table on each replica, and a name without a database would be
# resolved with the replicated database on the replicas, while the access is checked with the current database of the query.

db_r="${CLICKHOUSE_DATABASE}_r"
db_d="${CLICKHOUSE_DATABASE}_d"
user_move="u_move_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_replace="u_replace_${CLICKHOUSE_TEST_UNIQUE_NAME}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user_move}, ${user_replace}"
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${db_r} SYNC"
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${db_d} SYNC"
}

trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none <<EOF
CREATE DATABASE ${db_d} ENGINE = Atomic;
CREATE DATABASE ${db_r} ENGINE = Replicated('/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${db_r}', 's1', 'r1');
CREATE TABLE ${db_r}.src (p UInt8, v String) ENGINE = MergeTree PARTITION BY p ORDER BY v;
CREATE TABLE ${db_r}.dst (p UInt8, v String) ENGINE = MergeTree PARTITION BY p ORDER BY v;
CREATE TABLE ${db_r}.secret (p UInt8, v String) ENGINE = MergeTree PARTITION BY p ORDER BY v;
CREATE TABLE ${db_d}.dst (p UInt8, v String) ENGINE = MergeTree PARTITION BY p ORDER BY v;
CREATE TABLE ${db_d}.secret (p UInt8, v String) ENGINE = MergeTree PARTITION BY p ORDER BY v;
INSERT INTO ${db_r}.src VALUES (1, 'src_1'), (2, 'src_2'), (3, 'src_3');
INSERT INTO ${db_r}.secret VALUES (5, 'secret');
INSERT INTO ${db_d}.secret VALUES (5, 'not secret');
CREATE USER ${user_move};
GRANT ALTER MOVE PARTITION ON ${db_r}.src TO ${user_move};
GRANT INSERT ON ${db_d}.dst TO ${user_move};
CREATE USER ${user_replace};
GRANT ALTER DELETE, INSERT ON ${db_r}.dst TO ${user_replace};
GRANT SELECT ON ${db_d}.secret TO ${user_replace};
EOF

show()
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT * FROM
        (
            SELECT 'r.src' AS t, groupArray(v) FROM (SELECT v FROM ${db_r}.src ORDER BY v)
            UNION ALL SELECT 'r.dst', groupArray(v) FROM (SELECT v FROM ${db_r}.dst ORDER BY v)
            UNION ALL SELECT 'd.dst', groupArray(v) FROM (SELECT v FROM ${db_d}.dst ORDER BY v)
        )
        ORDER BY t"
}

echo "-- MOVE PARTITION to a table without INSERT on it"
${CLICKHOUSE_CLIENT} --user "${user_move}" --query "ALTER TABLE ${db_r}.src MOVE PARTITION 1 TO TABLE ${db_r}.dst" 2>&1 | grep -o -m1 "ACCESS_DENIED"

echo "-- MOVE PARTITION to a table of another database, with a bare and a qualified name"
${CLICKHOUSE_CLIENT} --user "${user_move}" --query "USE ${db_d}; ALTER TABLE ${db_r}.src MOVE PARTITION 1 TO TABLE dst" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${db_r}.src MOVE PARTITION 1 TO TABLE ${db_d}.dst" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"
show

echo "-- the same database as the altered table, with a bare and a qualified name"
${CLICKHOUSE_CLIENT} --query "USE ${db_r}; ALTER TABLE src MOVE PARTITION 2 TO TABLE dst" > /dev/null && echo "OK"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${db_r}.src MOVE PARTITION 3 TO TABLE ${db_r}.dst" > /dev/null && echo "OK"
show

# `REPLACE PARTITION` together with another command, so that the `Replicated` database replicates the query.
echo "-- REPLACE PARTITION from a table of another database"
${CLICKHOUSE_CLIENT} --user "${user_replace}" --query "USE ${db_d}; ALTER TABLE ${db_r}.dst REPLACE PARTITION 5 FROM secret, DROP PARTITION 9" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"
${CLICKHOUSE_CLIENT} --query "SELECT v FROM ${db_r}.dst WHERE p = 5"

echo "-- REPLACE PARTITION from a temporary table"
${CLICKHOUSE_CLIENT} --query "
    CREATE TEMPORARY TABLE secret (p UInt8, v String) ENGINE = MergeTree PARTITION BY p ORDER BY v;
    INSERT INTO secret VALUES (5, 'temporary');
    ALTER TABLE ${db_r}.dst REPLACE PARTITION 5 FROM secret, DROP PARTITION 9;
" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"
${CLICKHOUSE_CLIENT} --query "SELECT v FROM ${db_r}.dst WHERE p = 5"

echo "-- REPLACE PARTITION from the same database"
${CLICKHOUSE_CLIENT} --query "USE ${db_r}; ALTER TABLE dst REPLACE PARTITION 5 FROM secret, DROP PARTITION 9" > /dev/null && echo "OK"
${CLICKHOUSE_CLIENT} --query "SELECT v FROM ${db_r}.dst WHERE p = 5"
