#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A partition value may be an expression with a scalar subquery, which is evaluated where the command is executed. The
# replicas of a `Replicated` database execute an `ALTER` without the user, so the replica the query is submitted to must
# execute its own copy as the user, and the subquery must need the grants it needs in a local database.

db_r="${CLICKHOUSE_DATABASE}_r"
db_s="${CLICKHOUSE_DATABASE}_s"
user="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${db_r} SYNC"
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${db_s} SYNC"
}

trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none <<EOF
CREATE DATABASE ${db_s} ENGINE = Atomic;
CREATE DATABASE ${db_r} ENGINE = Replicated('/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${db_r}', 's1', 'r1');
CREATE TABLE ${db_s}.secret (p UInt8) ENGINE = MergeTree ORDER BY p;
INSERT INTO ${db_s}.secret VALUES (2);
CREATE TABLE ${db_r}.src (p UInt8, v String) ENGINE = MergeTree PARTITION BY p ORDER BY v;
CREATE TABLE ${db_r}.dst (p UInt8, v String) ENGINE = MergeTree PARTITION BY p ORDER BY v;
CREATE TABLE ${db_r}.other (p UInt8, v String) ENGINE = MergeTree PARTITION BY p ORDER BY v;
INSERT INTO ${db_r}.src VALUES (1, 'src_1'), (2, 'src_2'), (3, 'src_3');
INSERT INTO ${db_r}.other VALUES (2, 'other_2');
CREATE USER ${user};
GRANT ALTER MOVE PARTITION, ALTER DELETE, ALTER ADD COLUMN, INSERT, SELECT ON ${db_r}.src TO ${user};
GRANT INSERT, SELECT ON ${db_r}.dst TO ${user};
GRANT SELECT ON ${db_r}.other TO ${user};
EOF

error_as_user()
{
    ${CLICKHOUSE_CLIENT} --user "${user}" --query "$1" 2>&1 | grep -o -m1 "ACCESS_DENIED"
}

partitions()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT groupArray(p) FROM (SELECT DISTINCT p FROM ${db_r}.$1 ORDER BY p)"
}

echo "-- without SELECT on the table of the subquery"
error_as_user "ALTER TABLE ${db_r}.src MOVE PARTITION tuple((SELECT p FROM ${db_s}.secret LIMIT 1)) TO TABLE ${db_r}.dst"
# Two partition commands of different kinds, which a `Replicated` database replicates.
error_as_user "ALTER TABLE ${db_r}.src REPLACE PARTITION tuple((SELECT p FROM ${db_s}.secret LIMIT 1)) FROM ${db_r}.other, DROP PARTITION 3"
partitions src
partitions dst

echo "-- with SELECT on the table of the subquery"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${db_s}.secret TO ${user}"
${CLICKHOUSE_CLIENT} --user "${user}" --distributed_ddl_output_mode none --query "ALTER TABLE ${db_r}.src MOVE PARTITION tuple((SELECT p FROM ${db_s}.secret LIMIT 1)) TO TABLE ${db_r}.dst" && echo "OK"
partitions src
partitions dst

echo "-- an ALTER of the structure"
${CLICKHOUSE_CLIENT} --user "${user}" --distributed_ddl_output_mode none --query "ALTER TABLE ${db_r}.src ADD COLUMN c UInt8" && echo "OK"
${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.columns WHERE database = '${db_r}' AND table = 'src' ORDER BY position"
