#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The hosts of `ON CLUSTER` execute a query without the user, unless `distributed_ddl_use_initial_user_and_roles` is
# enabled, and a partition value is evaluated where the command is executed. So a partition value of such a query must
# be a literal, or a tuple, an array or a `CAST` of literals, which needs no access to evaluate.

# Not the database of the test: it may be a `Replicated` one, which refuses `ON CLUSTER` on its hosts.
db="${CLICKHOUSE_DATABASE}_c"
user="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${db} SYNC"
}

trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} <<EOF
CREATE DATABASE ${db} ENGINE = Atomic;
CREATE TABLE ${db}.secret (p UInt8) ENGINE = MergeTree ORDER BY p;
INSERT INTO ${db}.secret VALUES (2);
CREATE TABLE ${db}.src (p UInt8, v String) ENGINE = MergeTree PARTITION BY p ORDER BY v;
INSERT INTO ${db}.src VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');
CREATE TABLE ${db}.dated (d Date, k UInt8) ENGINE = MergeTree PARTITION BY (d, k) ORDER BY k;
INSERT INTO ${db}.dated VALUES ('2020-01-01', 1), ('2020-01-02', 2);
CREATE USER ${user};
GRANT ALTER DELETE, ALTER UPDATE, OPTIMIZE, BACKUP, SELECT ON ${db}.src TO ${user};
GRANT ALTER DELETE, SELECT ON ${db}.dated TO ${user};
GRANT CLUSTER ON *.* TO ${user};
GRANT WRITE ON DISK TO ${user};
EOF

error_as_user()
{
    ${CLICKHOUSE_CLIENT} --user "${user}" --distributed_ddl_output_mode none --query "$1" 2>&1 | grep -o -m1 -E "ACCESS_DENIED|NOT_IMPLEMENTED"
}

partitions()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT groupArray(p) FROM (SELECT DISTINCT p FROM ${db}.src ORDER BY p)"
}

echo "-- a partition value that is not a literal"
error_as_user "ALTER TABLE ${db}.src ON CLUSTER test_shard_localhost DROP PARTITION tuple((SELECT p FROM ${db}.secret LIMIT 1))"
error_as_user "ALTER TABLE ${db}.src ON CLUSTER test_shard_localhost DROP PARTITION tuple(toUInt8(2))"
# The parameters of a function are evaluated before the function is checked.
error_as_user "ALTER TABLE ${db}.src ON CLUSTER test_shard_localhost DROP PARTITION tuple(array((SELECT p FROM ${db}.secret LIMIT 1))(1))"
error_as_user "OPTIMIZE TABLE ${db}.src ON CLUSTER test_shard_localhost PARTITION tuple((SELECT p FROM ${db}.secret LIMIT 1)) FINAL"
error_as_user "ALTER TABLE ${db}.src ON CLUSTER test_shard_localhost UPDATE v = 'z' IN PARTITION tuple((SELECT p FROM ${db}.secret LIMIT 1)) WHERE 1"
error_as_user "BACKUP TABLE ${db}.src PARTITION tuple((SELECT p FROM ${db}.secret LIMIT 1)) ON CLUSTER test_shard_localhost TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"
partitions
${CLICKHOUSE_CLIENT} --query "SELECT groupArray(v) FROM (SELECT v FROM ${db}.src ORDER BY v)"

echo "-- a literal partition value"
${CLICKHOUSE_CLIENT} --user "${user}" --query "ALTER TABLE ${db}.src ON CLUSTER test_shard_localhost DROP PARTITION 2" > /dev/null && echo "OK"
${CLICKHOUSE_CLIENT} --user "${user}" --query "ALTER TABLE ${db}.src ON CLUSTER test_shard_localhost DROP PARTITION tuple(3)" > /dev/null && echo "OK"
${CLICKHOUSE_CLIENT} --user "${user}" --query "ALTER TABLE ${db}.src ON CLUSTER test_shard_localhost DROP PARTITION ID '4'" > /dev/null && echo "OK"
${CLICKHOUSE_CLIENT} --user "${user}" --query "ALTER TABLE ${db}.src ON CLUSTER test_shard_localhost DROP PARTITION tuple(1::UInt8)" > /dev/null && echo "OK"
partitions
# A typed literal is parsed into a conversion of a literal.
${CLICKHOUSE_CLIENT} --user "${user}" --query "ALTER TABLE ${db}.dated ON CLUSTER test_shard_localhost DROP PARTITION (DATE '2020-01-01', 1)" > /dev/null && echo "OK"
${CLICKHOUSE_CLIENT} --query "SELECT groupArray(d) FROM (SELECT d FROM ${db}.dated ORDER BY d)"
