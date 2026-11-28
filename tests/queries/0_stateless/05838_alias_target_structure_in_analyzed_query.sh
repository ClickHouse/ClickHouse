#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An `Alias` reports the structure of its target. A query that is only analyzed and never reads the target, such as the
# query of `CREATE VIEW`, would expose that structure, so it needs the same grants on every table of the chain of the
# alias as the query needs on a table: the structure visible, and `SELECT` on the columns it selects.

db_p="${CLICKHOUSE_DATABASE}_p"
# Not the database of the test: it may be a `Replicated` one, which refuses `ON CLUSTER` on its hosts.
db_a="${CLICKHOUSE_DATABASE}_a"
user="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${db_a} SYNC"
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${db_p} SYNC"
}

trap cleanup EXIT
cleanup

# The grants name exact tables: any grant on a table implies `SHOW COLUMNS` on it, so a grant on `${db_a}.*` would
# make the second hop of the chain below visible.
${CLICKHOUSE_CLIENT} <<EOF
CREATE DATABASE ${db_p} ENGINE = Atomic;
CREATE DATABASE ${db_a} ENGINE = Atomic;
CREATE TABLE ${db_p}.secret (hidden_name String, salary UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE ${db_a}.alias ENGINE = Alias('${db_p}', 'secret');
-- An alias of an existing alias is refused, so the outer one is created first.
CREATE TABLE ${db_a}.outer ENGINE = Alias('inner');
CREATE TABLE ${db_a}.inner ENGINE = Alias('${db_p}', 'secret');
CREATE USER ${user};
GRANT SELECT ON ${db_a}.alias TO ${user};
GRANT SELECT ON ${db_a}.outer TO ${user};
GRANT CREATE VIEW ON ${db_a}.v TO ${user};
GRANT CREATE VIEW ON ${db_a}.v_one TO ${user};
GRANT CREATE VIEW ON ${db_a}.v_nested TO ${user};
GRANT CREATE VIEW ON ${db_a}.v_cluster TO ${user};
GRANT CREATE VIEW ON ${db_a}.v_outer TO ${user};
GRANT CREATE TABLE, INSERT ON ${db_a}.copy TO ${user};
GRANT TABLE ENGINE ON Memory TO ${user};
GRANT CLUSTER ON *.* TO ${user};
EOF

error_as_user()
{
    ${CLICKHOUSE_CLIENT} --user "${user}" --distributed_ddl_output_mode none --query "$1" 2>&1 | grep -o -m1 "ACCESS_DENIED"
}

echo "-- without grants on the target"
error_as_user "CREATE VIEW ${db_a}.v AS SELECT * FROM ${db_a}.alias"
error_as_user "CREATE VIEW ${db_a}.v_cluster ON CLUSTER test_shard_localhost AS SELECT * FROM ${db_a}.alias"
error_as_user "CREATE TABLE ${db_a}.copy ENGINE = Memory AS SELECT * FROM ${db_a}.alias"
error_as_user "EXPLAIN QUERY TREE SELECT * FROM ${db_a}.alias"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${db_a}' AND name IN ('v', 'v_cluster', 'copy')"

echo "-- with SELECT on one column of the target"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(hidden_name) ON ${db_p}.secret TO ${user}"
error_as_user "CREATE VIEW ${db_a}.v AS SELECT * FROM ${db_a}.alias"
error_as_user "CREATE VIEW ${db_a}.v_nested AS SELECT * FROM (SELECT * FROM ${db_a}.alias)"
${CLICKHOUSE_CLIENT} --user "${user}" --query "CREATE VIEW ${db_a}.v_one AS SELECT hidden_name FROM ${db_a}.alias" && echo "OK"
${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.columns WHERE database = '${db_a}' AND table = 'v_one' ORDER BY name"

echo "-- with SHOW COLUMNS on the target"
${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON ${db_p}.secret TO ${user}"
error_as_user "CREATE VIEW ${db_a}.v AS SELECT * FROM ${db_a}.alias"

echo "-- with SELECT on the target"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${db_p}.secret TO ${user}"
${CLICKHOUSE_CLIENT} --user "${user}" --query "CREATE VIEW ${db_a}.v AS SELECT * FROM ${db_a}.alias" && echo "OK"
${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.columns WHERE database = '${db_a}' AND table = 'v' ORDER BY name"

echo "-- a chain of aliases requires every hop"
error_as_user "CREATE VIEW ${db_a}.v_outer AS SELECT * FROM ${db_a}.outer"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${db_a}.inner TO ${user}"
${CLICKHOUSE_CLIENT} --user "${user}" --query "CREATE VIEW ${db_a}.v_outer AS SELECT * FROM ${db_a}.outer" && echo "OK"
