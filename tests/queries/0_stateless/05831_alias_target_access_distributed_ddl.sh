#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An operation through an `Alias` checks the access to its target when it is executed. The hosts of `ON CLUSTER` and
# the replicas of a `Replicated` database execute a query without the user, so the initiator must check it instead.

db_p="${CLICKHOUSE_DATABASE}_p"
# Not the database of the test: it may be a `Replicated` one, which refuses `ON CLUSTER` on its hosts.
db_a="${CLICKHOUSE_DATABASE}_a"
db_r="${CLICKHOUSE_DATABASE}_r"
user="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${db_r} SYNC"
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${db_a} SYNC"
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${db_p} SYNC"
}

trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none <<EOF
CREATE DATABASE ${db_p} ENGINE = Atomic;
CREATE DATABASE ${db_a} ENGINE = Atomic;
CREATE DATABASE ${db_r} ENGINE = Replicated('/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${db_r}', 's1', 'r1');
CREATE TABLE ${db_p}.secret (x UInt8, v UInt8) ENGINE = MergeTree ORDER BY x
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO ${db_p}.secret VALUES (1, 0), (2, 0);
CREATE TABLE ${db_a}.alias ENGINE = Alias('${db_p}', 'secret');
CREATE TABLE ${db_r}.alias ENGINE = Alias('${db_p}', 'secret');
CREATE USER ${user};
GRANT CLUSTER ON *.* TO ${user};
GRANT TRUNCATE, ALTER, OPTIMIZE, SELECT ON ${db_a}.alias TO ${user};
GRANT CREATE TABLE, INSERT ON ${db_a}.copy TO ${user};
GRANT TABLE ENGINE ON MergeTree TO ${user};
GRANT TRUNCATE, ALTER ON ${db_r}.alias TO ${user};
EOF

run_as_user()
{
    ${CLICKHOUSE_CLIENT} --user "${user}" --distributed_ddl_output_mode none --query "$1" 2>&1 | grep -o -m1 "ACCESS_DENIED"
}

# Prints the name of the error code only, once: an error of a host is reported by the initiator with the code twice.
error_as_user()
{
    ${CLICKHOUSE_CLIENT} --user "${user}" --distributed_ddl_output_mode none --query "$1" 2>&1 | grep -o -m1 -E "ACCESS_DENIED|NOT_IMPLEMENTED" | head -n1
}

echo "-- without grants on the target"
run_as_user "TRUNCATE TABLE ${db_a}.alias ON CLUSTER test_shard_localhost"
run_as_user "TRUNCATE TABLE IF EMPTY ${db_a}.alias ON CLUSTER test_shard_localhost"
run_as_user "ALTER TABLE ${db_a}.alias ON CLUSTER test_shard_localhost DELETE WHERE 1"
run_as_user "DELETE FROM ${db_a}.alias ON CLUSTER test_shard_localhost WHERE x = 1"
run_as_user "UPDATE ${db_a}.alias ON CLUSTER test_shard_localhost SET v = 1 WHERE 1"
run_as_user "OPTIMIZE TABLE ${db_a}.alias ON CLUSTER test_shard_localhost FINAL"
run_as_user "TRUNCATE TABLE ${db_r}.alias"
run_as_user "ALTER TABLE ${db_r}.alias ADD COLUMN y UInt8"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(v) FROM ${db_p}.secret"
${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.columns WHERE database = '${db_p}' AND table = 'secret' ORDER BY name"

echo "-- the hosts do not read through an alias without the initiating user"
# The host the query is submitted to cannot authorize the alias for the other hosts, which may have another table of this name.
error_as_user "CREATE TABLE ${db_a}.copy ON CLUSTER test_shard_localhost ENGINE = MergeTree ORDER BY tuple() AS SELECT * FROM ${db_a}.alias"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.parts WHERE database = '${db_a}' AND table = 'copy' AND active"
# A trivial `count()` answers from the target without reading it. As the administrator, who passes the checks on the
# host the query is submitted to: the other hosts refuse it anyway.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none --query "CREATE TABLE ${db_a}.cnt ON CLUSTER test_shard_localhost ENGINE = MergeTree ORDER BY tuple() AS SELECT count() AS c FROM ${db_a}.alias SETTINGS optimize_trivial_count_query = 1" 2>&1 | grep -o -m1 -E "ACCESS_DENIED|NOT_IMPLEMENTED" | head -n1
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.parts WHERE database = '${db_a}' AND table = 'cnt' AND active"

echo "-- with grants on the target"
${CLICKHOUSE_CLIENT} --query "GRANT TRUNCATE ON ${db_p}.secret TO ${user}"
# `ON CLUSTER` reaches the target of an alias only if the hosts execute it as the initiating user, which is not the case here.
error_as_user "TRUNCATE TABLE ${db_a}.alias ON CLUSTER test_shard_localhost"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${db_p}.secret"
${CLICKHOUSE_CLIENT} --user "${user}" --distributed_ddl_output_mode none --log_queries 1 --query "TRUNCATE TABLE ${db_r}.alias" && echo "OK"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${db_p}.secret"
# An `ALTER` through an alias requires all of `ALTER` on the target, so as the administrator.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none --log_queries 1 --query "ALTER TABLE ${db_r}.alias COMMENT COLUMN v 'checked'" && echo "OK"
# The replica the query is submitted to executes its own copy as the submitting user. So the copy authorizes the target
# again, after the entries queued before it, which may have changed the alias.
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT query_kind, countIf(arrayExists(p -> endsWith(p, ' ON ${db_p}.secret'), used_privileges))
    FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query = 0 AND query_kind IN ('Alter', 'Drop')
        AND current_database = concat(currentDatabase(), '_r') AND has(tables, '${db_r}.alias')
    GROUP BY query_kind
    ORDER BY query_kind"

echo "-- a chain of aliases in a Replicated database"
# An alias cannot be created over another alias, but its target can be replaced by one afterwards. The replicas would look up
# the second hop in their own catalogs, which the initiator has not checked, so it is refused even for the administrator.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none --query "
    INSERT INTO ${db_p}.secret VALUES (3, 0);
    CREATE TABLE ${db_p}.hop (x UInt8, v UInt8) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE ${db_r}.chain ENGINE = Alias('${db_p}', 'hop');
    DROP TABLE ${db_p}.hop SYNC;
    CREATE TABLE ${db_p}.hop ENGINE = Alias('${db_p}', 'secret');
"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none --query "TRUNCATE TABLE ${db_r}.chain" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${db_p}.secret"

echo "-- a chain of aliases within the Replicated database"
# Its definitions are replicated, so every replica sees the same chain. The outer alias is created first, because an alias
# of an existing alias is refused.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none --query "
    INSERT INTO ${db_p}.secret VALUES (4, 0);
    CREATE TABLE ${db_r}.outer ENGINE = Alias('inner');
    CREATE TABLE ${db_r}.inner ENGINE = Alias('${db_p}', 'secret');
"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none --query "TRUNCATE TABLE ${db_r}.outer" && echo "OK"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${db_p}.secret"
