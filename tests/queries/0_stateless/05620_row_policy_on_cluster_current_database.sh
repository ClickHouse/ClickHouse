#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: its replicated access storage replaces the local_directory and memory storages

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_05620_${CLICKHOUSE_DATABASE}"
policy="p_05620_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05620 (x Int32) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER ON *.* TO ${user}"
${CLICKHOUSE_CLIENT} --query "GRANT CREATE ROW POLICY, ALTER ROW POLICY, DROP ROW POLICY ON ${CLICKHOUSE_DATABASE}.* TO ${user}"

# A bare table name refers to the current database, as it does without ON CLUSTER.
on_cluster() { ${CLICKHOUSE_CLIENT} --user "${user}" --distributed_ddl_output_mode none --query "$1"; }
show() { ${CLICKHOUSE_CLIENT} --query "SELECT database = currentDatabase(), table, select_filter, storage FROM system.row_policies WHERE short_name = '${policy}'"; }

on_cluster "CREATE ROW POLICY ${policy} ON t_05620 ON CLUSTER test_shard_localhost IN local_directory USING x = 1 TO ALL"
show
on_cluster "ALTER ROW POLICY ${policy} ON t_05620 ON CLUSTER test_shard_localhost USING x = 2"
show
on_cluster "MOVE ROW POLICY ${policy} ON t_05620 TO memory ON CLUSTER test_shard_localhost"
show
on_cluster "DROP ROW POLICY ${policy} ON t_05620 ON CLUSTER test_shard_localhost"
show

${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05620"
