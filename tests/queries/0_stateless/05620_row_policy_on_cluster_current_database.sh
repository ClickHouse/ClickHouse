#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: its replicated access storage replaces the local_directory and memory storages

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_05620_${CLICKHOUSE_DATABASE}"
user2="u2_05620_${CLICKHOUSE_DATABASE}"
policy="p_05620_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}, ${user2}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05620 (x Int32) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user}, ${user2} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER ON *.* TO ${user}, ${user2}"
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

# On a cluster with <default_database>, the privileges are also checked on the current database.
on_cluster "CREATE ROW POLICY ${policy} ON t_05620 ON CLUSTER test_cluster_two_shards_different_databases USING x = 3 TO ALL"
show
on_cluster "DROP ROW POLICY ${policy} ON t_05620 ON CLUSTER test_shard_localhost"
show

# The privileges are checked before the query is queued: on the current database, and globally for ON *.
denied()
{
    if ${CLICKHOUSE_CLIENT} --user "$1" --query "$2" 2>&1 | grep -qF "necessary to have the grant $3"; then echo denied; else echo "not denied"; fi
}
denied "${user2}" "CREATE ROW POLICY ${policy} ON t_05620 ON CLUSTER test_cluster_two_shards_different_databases USING 1 TO ALL" "CREATE ROW POLICY ON ${CLICKHOUSE_DATABASE}.t_05620"
denied "${user}" "CREATE ROW POLICY ${policy} ON * ON CLUSTER test_shard_localhost USING 1 TO ALL" "CREATE ROW POLICY ON *.*"
show

${CLICKHOUSE_CLIENT} --query "DROP USER ${user}, ${user2}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05620"
