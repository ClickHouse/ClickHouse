#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_05618_$CLICKHOUSE_DATABASE"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05618 (x Int32) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} --query "CREATE USER $user IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER, SYSTEM PREWARM MARK CACHE ON *.* TO $user"
${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM MERGES ON $CLICKHOUSE_DATABASE.t_05618 TO $user"

# A bare table name refers to the current database, as it does without ON CLUSTER.
on_cluster() { ${CLICKHOUSE_CLIENT} --user "$user" --distributed_ddl_output_mode none --query "$1"; }

on_cluster "SYSTEM STOP MERGES ON CLUSTER test_shard_localhost t_05618"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_05618 VALUES (1)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_05618 VALUES (2)"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE t_05618 FINAL -- { serverError ABORTED }"
on_cluster "SYSTEM START MERGES ON CLUSTER test_shard_localhost t_05618"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE t_05618 FINAL"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_05618' AND active"
on_cluster "SYSTEM PREWARM MARK CACHE ON CLUSTER test_shard_localhost t_05618"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05618_async (x Int32) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} --async_insert 1 --wait_for_async_insert 0 --async_insert_use_adaptive_busy_timeout 0 --async_insert_busy_timeout_max_ms 600000 --query "INSERT INTO t_05618_async VALUES (1)"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none --query "SYSTEM FLUSH ASYNC INSERT QUEUE ON CLUSTER test_shard_localhost t_05618_async"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_05618_async"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH ASYNC INSERT QUEUE t_05618_async"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05618_async"

${CLICKHOUSE_CLIENT} --query "DROP USER $user"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05618"
