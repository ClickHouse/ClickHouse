#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# SQL-managed cluster DDL persists cluster metadata, so `CREATE` / `ALTER` / `DROP CLUSTER`
# must be rejected in read-only contexts and when DDL is prohibited: the cluster access flags
# are part of `ContextAccess` `not_readonly_flags` and `ddl_flags`.

cluster="c_sql_cluster_ro_${CLICKHOUSE_DATABASE}"
create_stmt="CREATE CLUSTER ${cluster} (SHARD (REPLICA (host = '127.0.0.1', port = 9000)))"
alter_stmt="ALTER CLUSTER ${cluster} (SHARD (REPLICA (host = '127.0.0.1', port = 9000), REPLICA (host = '127.0.0.2', port = 9000)))"
drop_stmt="DROP CLUSTER ${cluster}"

${CLICKHOUSE_CLIENT} --query "DROP CLUSTER IF EXISTS ${cluster}"

expect()
{
    local code="$1"
    shift
    "$@" 2>&1 | grep -o "${code}" | head -1
}

# `readonly = 2` (the mode safe HTTP methods set) rejects all three cluster DDLs.
expect READONLY ${CLICKHOUSE_CLIENT} --readonly 2 --query "${create_stmt}"
expect READONLY ${CLICKHOUSE_CLIENT} --readonly 2 --query "${alter_stmt}"
expect READONLY ${CLICKHOUSE_CLIENT} --readonly 2 --query "${drop_stmt}"

# The strict, user-set `readonly = 1` rejects them as well.
expect READONLY ${CLICKHOUSE_CLIENT} --readonly 1 --query "${create_stmt}"
expect READONLY ${CLICKHOUSE_CLIENT} --readonly 1 --query "${alter_stmt}"
expect READONLY ${CLICKHOUSE_CLIENT} --readonly 1 --query "${drop_stmt}"

# The safe-method HTTP path: a `GET` request implies `readonly = 2`.
expect READONLY ${CLICKHOUSE_CURL} -sS -G "${CLICKHOUSE_URL}" --data-urlencode "query=${create_stmt}"
expect READONLY ${CLICKHOUSE_CURL} -sS -G "${CLICKHOUSE_URL}" --data-urlencode "query=${alter_stmt}"
expect READONLY ${CLICKHOUSE_CURL} -sS -G "${CLICKHOUSE_URL}" --data-urlencode "query=${drop_stmt}"

# `allow_ddl = 0` prohibits cluster DDL.
expect QUERY_IS_PROHIBITED ${CLICKHOUSE_CLIENT} --allow_ddl 0 --query "${create_stmt}"
expect QUERY_IS_PROHIBITED ${CLICKHOUSE_CLIENT} --allow_ddl 0 --query "${alter_stmt}"
expect QUERY_IS_PROHIBITED ${CLICKHOUSE_CLIENT} --allow_ddl 0 --query "${drop_stmt}"

# No cluster must have been created by any of the rejected queries.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.clusters WHERE cluster = '${cluster}'"
