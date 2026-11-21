#!/usr/bin/env bash
# Tags: zookeeper
# Tag zookeeper: `ON CLUSTER` queries use the distributed DDL queue.

# A restored temporary object is created by an internal query, so `RESTORE` asks the grant a `CREATE` of the same
# object asks: `CREATE TEMPORARY VIEW` for a view, `CREATE ARBITRARY TEMPORARY TABLE` for an engine other than `Memory`.
# The direct `CREATE` asks the latter for the engine it creates, also when it comes from `default_temporary_table_engine`.
# A temporary object cannot be restored `ON CLUSTER`: the hosts restore without a session to hold it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}"
user_table="user_table_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_memory="user_memory_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_arbitrary="user_arbitrary_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_view="user_view_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_cluster="user_cluster_${CLICKHOUSE_TEST_UNIQUE_NAME}"
bk_merge_tree="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_merge_tree')"
bk_memory="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_memory')"
bk_view="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_view')"
bk_database="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_database')"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${user_table}, ${user_memory}, ${user_arbitrary}, ${user_view}, ${user_cluster};
DROP DATABASE IF EXISTS ${db}_src;
DROP DATABASE IF EXISTS ${db}_restored;
CREATE USER ${user_table}, ${user_memory}, ${user_arbitrary}, ${user_view}, ${user_cluster};
-- No CREATE TABLE anywhere: it would imply CREATE ARBITRARY TEMPORARY TABLE.
GRANT CREATE TEMPORARY TABLE ON *.* TO ${user_table};
GRANT TABLE ENGINE ON MergeTree, TABLE ENGINE ON Log TO ${user_table};
GRANT CREATE TEMPORARY TABLE ON *.* TO ${user_memory};
GRANT TABLE ENGINE ON Memory TO ${user_memory};
GRANT CREATE ARBITRARY TEMPORARY TABLE ON *.* TO ${user_arbitrary};
GRANT CREATE TEMPORARY VIEW ON *.* TO ${user_view};
GRANT CREATE TEMPORARY TABLE, CLUSTER ON *.* TO ${user_cluster};
GRANT TABLE ENGINE ON Memory TO ${user_cluster};
GRANT READ ON DISK TO ${user_table}, ${user_memory}, ${user_arbitrary}, ${user_view}, ${user_cluster};
CREATE DATABASE ${db}_src ENGINE = Atomic;
"

${CLICKHOUSE_CLIENT} -q "
CREATE TEMPORARY TABLE tt_merge_tree (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TEMPORARY TABLE tt_memory (x UInt32) ENGINE = Memory;
CREATE TEMPORARY VIEW tv AS SELECT 42 AS x;
BACKUP TEMPORARY TABLE tt_merge_tree TO ${bk_merge_tree} SETTINGS structure_only = 1 FORMAT Null;
BACKUP TEMPORARY TABLE tt_memory TO ${bk_memory} SETTINGS structure_only = 1 FORMAT Null;
BACKUP TEMPORARY TABLE tv TO ${bk_view} FORMAT Null;
BACKUP DATABASE ${db}_src, TEMPORARY TABLE tt_merge_tree TO ${bk_database} SETTINGS structure_only = 1 FORMAT Null;
"

# Prints `allowed`, `denied: <the first missing grant>`, or the error code of another failure.
function check()
{
    local user=$1 query=$2 out
    shift 2
    if out=$(${CLICKHOUSE_CLIENT} --user "${user}" "$@" -q "${query}" 2>&1); then
        echo 'allowed'
    elif grep -q 'ACCESS_DENIED' <<< "${out}"; then
        echo "denied: $(grep -oE 'grant [A-Z ]+ ON [^ ]+\.( |$)' <<< "${out}" | head -1 | sed -E 's/^grant //; s/\.( )?$//')"
    else
        echo "error: $(grep -oE 'Code: [0-9]+' <<< "${out}" | head -1)"
    fi
}

echo '--- a temporary table with an engine other than Memory needs CREATE ARBITRARY TEMPORARY TABLE'
check "${user_table}" "CREATE TEMPORARY TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x"
echo '--- also when the engine comes from default_temporary_table_engine'
check "${user_table}" "CREATE TEMPORARY TABLE t (x UInt8)" --default_temporary_table_engine Log
echo '--- and when the table is restored'
check "${user_table}" "RESTORE TEMPORARY TABLE tt_merge_tree FROM ${bk_merge_tree} SETTINGS structure_only = 1 FORMAT Null"
echo '--- a restore of a database together with that table is refused before anything is created'
${CLICKHOUSE_CLIENT} -q "GRANT CREATE DATABASE ON ${db}_restored.* TO ${user_table}"
check "${user_table}" "RESTORE DATABASE ${db}_src AS ${db}_restored, TEMPORARY TABLE tt_merge_tree
    FROM ${bk_database} SETTINGS structure_only = 1 FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.databases WHERE name = '${db}_restored'"
echo '--- with the grant, all three are allowed'
${CLICKHOUSE_CLIENT} -q "GRANT CREATE ARBITRARY TEMPORARY TABLE ON *.* TO ${user_table}"
check "${user_table}" "CREATE TEMPORARY TABLE t (x UInt8)" --default_temporary_table_engine Log
check "${user_table}" "RESTORE TEMPORARY TABLE tt_merge_tree FROM ${bk_merge_tree} SETTINGS structure_only = 1 FORMAT Null"
check "${user_table}" "RESTORE DATABASE ${db}_src AS ${db}_restored, TEMPORARY TABLE tt_merge_tree
    FROM ${bk_database} SETTINGS structure_only = 1 FORMAT Null"

echo '--- a Memory temporary table needs only CREATE TEMPORARY TABLE, to create and to restore'
check "${user_memory}" "CREATE TEMPORARY TABLE t (x UInt8)"
check "${user_memory}" "RESTORE TEMPORARY TABLE tt_memory FROM ${bk_memory} SETTINGS structure_only = 1 FORMAT Null"

echo '--- a temporary view needs CREATE TEMPORARY VIEW, which CREATE ARBITRARY TEMPORARY TABLE does not imply'
check "${user_arbitrary}" "CREATE TEMPORARY VIEW v AS SELECT 1"
check "${user_arbitrary}" "RESTORE TEMPORARY TABLE tv FROM ${bk_view} FORMAT Null"
echo '--- and CREATE TEMPORARY VIEW alone is enough to restore one'
check "${user_view}" "CREATE TEMPORARY VIEW v AS SELECT 1"
check "${user_view}" "RESTORE TEMPORARY TABLE tv FROM ${bk_view} FORMAT Null"

echo '--- a temporary table cannot be restored ON CLUSTER, and the query is not sent to the hosts'
check "${user_cluster}" "RESTORE TEMPORARY TABLE tt_memory ON CLUSTER test_shard_localhost FROM ${bk_memory}
    SETTINGS structure_only = 1 FORMAT Null" --distributed_ddl_output_mode throw
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.distributed_ddl_queue WHERE query LIKE '%${CLICKHOUSE_TEST_UNIQUE_NAME}_memory%'"

${CLICKHOUSE_CLIENT} -q "
DROP DATABASE IF EXISTS ${db}_restored;
DROP DATABASE IF EXISTS ${db}_src;
DROP USER IF EXISTS ${user_table}, ${user_memory}, ${user_arbitrary}, ${user_view}, ${user_cluster};
"
