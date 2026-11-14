#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas, no-replicated-database
# The query plan cache is a single, server-wide cache that this test inspects via the global
# `SYSTEM DROP QUERY PLAN CACHE` and exact `QueryPlanCacheHits` counts, and the test creates a global
# user (no-parallel). The cache only engages with the analyzer and is incompatible with parallel
# replicas. no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables`
# setting.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test: a `Dictionary` table read inside a folded scalar subquery must not be cached even
# when the catalog reaches it through the `lazy_load_tables` stand-in (`StorageTableProxy`), because
# `StorageDictionary::read` checks `dictGet` on the underlying dictionary, which a hit would not repeat.

user="user_05408_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}_lazy"
SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --query_plan_cache_allow_scalar_subqueries=1"

$CLICKHOUSE_CLIENT --query "
    DROP USER IF EXISTS $user;
    CREATE USER $user;
    REVOKE ALL ON *.* FROM $user;
    DROP DATABASE IF EXISTS $db;
    CREATE DATABASE $db ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE $db.t_dict_source (id UInt64, value UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO $db.t_dict_source VALUES (1, 10), (2, 20);
    CREATE DICTIONARY $db.d_05408 (id UInt64, value UInt64) PRIMARY KEY id
        SOURCE(CLICKHOUSE(TABLE 't_dict_source' DB '$db')) LAYOUT(FLAT()) LIFETIME(0);
    CREATE TABLE $db.t_dict_wrapper (id UInt64, value UInt64) ENGINE = Dictionary($db.d_05408);
    GRANT SELECT ON $db.t_dict_wrapper TO $user;
    GRANT dictGet ON $db.d_05408 TO $user;
"

# A stand-in appears when the database is loaded.
$CLICKHOUSE_CLIENT --query "DETACH DATABASE $db"
$CLICKHOUSE_CLIENT --query "ATTACH DATABASE $db"

run_user()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT --user="$user" $SETTINGS --query "$1" 2>&1 | grep -o -m1 -E 'ACCESS_DENIED|^[^ ]+$'
}

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
QUERY="SELECT (SELECT sum(value) FROM $db.t_dict_wrapper)"
run_user "$QUERY"
run_user "$QUERY"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
echo "hits (must be 0): $($CLICKHOUSE_CLIENT --query "
    SELECT ProfileEvents['QueryPlanCacheHits']
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND type = 'QueryFinish'
      AND query = '$QUERY'
    ORDER BY event_time_microseconds DESC
    LIMIT 1")"

$CLICKHOUSE_CLIENT --query "REVOKE dictGet ON $db.d_05408 FROM $user"
run_user "$QUERY"

$CLICKHOUSE_CLIENT --query "
    DROP DATABASE $db;
    DROP USER $user;
"
