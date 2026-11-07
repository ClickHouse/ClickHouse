#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression test: a cached plan over a `Buffer` table pushed the row policy into the read, because
# the destination (`MergeTree`) supports `PREWHERE`. After the destination was exchanged with a `Log`
# table, which ignores the pushed filter, the identity of the `Buffer` still validated and a hit
# returned the rows the policy excludes. Such plans are not cached now.
# The plan cache is a single, server-wide cache and the test creates a global user, so it runs in
# isolation (see 04489 for the full rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05509_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS buf;
    DROP TABLE IF EXISTS dst;
    DROP TABLE IF EXISTS dst_log;
    CREATE TABLE dst (id UInt32) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE dst_log (id UInt32) ENGINE = Log;
    INSERT INTO dst VALUES (1), (2);
    INSERT INTO dst_log VALUES (1), (2);
    CREATE TABLE buf (id UInt32) ENGINE = Buffer(${CLICKHOUSE_DATABASE}, dst, 1, 1000, 1000, 1000000, 1000000, 100000000, 100000000);

    DROP USER IF EXISTS $user;
    CREATE USER $user;
    REVOKE ALL ON *.* FROM $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO $user;

    DROP ROW POLICY IF EXISTS p_05509 ON ${CLICKHOUSE_DATABASE}.buf;
    CREATE ROW POLICY p_05509 ON ${CLICKHOUSE_DATABASE}.buf USING id = 1 TO $user;
"

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

query="SELECT id FROM ${CLICKHOUSE_DATABASE}.buf ORDER BY id
    SETTINGS allow_experimental_query_plan_cache = 1, enable_query_plan_cache = 1"

echo "-- warm up with the MergeTree destination"
$CLICKHOUSE_CLIENT --user="$user" --query "$query"
$CLICKHOUSE_CLIENT --user="$user" --query "$query"

$CLICKHOUSE_CLIENT --query "EXCHANGE TABLES dst AND dst_log"

echo "-- the destination is a Log table now"
$CLICKHOUSE_CLIENT --user="$user" --query "$query"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
echo "-- the plan was not cached"
$CLICKHOUSE_CLIENT --query "
    SELECT count(), sum(ProfileEvents['QueryPlanCacheHits'])
    FROM system.query_log
    WHERE current_database = currentDatabase() AND user = '$user' AND type = 'QueryFinish'"

$CLICKHOUSE_CLIENT --query "
    DROP ROW POLICY IF EXISTS p_05509 ON ${CLICKHOUSE_DATABASE}.buf;
    DROP USER IF EXISTS $user;
    DROP TABLE buf;
    DROP TABLE dst;
    DROP TABLE dst_log;
"
