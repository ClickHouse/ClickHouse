#!/usr/bin/env bash
# Tags: shard
# The limit on groups of a trivial `GROUP BY ... LIMIT` does not override the limits of the user, also on shards.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 10;
    INSERT INTO t SELECT number FROM numbers(1000);
    CREATE TABLE d AS t ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t);
"

for profile in "50 CONST" "20 CONST" "50 CONST, group_by_overflow_mode = 'throw' CONST" "50 CONST, optimize_trivial_group_by_limit_query = 0 CONST"
do
    $CLICKHOUSE_CLIENT -q "
        DROP USER IF EXISTS $user;
        CREATE USER $user IDENTIFIED WITH no_password SETTINGS max_rows_to_group_by = $profile;
        GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO $user;
    "
    for table in t d
    do
        echo -n "$profile, $table: "
        $CLICKHOUSE_CLIENT --user "$user" -q "
            SELECT x FROM $table GROUP BY x LIMIT 30
            SETTINGS max_block_size = 10, max_threads = 1, prefer_localhost_replica = 0,
                optimize_aggregation_in_order = 0, enable_parallel_replicas = 0
        " 2>&1 | awk '/TOO_MANY_ROWS/ { error = 1 } END { print error ? "TOO_MANY_ROWS" : NR }'
    done
done

$CLICKHOUSE_CLIENT -q "DROP USER $user; DROP TABLE d; DROP TABLE t;"
