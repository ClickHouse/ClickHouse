#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-fasttest
# no-replicated-database: the table has explicit ReplicatedMergeTree arguments.
# no-shared-merge-tree: the test relies on FORGET PARTITION removing the block numbers of a ReplicatedMergeTree partition.
# no-fasttest: waits for the cleanup thread.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# FORGET PARTITION restarts the block numbers of a partition, so the next INSERT writes a part with the name of a
# dropped one. A query condition cache entry of the dropped part with another number of marks must not be used.

# The uncompressed cache is keyed by file path, so it can serve the dropped part's data here too.
SETTINGS="SET use_query_condition_cache = 1, use_uncompressed_cache = 0, insert_keeper_fault_injection_probability = 0;"

function check()
{
    local table=$1 rows_before=$2 rows_after=$3

    $CLICKHOUSE_CLIENT -m -q "
        $SETTINGS
        CREATE TABLE $table (p UInt8, k UInt64, s String)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/$table', 'r1') PARTITION BY p ORDER BY k
        SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi', old_parts_lifetime = 0,
            cleanup_delay_period = 1, max_cleanup_delay_period = 1, cleanup_delay_period_random_add = 0;
        INSERT INTO $table SELECT 1, number, 'pear' FROM numbers($rows_before);
        SELECT name, marks FROM system.parts WHERE database = currentDatabase() AND table = '$table' AND active;
        SELECT sum(k) FROM $table WHERE length(s) = 5;
        ALTER TABLE $table DROP PARTITION 1;
    "

    # DROP PARTITION does not wait until the dropped parts are removed, and FORGET PARTITION needs it.
    while [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '$table'") != 0 ]]; do
        sleep 0.5
    done

    $CLICKHOUSE_CLIENT -m -q "
        $SETTINGS
        ALTER TABLE $table FORGET PARTITION 1;
        INSERT INTO $table SELECT 1, number, 'apple' FROM numbers($rows_after);
        SELECT name, marks FROM system.parts WHERE database = currentDatabase() AND table = '$table' AND active;
        SELECT sum(k) FROM $table WHERE length(s) = 5;
        SELECT sum(k) FROM $table WHERE length(s) = 5;
        DROP TABLE $table SYNC;
    "
}

# The new part has fewer marks than the dropped one.
check t_fewer 1000 16
# The new part has more marks than the dropped one.
check t_more 16 1000
