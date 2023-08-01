#!/usr/bin/env bash

# shellcheck disable=SC2154

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


check_replicas_read_in_order() {
    # to check this we actually look for at least one log message from MergeTreeInOrderSelectProcessor.
    # hopefully logger's names are a bit more stable than log messages itself
    #
    # NOTE: lack of "current_database = '$CLICKHOUSE_DATABASE'" filter is made on purpose
    $CLICKHOUSE_CLIENT -nq "
        SYSTEM FLUSH LOGS;

        SELECT COUNT() > 0
        FROM system.text_log
        WHERE query_id IN (SELECT query_id FROM system.query_log WHERE query_id != '$1' AND initial_query_id = '$1' AND event_date >= yesterday())
            AND event_date >= yesterday() AND logger_name = 'MergeTreeInOrderSelectProcessor'"
}

# replicas should use reading in order following initiator's decision to execute aggregation in order.
# at some point we had a bug in this logic (see https://github.com/ClickHouse/ClickHouse/pull/45892#issue-1566140414)
test1() {
    query_id="query_id_memory_bound_merging_$RANDOM$RANDOM"
    $CLICKHOUSE_CLIENT --query_id="$query_id" -nq "
        SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

        SELECT URL, EventDate, max(URL)
        FROM remote(test_cluster_one_shard_two_replicas, test.hits)
        WHERE CounterID = 1704509 AND UserID = 4322253409885123546
        GROUP BY CounterID, URL, EventDate
        ORDER BY URL, EventDate
        LIMIT 5 OFFSET 10
        SETTINGS optimize_aggregation_in_order = 1, enable_memory_bound_merging_of_aggregation_results = 1, allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3, use_hedged_requests = 0"
    check_replicas_read_in_order $query_id
}

# replicas should use reading in order following initiator's decision to execute aggregation in order.
# at some point we had a bug in this logic (see https://github.com/ClickHouse/ClickHouse/pull/45892#issue-1566140414)
test2() {
    query_id="query_id_memory_bound_merging_$RANDOM$RANDOM"
    $CLICKHOUSE_CLIENT --query_id="$query_id" -nq "
        SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

        SELECT URL, EventDate, max(URL)
        FROM remote(test_cluster_one_shard_two_replicas, test.hits)
        WHERE CounterID = 1704509 AND UserID = 4322253409885123546
        GROUP BY URL, EventDate
        ORDER BY URL, EventDate
        LIMIT 5 OFFSET 10
        SETTINGS optimize_aggregation_in_order = 1, enable_memory_bound_merging_of_aggregation_results = 1, allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3, use_hedged_requests = 0, query_plan_aggregation_in_order = 1"
    check_replicas_read_in_order $query_id
}

test3() {
    $CLICKHOUSE_CLIENT -nq "
        SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
        SET max_threads = 16, prefer_localhost_replica = 1, read_in_order_two_level_merge_threshold = 1000, query_plan_aggregation_in_order = 1, distributed_aggregation_memory_efficient = 1;

        SELECT replaceRegexpOne(explain, '^ *(\w+).*', '\\1')
        FROM (
            EXPLAIN PIPELINE
            SELECT URL, EventDate, max(URL)
            FROM test.hits
            WHERE CounterID = 1704509 AND UserID = 4322253409885123546
            GROUP BY URL, EventDate
            SETTINGS optimize_aggregation_in_order = 1, enable_memory_bound_merging_of_aggregation_results = 1, allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3, use_hedged_requests = 0
        )
        WHERE explain LIKE '%Aggr%Transform%' OR explain LIKE '%InOrder%'"
}

test1
test2
test3
