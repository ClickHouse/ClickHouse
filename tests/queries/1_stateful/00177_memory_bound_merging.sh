#!/usr/bin/env bash

# shellcheck disable=SC2154

unset CLICKHOUSE_LOG_COMMENT

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


check_replicas_read_in_order() {
    $CLICKHOUSE_CLIENT -nq "
        SYSTEM FLUSH LOGS;

        SELECT COUNT() > 0
        FROM system.text_log
        WHERE query_id IN (SELECT query_id FROM system.query_log WHERE query_id != '$1' AND initial_query_id = '$1' AND event_date >= yesterday())
            AND event_date >= yesterday() AND logger_name = 'MergeTreeInOrderSelectProcessor'"
}

test1() {
    query_id="query_id_memory_bound_merging_$RANDOM$RANDOM"
    $CLICKHOUSE_CLIENT --query_id="$query_id" -q "
        SELECT URL, EventDate, max(URL)
        FROM remote('127.0.0.{1,2}', test, hits)
        WHERE CounterID = 1704509 AND UserID = 4322253409885123546
        GROUP BY CounterID, URL, EventDate, EventDate
        ORDER BY URL, EventDate
        LIMIT 5 OFFSET 10
        SETTINGS optimize_aggregation_in_order = 1, enable_memory_bound_merging_of_aggregation_results = 1, allow_experimental_parallel_reading_from_replicas = 1, max_parallel_replicas = 3, use_hedged_requests = 0"
    check_replicas_read_in_order $query_id
}

test2() {
    query_id="query_id_memory_bound_merging_$RANDOM$RANDOM"
    $CLICKHOUSE_CLIENT --query_id="$query_id" -q "
        SELECT URL, EventDate, max(URL)
        FROM remote('127.0.0.{1,2}', test, hits)
        WHERE CounterID = 1704509 AND UserID = 4322253409885123546
        GROUP BY URL, EventDate, EventDate
        ORDER BY URL, EventDate
        LIMIT 5 OFFSET 10
        SETTINGS optimize_aggregation_in_order = 1, enable_memory_bound_merging_of_aggregation_results = 1, allow_experimental_parallel_reading_from_replicas = 1, max_parallel_replicas = 3, use_hedged_requests = 0, query_plan_aggregation_in_order = 1"
    check_replicas_read_in_order $query_id
}

test3() {
    $CLICKHOUSE_CLIENT -nq "
        SET max_threads = 16, prefer_localhost_replica = 1, read_in_order_two_level_merge_threshold = 1000;

        EXPLAIN PIPELINE
        SELECT URL, EventDate, max(URL)
        FROM remote('127.0.0.{1,2}', test, hits)
        WHERE CounterID = 1704509 AND UserID = 4322253409885123546
        GROUP BY URL, EventDate
        SETTINGS optimize_aggregation_in_order = 1, enable_memory_bound_merging_of_aggregation_results = 1, allow_experimental_parallel_reading_from_replicas = 1, max_parallel_replicas = 3, use_hedged_requests = 0"
}

test1
test2
test3
