#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# default coordinator
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0" -d @- <<< "SELECT * FROM test.hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10 SETTINGS allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas = 3, use_hedged_requests=0, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost'" -v |& grep -e "X-ClickHouse-Summary:" | grep -F -m 1 '"total_rows_to_read":"8873898"' | awk -F ',' '{print $5}'

query_id="parallel_replicas_progress_bar$RANDOM$RANDOM"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0&query_id=${query_id}" -d @- <<< "SELECT CounterID, sipHash64(URL) FROM test.hits order by CounterID limit 5 offset 2000000 SETTINGS optimize_read_in_order=1, allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas = 3, use_hedged_requests=0, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost', parallel_replicas_single_task_marks_count_multiplier = 0.001" -v |& grep -e "X-ClickHouse-Summary:" | grep -F -m 1 '"total_rows_to_read":"8873898"' | awk -F ',' '{print $5}'

check_replicas_read_in_order() {
    # NOTE: lack of "current_database = '$CLICKHOUSE_DATABASE'" filter is made on purpose
    $CLICKHOUSE_CLIENT -nq "
        SYSTEM FLUSH LOGS;

        SELECT COUNT() > 0
        FROM system.text_log
        WHERE query_id IN (SELECT query_id FROM system.query_log WHERE query_id != '$1' AND initial_query_id = '$1' AND event_date >= yesterday())
            AND event_date >= yesterday() AND message ILIKE '%Reading%ranges in order%'"
}

check_replicas_read_in_order $query_id
