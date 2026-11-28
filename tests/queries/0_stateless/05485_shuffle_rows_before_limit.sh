#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e -o pipefail

SETTINGS="allow_experimental_shuffle_query = 1, query_plan_use_shuffle_for_order_by_rand = 1, exact_rows_before_limit = 1, max_rows_to_sort = 0, max_bytes_to_sort = 0, max_bytes_before_external_sort = 0, max_bytes_ratio_before_external_sort = 0, max_block_size = 1000, enable_parallel_replicas = 0"

for threads in 1 4; do
    for clause in "SHUFFLE" "ORDER BY rand()"; do
        # Ensure the checks exercise `ReservoirSampleTransform`, not ordinary sorting.
        $CLICKHOUSE_CLIENT --query "EXPLAIN PIPELINE SELECT number FROM numbers_mt(10000) $clause LIMIT 10 SETTINGS $SETTINGS, max_threads = $threads" \
            | grep 'ReservoirSampleTransform' > /dev/null

        for source in "numbers_mt(10000) WHERE number % 2 = 0" "numbers_mt(7)" "numbers_mt(0)"; do
            $CLICKHOUSE_CLIENT --query "SELECT number FROM $source $clause LIMIT 10 FORMAT JSON SETTINGS $SETTINGS, max_threads = $threads" \
                | grep -o '"rows_before_limit_at_least": [0-9]*'
        done
    done
done
