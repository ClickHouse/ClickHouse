#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the dispatched plan needs the stateless worker configuration (tests/config/config.d/distributed_query.xml).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

export TABLE="t_two_initiators"
${CLICKHOUSE_CLIENT} --multiline --query "
CREATE TABLE $TABLE (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 1024, max_concurrent_queries = 1, min_marks_to_honor_max_concurrent_queries = 1;

SYSTEM STOP MERGES $TABLE;
INSERT INTO $TABLE SELECT number, number FROM numbers(100000) SETTINGS max_insert_threads = 1;
INSERT INTO $TABLE SELECT number + 100000, number FROM numbers(100000) SETTINGS max_insert_threads = 1;
"

python3 "$CURDIR"/05876_max_concurrent_queries_query_id_from_two_initiators.python

${CLICKHOUSE_CLIENT} --query "DROP TABLE $TABLE"
