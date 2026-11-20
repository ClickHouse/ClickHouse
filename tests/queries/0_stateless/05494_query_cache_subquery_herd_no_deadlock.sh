#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: Messes with internal cache

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Two queries acquire the herd tokens of the same two subqueries in a different order: the first one plans `A` then `B`,
# the second one `B` then `A`. A token is held from planning until the subquery has run, so if a query owning one subquery
# token waited for another one, both queries could wait for each other until `query_cache_herd_wait_timeout`.
# A query owning a subquery token runs other subqueries independently instead, so all queries finish promptly.

A="SELECT sum(number) AS x FROM numbers(3000000)"
B="SELECT sum(number + 1) AS x FROM numbers(3000000)"
SETTINGS="enable_analyzer = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_herd_wait_timeout = 600"

for round in $(seq 1 5); do
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"
    for i in $(seq 1 4); do
        ${CLICKHOUSE_CLIENT} --query "
            SELECT a.x, b.x
            FROM (${A} SETTINGS use_query_cache = 1) AS a, (${B} SETTINGS use_query_cache = 1) AS b
            SETTINGS ${SETTINGS}, max_execution_time = 300" &
        ${CLICKHOUSE_CLIENT} --query "
            SELECT a.x, b.x
            FROM (${B} SETTINGS use_query_cache = 1) AS b, (${A} SETTINGS use_query_cache = 1) AS a
            SETTINGS ${SETTINGS}, max_execution_time = 300" &
    done
    wait
done | sort | uniq -c | sed 's/^ *//'

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"
