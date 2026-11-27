#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: Messes with internal cache

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A first client cancel with `partial_result_on_first_cancel` stops only the sources. The query finishes successfully with the
# rows read so far, and such a partial subquery result must not be stored in the query cache.
# https://github.com/ClickHouse/ClickHouse/issues/124409

$CLICKHOUSE_CLIENT -q "SYSTEM DROP QUERY CACHE"

QUERY_ID="${CLICKHOUSE_DATABASE}_query_cache_partial_result_on_cancel"

# One row per block, 0.1 s per row: the query runs for about 10 s unless it is cancelled.
$CLICKHOUSE_CLIENT --query_id="$QUERY_ID" --partial_result_on_first_cancel=1 --max_block_size=1 -q "
    SELECT sum(s) FROM (SELECT sleepEachRow(0.1) AS s, number FROM numbers(100) SETTINGS use_query_cache = 1) FORMAT Null" &
pid=$!

for _ in {0..60}
do
    $CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM system.processes WHERE query_id = '$QUERY_ID'" | grep -qF '1' && break
    sleep 0.5
done

kill -SIGINT $pid
wait $pid

$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.query_cache WHERE is_subquery = 1"

$CLICKHOUSE_CLIENT -q "SYSTEM DROP QUERY CACHE"
