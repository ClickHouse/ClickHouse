#!/usr/bin/env bash
# Tags: no-replicated-database
# A database with `lazy_load_tables` replaces the proxy of a loaded table with the loaded storage, but
# only when nobody holds the proxy: otherwise one query could use the proxy and its table locks while
# another uses the loaded storage. While a query that loaded the table through the proxy is still
# running, the proxy stays. Once that query is gone, the next lookup replaces it.
#
# The proxy does not forward `totalBytesUncompressed`, so `system.tables.total_bytes_uncompressed`
# tells the proxy (NULL) from the loaded `MergeTree` (not NULL).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_lazy"

cleanup() {
    ${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '${CLICKHOUSE_DATABASE}_reader' SYNC SETTINGS kill_throw_if_noop = 0 FORMAT Null" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS \`${DB}\` SYNC" 2>/dev/null
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -m -q "
    CREATE DATABASE \`${DB}\` ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE \`${DB}\`.t (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO \`${DB}\`.t SELECT number FROM numbers(1000);
    DETACH DATABASE \`${DB}\`;
    ATTACH DATABASE \`${DB}\`;
"

state() {
    ${CLICKHOUSE_CLIENT} -q "SELECT engine, total_bytes_uncompressed IS NOT NULL FROM system.tables WHERE database = '${DB}' AND name = 't'"
}

echo "cold: $(state)"

# The reader loads the table through the proxy and holds it until it is killed. Neither `max_block_size` nor
# a join bounds the size of the blocks reliably, and for a block longer than three rows `sleepEachRow` would
# throw `TOO_SLOW` and end the reader, releasing the proxy too early. Lift the limit instead: a long sleep
# still checks every second whether the query has been killed.
${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_DATABASE}_reader" --function_sleep_max_microseconds_per_block 0 -q "
    SELECT sleepEachRow(1) FROM \`${DB}\`.t FORMAT Null" >/dev/null 2>&1 &

# Wait until the reader has loaded the table, i.e. the engine is known.
for _ in {1..600}
do
    [ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query_id = '${CLICKHOUSE_DATABASE}_reader'")" = 1 ] \
        && [ "$(${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = '${DB}' AND name = 't'")" = MergeTree ] \
        && break
    sleep 0.1
done

# The reader holds the proxy, so the lookup above and this one must leave it in place.
echo "held: $(state)"

${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '${CLICKHOUSE_DATABASE}_reader' SYNC SETTINGS kill_throw_if_noop = 0 FORMAT Null"
wait

# The last references of the killed query may go away only a moment after it has left the process list.
for _ in {1..600}
do
    [ "$(state)" = "MergeTree	1" ] && break
    sleep 0.1
done

echo "released: $(state)"
