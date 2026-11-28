#!/usr/bin/env bash
# Tags: no-replicated-database
# A table of a database with `lazy_load_tables` stays usable through the catalog while a query that loaded it
# is still running, and after that query is gone.
#
# The database replaces the stand-in of a loaded table with the loaded storage only when nobody holds the
# stand-in, so the reader below keeps it in the catalog until it is killed, and the next lookup replaces it.
# That timing is not observable from SQL and is not what the test asserts: the stand-in reports the engine from
# the `CREATE` query and forwards everything else to the loaded storage, so `system.tables` shows only whether
# the table is loaded. The test asserts that `system.tables` and a read of the table give the same answers
# at both moments, whichever of the two objects the catalog holds.

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
    ${CLICKHOUSE_CLIENT} -q "SELECT engine, is_loaded, total_rows FROM system.tables WHERE database = '${DB}' AND name = 't'"
}

echo "cold: $(state)"

# The reader loads the table through the proxy and holds it until it is killed. Neither `max_block_size` nor
# a join bounds the size of the blocks reliably, and for a block longer than three rows `sleepEachRow` would
# throw `TOO_SLOW` and end the reader, releasing the proxy too early. Lift the limit instead: a long sleep
# still checks every second whether the query has been killed.
${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_DATABASE}_reader" --function_sleep_max_microseconds_per_block 0 -q "
    SELECT sleepEachRow(1) FROM \`${DB}\`.t FORMAT Null" >/dev/null 2>&1 &

# Wait until the reader has loaded the table.
for _ in {1..600}
do
    [ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query_id = '${CLICKHOUSE_DATABASE}_reader'")" = 1 ] \
        && [ "$(${CLICKHOUSE_CLIENT} -q "SELECT is_loaded FROM system.tables WHERE database = '${DB}' AND name = 't'")" = 1 ] \
        && break
    sleep 0.1
done

# The reader is still running, so these lookups find the stand-in.
echo "held: $(state)"
echo "held count: $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM \`${DB}\`.t")"

${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '${CLICKHOUSE_DATABASE}_reader' SYNC SETTINGS kill_throw_if_noop = 0 FORMAT Null"
wait

echo "released: $(state)"
echo "released count: $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM \`${DB}\`.t")"
