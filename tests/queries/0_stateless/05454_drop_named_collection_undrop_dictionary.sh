#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-replicated-database: the named collection is created on one replica only, so the other replicas cannot create the dictionary.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `UNDROP TABLE` of a dictionary moves its metadata back into the database, and then fails, because the
# short `ATTACH TABLE` it uses does not accept a dictionary. Its metadata stays, so the named collection its
# source references must stay protected from `DROP NAMED COLLECTION`: otherwise the dictionary attached
# later (or at the next server start) cannot load.

NC="nc_${CLICKHOUSE_DATABASE}"
DB="${CLICKHOUSE_DATABASE}_1"
READER_ID="reader_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS host = 'localhost', port = ${CLICKHOUSE_PORT_TCP}, user = 'default', password = '', db = 'system', table = 'one';
CREATE DATABASE ${DB} ENGINE = Atomic;
CREATE DICTIONARY ${DB}.d (dummy UInt8) PRIMARY KEY dummy SOURCE(CLICKHOUSE(NAME ${NC})) LAYOUT(FLAT()) LIFETIME(0);
SELECT dictHas('${DB}.d', toUInt64(0)) FORMAT Null;
"

# A dictionary has no data on disk and is removed from the queue of dropped tables at once, unless a query
# still holds it. The query holds it once it has started to read.
${CLICKHOUSE_CLIENT} --query_id "${READER_ID}" -q "SELECT sleepEachRow(1) FROM ${DB}.d AS d, numbers(1000) AS n SETTINGS max_block_size = 1, function_sleep_max_microseconds_per_block = 0" >/dev/null 2>&1 &
READER_PID=$!

while [[ $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query_id = '${READER_ID}' AND read_rows > 0") == 0 ]]
do
    sleep 0.1
done

# Do not wait for the reader to release the dictionary: it holds it until it is killed below.
${CLICKHOUSE_CLIENT} -q "DROP DICTIONARY ${DB}.d SETTINGS database_atomic_wait_for_drop_and_detach_synchronously = 0"

# `UNDROP` moves the metadata and takes the dictionary out of the queue, then waits for the reader to finish.
${CLICKHOUSE_CLIENT} -q "UNDROP TABLE ${DB}.d" > "${CLICKHOUSE_TMP}/undrop.log" 2>&1 &
UNDROP_PID=$!

while [[ $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.dropped_tables WHERE database = '${DB}' AND table = 'd'") != 0 ]]
do
    sleep 0.1
done

${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '${READER_ID}' SYNC" >/dev/null
wait ${READER_PID}
wait ${UNDROP_PID}
grep -o -m1 "Code: [0-9]*" "${CLICKHOUSE_TMP}/undrop.log"

${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${NC}" 2>&1 | grep -o -m1 "NAMED_COLLECTION_IS_USED"

${CLICKHOUSE_CLIENT} -m -q "
ATTACH DICTIONARY ${DB}.d;
SELECT dictHas('${DB}.d', toUInt64(0));
DROP DATABASE ${DB} SYNC;
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"
