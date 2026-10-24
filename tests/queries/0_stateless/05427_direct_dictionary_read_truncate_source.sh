#!/usr/bin/env bash
# Tags: no-fasttest, use-rocksdb

# Reading a DIRECT dictionary as a table runs its source query inside the reading query. The source query
# must release its table locks only after its processors (and the RocksDB iterator) are destroyed, or a
# TRUNCATE of the source table closes the RocksDB database under the iterator.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE rdb (k UInt64, v String) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
    INSERT INTO rdb SELECT number, toString(number) FROM numbers(300);
    CREATE DICTIONARY dd (k UInt64, v String) PRIMARY KEY k
        SOURCE(CLICKHOUSE(DB '${CLICKHOUSE_DATABASE}' TABLE 'rdb')) LAYOUT(DIRECT());
"

query_id="${CLICKHOUSE_DATABASE}_reader"

# ORDER BY ... LIMIT consumes the whole dictionary before producing a row, so the source query has finished
# before the sleeps start; the reader then runs until it is killed below.
$CLICKHOUSE_CLIENT --query_id "${query_id}" -q "
    SELECT sum(sleepEachRow(0.2)) FROM (SELECT k FROM dd ORDER BY k LIMIT 1000)
    SETTINGS max_block_size = 1, max_threads = 1, function_sleep_max_microseconds_per_block = 100000000
" > /dev/null 2>&1 &
reader_pid=$!

started=0
for _ in {1..200}; do
    if [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '${query_id}' AND read_rows > 0")" = "1" ]; then
        started=1
        break
    fi
    sleep 0.1
done
echo "reader started: ${started}"

$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE rdb SETTINGS lock_acquire_timeout = 10" && echo "truncate: ok"
echo "reader alive after truncate: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '${query_id}'")"

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '${query_id}' SYNC" > /dev/null 2>&1
wait "${reader_pid}" 2>/dev/null

$CLICKHOUSE_CLIENT -q "SELECT count() FROM rdb"
