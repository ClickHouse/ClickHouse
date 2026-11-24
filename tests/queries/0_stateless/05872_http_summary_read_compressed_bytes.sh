#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `read_compressed_bytes` in `X-ClickHouse-Summary` is the `ReadCompressedBytes` profile event of the whole query,
# including the parts executed on remote servers.

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t (x UInt64, s String) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t SELECT number, toString(cityHash64(number)) FROM numbers(100000);
"

# Disable the caches which can serve the data without reading it from disk.
SETTINGS="SETTINGS use_uncompressed_cache = 0, use_columns_cache = 0, use_query_condition_cache = 0"

function check()
{
    local table=$1
    local query_id="${CLICKHOUSE_DATABASE}_$RANDOM$RANDOM"

    local summary
    summary=$(${CLICKHOUSE_CURL} -sS -D - -o /dev/null "${CLICKHOUSE_URL}&query_id=${query_id}" \
        -d "SELECT sum(x), sum(length(s)) FROM ${table} FORMAT Null ${SETTINGS}" | grep -i '^X-ClickHouse-Summary:')
    local read_compressed_bytes
    read_compressed_bytes=$(echo "$summary" | grep -o '"read_compressed_bytes":"[0-9]*"' | grep -o '[0-9]\+')

    # The query is logged after the response is sent, so one `SYSTEM FLUSH LOGS` right after can miss it.
    local logged=0
    for _ in $(seq 1 60); do
        ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
        logged=$(${CLICKHOUSE_CLIENT} -q "
            SELECT sum(ProfileEvents['ReadCompressedBytes'])
            FROM system.query_log
            WHERE event_date >= yesterday() AND type = 'QueryFinish' AND initial_query_id = '${query_id}'")
        [ "$logged" = "$read_compressed_bytes" ] && break
        sleep 0.5
    done

    echo -e "$(( read_compressed_bytes > 0 ))\t$(( logged == read_compressed_bytes ))"
}

check "t"
check "remote('127.0.0.2', ${CLICKHOUSE_DATABASE}, t)"
