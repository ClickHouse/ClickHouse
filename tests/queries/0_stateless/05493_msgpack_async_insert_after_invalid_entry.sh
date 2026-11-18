#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: MsgPack format is not available in the fasttest environment

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# One async insert batch reuses one MsgPack input format for all its entries. Each case queues an
# invalid entry followed by a valid one: the valid entry must be parsed from a clean state.

url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0&async_insert_use_adaptive_busy_timeout=0&async_insert_busy_timeout_min_ms=100000&async_insert_busy_timeout_max_ms=100000"

function insert_pair()
{
    local table=$1 bad=$2 good=$3
    printf '%b' "$bad" | ${CLICKHOUSE_CURL} -sS "${url}&query=INSERT+INTO+${table}+FORMAT+MsgPack" --data-binary @-
    printf '%b' "$good" | ${CLICKHOUSE_CURL} -sS "${url}&query=INSERT+INTO+${table}+FORMAT+MsgPack" --data-binary @-
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE ${table}"
    ${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${table} ORDER BY ALL"
}

${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_scalar (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE t_exception (id UInt64, arr Array(UInt8)) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE t_array (id UInt64, arr Array(UInt8)) ENGINE = MergeTree ORDER BY id;
"

# A uint32 with 1 of its 4 bytes; the valid entry is four rows 1, 2, 3, 4.
insert_pair t_scalar '\xce\x00' '\x01\x02\x03\x04'
# A string inside Array(UInt8) is rejected in the middle of the array.
insert_pair t_exception '\x07\x92\x01\xa1x' '\x01\x93\x01\x02\x03'
# An array of 3 elements with only 1 present.
insert_pair t_array '\x07\x93\x01' '\x01\x93\x01\x02\x03'

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_scalar; DROP TABLE t_exception; DROP TABLE t_array"
