#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS tab;
    DROP TABLE IF EXISTS tab_tuple;
    CREATE TABLE tab (x UInt64) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE tab_tuple (x Tuple(UInt64, UInt64)) ENGINE = MergeTree ORDER BY tuple();
"

# We should have correct env vars from shell_config.sh to run this test
python3 "$CUR_DIR"/05617_async_insert_native_serialization_kind_gate.python

# The server must still be alive, and only the block in the default serialization is stored.
$CLICKHOUSE_CLIENT -q "
    SELECT 'server alive';
    SELECT count(), sum(x) FROM tab;
    SELECT count() FROM tab_tuple;
"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE tab;
    DROP TABLE tab_tuple;
"
