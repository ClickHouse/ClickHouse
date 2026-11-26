#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `NativeWithDefaults` block of 3 rows: `id` has no defaults mask, row 2 of `x` takes the default,
# rows 1 and 3 of `s` take the default, row 2 of `n` takes the default and row 1 of `n` is NULL.
DATA_HEX='04030269640655496e7433320001000000020000000300000001780655496e743332010001000a0000001e000000017306537472696e67010100010374776f016e104e756c6c61626c652855496e743332290100010001000000000003000000'

function data()
{
    $CLICKHOUSE_LOCAL --query "SELECT unhex('$DATA_HEX') FORMAT RawBLOB"
}

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_native_with_defaults;
    CREATE TABLE t_native_with_defaults
    (
        id UInt32,
        x UInt32 DEFAULT 42,
        s String DEFAULT concat('s', toString(id)),
        n Nullable(UInt32) DEFAULT 7
    )
    ENGINE = MergeTree ORDER BY id;
"

echo 'HTTP'
data | $CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+t_native_with_defaults+FORMAT+NativeWithDefaults" --data-binary @-
$CLICKHOUSE_CLIENT --query "SELECT * FROM t_native_with_defaults ORDER BY id; TRUNCATE TABLE t_native_with_defaults;"

# The client parses the data and computes the defaults itself.
echo 'client'
data | $CLICKHOUSE_CLIENT --query "INSERT INTO t_native_with_defaults FORMAT NativeWithDefaults"
$CLICKHOUSE_CLIENT --query "SELECT * FROM t_native_with_defaults ORDER BY id; TRUNCATE TABLE t_native_with_defaults;"

echo 'async insert'
data | $CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&query=INSERT+INTO+t_native_with_defaults+FORMAT+NativeWithDefaults" --data-binary @-
$CLICKHOUSE_CLIENT --query "SELECT * FROM t_native_with_defaults ORDER BY id;"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_native_with_defaults"
