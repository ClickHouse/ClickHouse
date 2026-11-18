#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "CREATE TABLE j (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k)"

# The loopback query of RETURNING reads the Join table while the insert writes it, under the same initial query id.
# The insert either succeeds or fails with DEADLOCK_AVOIDED (473); any other error is printed.
for _ in {1..3}; do
    $CLICKHOUSE_CLIENT --format Null -q "
        INSERT INTO j RETURNING (SELECT * FROM remote('127.0.0.1', currentDatabase(), 'j') LIMIT 1)
        SELECT number, number FROM numbers(300000)
        SETTINGS prefer_localhost_replica = 0, max_threads = 1, max_insert_threads = 1, max_block_size = 100000, min_insert_block_size_rows = 100000
    " 2>&1 | grep -o 'Code: [0-9]*' | grep -v -x 'Code: 473'
    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE j"
done

echo OK
