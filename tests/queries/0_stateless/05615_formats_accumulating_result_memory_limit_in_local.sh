#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every stored block is larger than the memory limit, so the format that accumulates the result fails to copy it
# when the next block arrives: the query must fail with an error, not crash.
for format in PrettyCompactMonoBlock PrettyCompact JSONColumns; do
    $CLICKHOUSE_LOCAL --query "
        CREATE TABLE t (x UInt64) ENGINE = Memory;
        INSERT INTO t SELECT number FROM numbers(5000000) SETTINGS max_block_size = 5000000, max_insert_block_size = 5000000;
        INSERT INTO t SELECT number FROM numbers(5000000) SETTINGS max_block_size = 5000000, max_insert_block_size = 5000000;
        SELECT * FROM t FORMAT $format SETTINGS max_threads = 1, max_memory_usage = '20Mi',
            output_format_pretty_squash_consecutive_ms = 1000000, output_format_pretty_squash_max_wait_ms = 1000000;
    " 2>&1 >/dev/null | grep -o -m1 'MEMORY_LIMIT_EXCEEDED' | head -n1
    echo "$format ${PIPESTATUS[0]}"
done
