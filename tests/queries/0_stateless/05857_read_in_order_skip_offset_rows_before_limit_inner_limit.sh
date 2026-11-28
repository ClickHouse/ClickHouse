#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings

# When the OFFSET-skip read-in-order optimization drops leading granules, a LIMIT below the OFFSET no longer sees
# the skipped rows. It must pass as many fewer rows, otherwise `rows_before_limit_at_least` adds the skipped rows
# to the rows it passed from the tail and reports more rows than the inner LIMIT can produce.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_skip_offset_rows_before_limit;
    CREATE TABLE t_skip_offset_rows_before_limit (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
    INSERT INTO t_skip_offset_rows_before_limit SELECT number FROM numbers(1000);
"

# The inner LIMIT is exactly `outer offset + outer limit`, the largest prefix the outer query can use.
for inner_limit in 6 20; do
    for skip_offset in 0 1; do
        rows_before_limit=$($CLICKHOUSE_CLIENT -q "
            SELECT k FROM (SELECT k FROM t_skip_offset_rows_before_limit ORDER BY k LIMIT $inner_limit) LIMIT $((inner_limit - 2)) OFFSET 2
            SETTINGS optimize_read_in_order = 1, query_plan_optimize_read_in_order_skip_offset = $skip_offset,
                max_threads = 1, max_block_size = 65536, output_format_write_statistics = 0
            FORMAT JSONCompact" | grep -oP '"rows_before_limit_at_least": \K[0-9]+')
        echo "inner limit $inner_limit, skip_offset $skip_offset: $((rows_before_limit <= inner_limit))"
    done
done

$CLICKHOUSE_CLIENT -q "DROP TABLE t_skip_offset_rows_before_limit"
