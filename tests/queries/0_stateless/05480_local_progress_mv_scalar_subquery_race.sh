#!/usr/bin/env bash
# The interactive cancel callback of `LocalConnection` sends accumulated progress to the client.
# It is also called from pipeline threads, e.g. by a scalar subquery of a materialized view during
# `INSERT ... SELECT`, concurrently with the client thread. Only the client thread may send progress,
# otherwise `ClientBase::onProgress` runs concurrently (a data race reported by TSan).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} --multiquery "
    CREATE TABLE src (x UInt64) ENGINE = Null;
    CREATE TABLE dst (x UInt64, s UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE MATERIALIZED VIEW mv TO dst AS SELECT x, (SELECT count() FROM numbers(100000)) AS s FROM src;

    INSERT INTO src SELECT number FROM numbers(100000)
        SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, min_insert_block_size_bytes = 0,
            max_threads = 8, max_insert_threads = 8, parallel_view_processing = 1, interactive_delay = 1;

    SELECT count(), uniqExact(x), any(s) FROM dst;
"

# The progress of the scalar subquery is still delivered to the final statistics.
${CLICKHOUSE_LOCAL} --query "SELECT (SELECT count() FROM numbers(100000)) FROM numbers(10) FORMAT JSON" | grep -o '"rows_read": [0-9]*'
