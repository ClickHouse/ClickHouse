#!/usr/bin/env bash
# A `DEFAULT p IN table` of a column missing in a part builds its set on behalf of the reading query.
# The nested reading of the table of the set has to obey the parallelism limits of that query (`max_threads`),
# exactly as the direct `p IN table` spelling does.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_default_set_threads;
    DROP TABLE IF EXISTS t_default_set_threads_rhs;
    CREATE TABLE t_default_set_threads_rhs (p UInt64) ENGINE = MergeTree ORDER BY p;
    INSERT INTO t_default_set_threads_rhs SELECT number * 2 FROM numbers(2000000);
    CREATE TABLE t_default_set_threads (p UInt64) ENGINE = MergeTree ORDER BY p;
    INSERT INTO t_default_set_threads VALUES (1), (2), (3);
    ALTER TABLE t_default_set_threads ADD COLUMN f UInt8 DEFAULT p IN t_default_set_threads_rhs;
"

# Print the result and the number of streams and threads used to read the table of the set: none above one is expected.
function check()
{
    $CLICKHOUSE_CLIENT --enable_parallel_replicas 0 --max_threads 1 --send_logs_level trace -q "$1" 2>&1 \
        | grep -v -F 'executeQuery' \
        | grep -E '^[0-9]+$|t_default_set_threads_rhs.*(rows with [0-9]+ streams|across [0-9]+ threads)' \
        | sed -E 's/.*rows with ([0-9]+) streams.*/streams \1/; s/.*across ([0-9]+) threads.*/threads \1/' \
        | awk '/^(streams|threads)/ { if ($2 > 1) print "more than one: " $0; next } { print }'
}

check "SELECT sum(f) FROM t_default_set_threads"
check "SELECT sum(p IN t_default_set_threads_rhs) FROM t_default_set_threads"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE t_default_set_threads;
    DROP TABLE t_default_set_threads_rhs;
"
