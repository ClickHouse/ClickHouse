#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# A merge run by `OPTIMIZE` adds its profile events to the global counters through its own thread group,
# and the same events are transferred to the query that ran it. The transfer must not add them to the
# global counters again. `clickhouse-local` is a separate process, so its global counters see only this
# merge: one part (a background merge never takes a single part) of 1000 rows, merged by `OPTIMIZE FINAL`.

${CLICKHOUSE_LOCAL} --multiquery "
CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t SELECT number FROM numbers(1000)
    SETTINGS max_insert_block_size = 100000, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, max_insert_threads = 1;
SELECT count() FROM system.parts WHERE table = 't' AND active;
CREATE TABLE merged_rows_before ENGINE = Memory AS SELECT sum(value) AS v FROM system.events WHERE event = 'MergedRows';
OPTIMIZE TABLE t FINAL;
SELECT (SELECT sum(value) FROM system.events WHERE event = 'MergedRows') - (SELECT v FROM merged_rows_before);
" < /dev/null
