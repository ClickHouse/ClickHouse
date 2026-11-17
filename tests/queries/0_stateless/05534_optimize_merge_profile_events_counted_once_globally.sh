#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A merge run by `OPTIMIZE` adds its profile events to the counters of the query, and through them to the global
# counters, once. `clickhouse-local` is a separate process, so its global counters see only the merges below.
# Each partition has one part, which a background merge never takes, so only `OPTIMIZE FINAL` merges it.
# One partition takes the sequential path of `OPTIMIZE FINAL`, two partitions take the path with parallel merges.

INSERT_SETTINGS="max_block_size = 65536, max_threads = 1, max_insert_block_size = 100000, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, max_insert_threads = 1"

${CLICKHOUSE_LOCAL} --multiquery "
CREATE TABLE t_one (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_two (x UInt64) ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x;
INSERT INTO t_one SELECT number FROM numbers(1000) SETTINGS ${INSERT_SETTINGS};
INSERT INTO t_two SELECT number FROM numbers(1000) SETTINGS ${INSERT_SETTINGS};
SELECT table, count() FROM system.parts WHERE database = currentDatabase() AND active GROUP BY table ORDER BY table;

CREATE TABLE merged_rows (step UInt8, value UInt64) ENGINE = Memory;
INSERT INTO merged_rows SELECT 0, sum(value) FROM system.events WHERE event = 'MergedRows';
OPTIMIZE TABLE t_one FINAL;
INSERT INTO merged_rows SELECT 1, sum(value) FROM system.events WHERE event = 'MergedRows';
OPTIMIZE TABLE t_two FINAL;
INSERT INTO merged_rows SELECT 2, sum(value) FROM system.events WHERE event = 'MergedRows';
SELECT after.step, after.value - before.value FROM merged_rows AS before JOIN merged_rows AS after ON after.step = before.step + 1 ORDER BY after.step;
" < /dev/null

# The same merges on the server: each `OPTIMIZE` query gets the profile events of its merges.
${CLICKHOUSE_CLIENT} --multiquery "
CREATE TABLE t_one (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_two (x UInt64) ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x;
INSERT INTO t_one SELECT number FROM numbers(1000) SETTINGS ${INSERT_SETTINGS};
INSERT INTO t_two SELECT number FROM numbers(1000) SETTINGS ${INSERT_SETTINGS};
OPTIMIZE TABLE t_one FINAL;
OPTIMIZE TABLE t_two FINAL;
SYSTEM FLUSH LOGS query_log;
SELECT if(query LIKE '%t_one%', 't_one', 't_two') AS t, ProfileEvents['MergedRows']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
    AND type = 'QueryFinish' AND query LIKE 'OPTIMIZE TABLE t_%'
ORDER BY t;
DROP TABLE t_one;
DROP TABLE t_two;
"
