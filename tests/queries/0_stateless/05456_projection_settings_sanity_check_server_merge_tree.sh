#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A server with a small background pool needs lower `number_of_free_entries_in_pool_to_*` settings, set either in the
# `merge_tree` section of its configuration or in the `SETTINGS` of a table. The settings of a projection used to be
# checked against the pool with the built-in defaults of these settings, so `CREATE TABLE` with a projection and
# `ALTER TABLE ... ADD PROJECTION` were refused with `BAD_ARGUMENTS` in both cases.

# With `background_pool_size = 2` there are 4 tasks in the pool, fewer than the built-in defaults.
SMALL_POOL=--background_pool_size=2

QUERIES="
    ALTER TABLE t ADD PROJECTION by_ts_id (SELECT id, ts ORDER BY (ts, id));
    ALTER TABLE t ADD PROJECTION by_ts_small (SELECT id, ts ORDER BY ts) WITH SETTINGS (index_granularity = 64);
    INSERT INTO t VALUES (1, '2020-01-01 00:00:00'), (2, '2020-01-02 00:00:00');
    SELECT count() FROM t;
    SELECT groupArray(name) FROM (SELECT name FROM system.projections WHERE database = currentDatabase() AND table = 't' ORDER BY name);
"

echo "merge_tree section"
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE t (id UInt64, ts DateTime, PROJECTION by_ts (SELECT id, ts ORDER BY ts)) ENGINE = MergeTree ORDER BY id;
    ${QUERIES}
" -- "${SMALL_POOL}" \
    --merge_tree.number_of_free_entries_in_pool_to_execute_mutation=2 \
    --merge_tree.number_of_free_entries_in_pool_to_lower_max_size_of_merge=2 \
    --merge_tree.number_of_free_entries_in_pool_to_execute_optimize_entire_partition=2

echo "table SETTINGS"
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE t (id UInt64, ts DateTime, PROJECTION by_ts (SELECT id, ts ORDER BY ts)) ENGINE = MergeTree ORDER BY id
        SETTINGS number_of_free_entries_in_pool_to_execute_mutation = 2,
            number_of_free_entries_in_pool_to_lower_max_size_of_merge = 2,
            number_of_free_entries_in_pool_to_execute_optimize_entire_partition = 2;
    ${QUERIES}
" -- "${SMALL_POOL}"

# The settings of the table are still checked against the pool.
echo "table without the lower settings"
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE t (id UInt64) ENGINE = MergeTree ORDER BY id;
" -- "${SMALL_POOL}" 2>&1 | grep -o -m1 "number_of_free_entries_in_pool_to_execute_mutation' setting (20)"
