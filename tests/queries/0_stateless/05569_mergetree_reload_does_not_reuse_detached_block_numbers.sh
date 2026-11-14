#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-merge-tree
# no-replicated-database: the block numbers below are those of a non-replicated MergeTree.
# no-shared-merge-tree: the test is about the block-number counter of a non-replicated MergeTree.

# After a reload of the table, new inserts must not get the block numbers of the parts in `detached/`,
# otherwise `ATTACH PARTITION` does not restore them: a merged new part covers them (they are renamed to
# `inactive_`), or a second DETACH of a same-named part writes a `_try1` copy that ATTACH skips.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The originals of the detached parts and the empty part covering them must be removed before the reload.
SETTINGS="old_parts_lifetime = 0, cleanup_delay_period = 1, cleanup_delay_period_random_add = 0, max_cleanup_delay_period = 1,
    merge_tree_clear_old_parts_interval_seconds = 1, remove_empty_parts = 1, empty_parts_lifetime = 0"

wait_for_no_parts()
{
    for _ in {1..600}; do
        [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '$1' AND partition_id = '$2'") == 0 ]] && return
        sleep 0.1
    done
    echo "the parts of $1 in partition $2 were not removed"
}

echo "covering"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_cover;
    CREATE TABLE t_cover (p UInt8, k UInt32) ENGINE = MergeTree PARTITION BY p ORDER BY k SETTINGS $SETTINGS;
    INSERT INTO t_cover SETTINGS async_insert = 0 VALUES (1, 1);
    INSERT INTO t_cover SETTINGS async_insert = 0 VALUES (2, 1);
    ALTER TABLE t_cover DETACH PARTITION 2;
"
wait_for_no_parts t_cover 2
$CLICKHOUSE_CLIENT -q "
    SELECT (SELECT max(max_block_number) FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_cover')
         > (SELECT max(max_block_number) FROM system.parts WHERE database = currentDatabase() AND table = 't_cover');
    DETACH TABLE t_cover;
    ATTACH TABLE t_cover;
    INSERT INTO t_cover SETTINGS async_insert = 0 VALUES (2, 2);
    INSERT INTO t_cover SETTINGS async_insert = 0 VALUES (2, 3);
    SELECT (SELECT min(min_block_number) FROM system.parts WHERE database = currentDatabase() AND table = 't_cover' AND partition_id = '2' AND active)
         > (SELECT max(max_block_number) FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_cover');
    OPTIMIZE TABLE t_cover PARTITION 2 FINAL;
    ALTER TABLE t_cover DETACH PARTITION 2;
    ALTER TABLE t_cover ATTACH PARTITION 2;
    SELECT p, groupArray(k) FROM (SELECT p, k FROM t_cover ORDER BY p, k) GROUP BY p ORDER BY p;
    SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_cover';
"

echo "try_suffix"
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_try;
    CREATE TABLE t_try (k UInt32) ENGINE = MergeTree ORDER BY k SETTINGS $SETTINGS;
    INSERT INTO t_try SETTINGS async_insert = 0 VALUES (1);
    ALTER TABLE t_try DETACH PARTITION tuple();
"
wait_for_no_parts t_try all
$CLICKHOUSE_CLIENT -q "
    DETACH TABLE t_try;
    ATTACH TABLE t_try;
    INSERT INTO t_try SETTINGS async_insert = 0 VALUES (2);
    ALTER TABLE t_try DETACH PARTITION tuple();
    ALTER TABLE t_try ATTACH PARTITION tuple();
    SELECT groupArray(k) FROM (SELECT k FROM t_try ORDER BY k);
    SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_try';
"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE t_cover;
    DROP TABLE t_try;
"
