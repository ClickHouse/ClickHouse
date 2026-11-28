#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-replicated-database, no-fasttest
# no-fasttest: waits for background cleanup
# Random settings limits: use_uncompressed_cache=(0, 0); min_bytes_to_use_mmap_io=(0, 0)
# The uncompressed and mmapped file caches are keyed by file path and keep the dropped part's data for the new part.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function wait_until_no_parts()
{
    for _ in {1..300}; do
        [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND $1") == 0 ]] && return
        sleep 0.2
    done
    echo "Timeout waiting for no parts with $1"
}

SETTINGS="enable_block_number_column = 1, enable_block_offset_column = 1, remove_unused_patch_parts = 0,
    old_parts_lifetime = 1, cleanup_delay_period = 1, max_cleanup_delay_period = 1, cleanup_delay_period_random_add = 0"

# One replica: the patch part of the dropped partition would be applied to the row inserted after FORGET PARTITION.
$CLICKHOUSE_CLIENT -m -q "
CREATE TABLE t (p UInt32, id UInt64, val Int64, tag String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r1') PARTITION BY p ORDER BY (p, id)
SETTINGS $SETTINGS;

INSERT INTO t VALUES (1, 1, 100, 'old');
UPDATE t SET val = 999, tag = 'updated' WHERE id = 1 SETTINGS enable_lightweight_update = 1;
ALTER TABLE t DROP PARTITION 1 SETTINGS alter_sync = 2;
"
wait_until_no_parts "table = 't' AND partition_id = '1'"

$CLICKHOUSE_CLIENT -m -q "
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active AND startsWith(partition_id, 'patch-');
ALTER TABLE t FORGET PARTITION 1; -- { serverError CANNOT_FORGET_PARTITION }
ALTER TABLE t MODIFY SETTING remove_unused_patch_parts = 1;
"
wait_until_no_parts "table = 't' AND startsWith(partition_id, 'patch-')"

$CLICKHOUSE_CLIENT -m -q "
ALTER TABLE t FORGET PARTITION 1;
INSERT INTO t VALUES (1, 1, 555, 'new');
SELECT p, id, val, tag FROM t ORDER BY id;
DROP TABLE t;
"

# Two replicas: the patch part is only on r1, FORGET PARTITION runs on r2.
$CLICKHOUSE_CLIENT -m -q "
CREATE TABLE t1 (p UInt32, id UInt64, val Int64, tag String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t12', 'r1') PARTITION BY p ORDER BY (p, id)
SETTINGS $SETTINGS;
CREATE TABLE t2 (p UInt32, id UInt64, val Int64, tag String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t12', 'r2') PARTITION BY p ORDER BY (p, id)
SETTINGS $SETTINGS;

INSERT INTO t1 VALUES (1, 1, 100, 'old');
SYSTEM SYNC REPLICA t2;
SYSTEM STOP FETCHES t2;
UPDATE t1 SET val = 999, tag = 'updated' WHERE id = 1 SETTINGS enable_lightweight_update = 1;
ALTER TABLE t1 DROP PARTITION 1 SETTINGS alter_sync = 1;
"
wait_until_no_parts "table IN ('t1', 't2') AND partition_id = '1'"

$CLICKHOUSE_CLIENT -m -q "
SELECT table, count() FROM system.parts WHERE database = currentDatabase() AND table IN ('t1', 't2') AND active AND startsWith(partition_id, 'patch-') GROUP BY table ORDER BY table;
ALTER TABLE t2 FORGET PARTITION 1; -- { serverError CANNOT_FORGET_PARTITION }
DROP TABLE t1;
DROP TABLE t2;
"
