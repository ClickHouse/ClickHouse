#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree
# no-shared-merge-tree: custom disk

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="${CLICKHOUSE_CLIENT} --database_atomic_wait_for_drop_and_detach_synchronously 1 --multiline"

# A disk defined inline with `disk(...)` by a `MergeTree` table stays registered while a table of another engine
# refers to it by name (`SETTINGS disk = '<disk>'`, or the implicit storage policy `__<disk>` of the `Log` family),
# also after the defining table is dropped. It is unregistered once the last of these tables is dropped.
disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_disk"

$CLIENT -q "
DROP TABLE IF EXISTS test_defining SYNC;
DROP TABLE IF EXISTS test_log SYNC;
DROP TABLE IF EXISTS test_tiny_log SYNC;
DROP TABLE IF EXISTS test_stripe_log SYNC;
DROP TABLE IF EXISTS test_set SYNC;
DROP TABLE IF EXISTS test_join SYNC;

CREATE TABLE test_defining (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$disk', type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/$disk/');

CREATE TABLE test_log (a Int32) ENGINE = Log SETTINGS disk = '$disk';
CREATE TABLE test_tiny_log (a Int32) ENGINE = TinyLog SETTINGS storage_policy = '__$disk';
CREATE TABLE test_stripe_log (a Int32) ENGINE = StripeLog SETTINGS disk = '$disk';
CREATE TABLE test_set (a Int32) ENGINE = Set SETTINGS disk = '$disk';
CREATE TABLE test_join (a Int32, b Int32) ENGINE = Join(ANY, LEFT, a) SETTINGS disk = '$disk';

INSERT INTO test_log SELECT number FROM numbers(1);
INSERT INTO test_tiny_log SELECT number FROM numbers(2);
INSERT INTO test_stripe_log SELECT number FROM numbers(3);
INSERT INTO test_set SELECT number FROM numbers(4);
INSERT INTO test_join SELECT number, number FROM numbers(5);

DROP TABLE test_defining SYNC;
SELECT 'disks after the defining table is dropped', count() FROM system.disks WHERE name = '$disk';

DETACH TABLE test_log SYNC;
DETACH TABLE test_tiny_log SYNC;
DETACH TABLE test_stripe_log SYNC;
DETACH TABLE test_set SYNC;
ATTACH TABLE test_log;
ATTACH TABLE test_tiny_log;
ATTACH TABLE test_stripe_log;
ATTACH TABLE test_set;
SELECT 'rows in Log', count() FROM test_log;
SELECT 'rows in TinyLog', count() FROM test_tiny_log;
SELECT 'rows in StripeLog', count() FROM test_stripe_log;
SELECT 'rows in Set', count() FROM numbers(10) WHERE number IN test_set;
SELECT 'rows in Join', count() FROM test_join;

DROP TABLE test_log SYNC;
DROP TABLE test_tiny_log SYNC;
DROP TABLE test_stripe_log SYNC;
DROP TABLE test_set SYNC;
SELECT 'disks before the last table is dropped', count() FROM system.disks WHERE name = '$disk';
DROP TABLE test_join SYNC;
SELECT 'disks after the last table is dropped', count() FROM system.disks WHERE name = '$disk';
"
