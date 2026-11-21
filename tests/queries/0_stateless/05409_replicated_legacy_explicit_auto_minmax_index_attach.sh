#!/usr/bin/env bash
# Tags: zookeeper, no-random-merge-tree-settings, no-shared-merge-tree, no-replicated-database
# Tag no-random-merge-tree-settings: the test lists the skip indices of a table.
# Tag no-shared-merge-tree: the test covers the `ReplicatedMergeTree` metadata check against ZooKeeper.
# Tag no-replicated-database: the test re-attaches the table with a different definition.

# A table may declare an index of its own that happens to be named `auto_minmax_index_<column>`
# while the implicit-index policy is off. When the table does not store the policy in its own
# `SETTINGS` clause and the policy is on now, the local loader keeps the index explicit (see `05233`).
# The normalization of the ZooKeeper metadata must apply the same rule: stripping the index there made
# the strict comparison on attach fail with `METADATA_MISMATCH` and left the replica read-only.

# The full-definition `ATTACH` below logs a warning that it is not recommended.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=error

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A full-definition `ATTACH` into an `Atomic` database needs a UUID, and the detached table keeps its
# name and UUID, so the same replica is attached as a new table.
NEW_UUID=$($CLICKHOUSE_CLIENT --query "SELECT generateUUIDv4()")

$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_05409 (x UInt64, INDEX auto_minmax_index_x x TYPE minmax GRANULARITY 3)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_05409', 'r1') ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 0;

DETACH TABLE t_05409;

-- The same table, now inheriting the implicit-index policy instead of storing it.
ATTACH TABLE t_05409_attached UUID '$NEW_UUID' (x UInt64, INDEX auto_minmax_index_x x TYPE minmax GRANULARITY 3)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_05409', 'r1') ORDER BY tuple();

SELECT name, creation FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_05409_attached' ORDER BY name;

SELECT is_readonly FROM system.replicas WHERE database = currentDatabase() AND table = 't_05409_attached';

INSERT INTO t_05409_attached SELECT number FROM numbers(10);
SELECT count() FROM t_05409_attached WHERE x = 5;

DROP TABLE t_05409_attached SYNC;
"
