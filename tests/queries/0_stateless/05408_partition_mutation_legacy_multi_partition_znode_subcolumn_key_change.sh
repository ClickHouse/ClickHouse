#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-shared-catalog
#
# `no-replicated-database` / `no-shared-merge-tree` / `no-shared-catalog`: the fixture edits the
# raw mutation znode of a `ReplicatedMergeTree` table at a known ZooKeeper path.
#
# A variant of `05220_partition_mutation_legacy_multi_partition_znode_key_change` with a partition key
# which reads a subcolumn (`PARTITION BY p.e`): a type change of the subcolumn through its storage column
# (`MODIFY COLUMN p Tuple(e Int8)`) makes the legacy `IN PARTITION <value>` literals undecodable too, so
# it is refused while the legacy multi-partition entry is pending.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_legacy_multi_partition_znode_subcolumn"
ZK_PATH="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/$TABLE"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS $TABLE SYNC"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE $TABLE (p Tuple(e Enum8('a' = 1, 'b' = 2, 'c' = 3)), n Int64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/$TABLE', '1')
    PARTITION BY p.e ORDER BY tuple()"

${CLICKHOUSE_CLIENT} --query "INSERT INTO $TABLE VALUES (tuple('a'), 1), (tuple('b'), 2), (tuple('c'), 3)"

# The mutation fails on every part it applies to, so it stays pending without relying on
# `SYSTEM STOP MERGES` (which does not survive the DETACH / ATTACH below).
${CLICKHOUSE_CLIENT} --query "
    ALTER TABLE $TABLE UPDATE n = throwIf(n > 0, 'keep the mutation pending') IN PARTITION 'a', 'b' WHERE 1"

# Fabricate the legacy format, see `05220_partition_mutation_legacy_multi_partition_znode_key_change`.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO system.zookeeper (name, path, value)
    SELECT name, path, replace(value,
        concat('IN PARTITION ID ', char(92, 39), '1', char(92, 39), ', ID ', char(92, 39), '2', char(92, 39)),
        concat('IN PARTITION ', char(92, 39), 'a', char(92, 39), ', ', char(92, 39), 'b', char(92, 39)))
    FROM system.zookeeper
    WHERE path = '$ZK_PATH/mutations'"

${CLICKHOUSE_CLIENT} --query "
    SELECT 'legacy znodes:', count() FROM system.zookeeper
    WHERE path = '$ZK_PATH/mutations'
      AND value LIKE concat('%IN PARTITION ', char(92, 39), 'a', char(92, 39), ', ', char(92, 39), 'b', char(92, 39), '%')"

# Simulate a restart so that the legacy entry is read back from ZooKeeper.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE $TABLE"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE $TABLE"

${CLICKHOUSE_CLIENT} --query "
    SELECT 'pending legacy mutations:', count() FROM system.mutations
    WHERE database = currentDatabase() AND table = '$TABLE' AND NOT is_done"

# The type change of the partition key subcolumn is refused while the legacy multi-partition entry is pending.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $TABLE MODIFY COLUMN p Tuple(e Int8) SETTINGS alter_sync = 2" 2>&1 \
    | grep -o "ALTER_OF_COLUMN_IS_FORBIDDEN" | head -n 1

${CLICKHOUSE_CLIENT} --query "KILL MUTATION WHERE database = currentDatabase() AND table = '$TABLE' SYNC FORMAT Null"

# Without the legacy entry the same change is accepted.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $TABLE MODIFY COLUMN p Tuple(e Int8) SETTINGS alter_sync = 2"
${CLICKHOUSE_CLIENT} --query "SELECT toTypeName(p.e) FROM $TABLE LIMIT 1"
${CLICKHOUSE_CLIENT} --query "SELECT p, n FROM $TABLE ORDER BY p, n"

${CLICKHOUSE_CLIENT} --query "DROP TABLE $TABLE SYNC"
