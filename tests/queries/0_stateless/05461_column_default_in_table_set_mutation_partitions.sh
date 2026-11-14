#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree
# no-replicated-database: the mutation must stay pending, but `SYSTEM STOP REPLICATION QUEUES` stops only the local replica
# no-shared-merge-tree: `SYSTEM STOP REPLICATION QUEUES` does not keep the mutations of a `SharedMergeTree` pending

# A mutation through `flag ALIAS p IN keys` on a table partitioned by `p` must not be pruned to the
# partitions that match the contents of `keys` at submission time: the set is built again when the
# mutation executes, and rows in a partition that matches only then must not escape the mutation.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --mutations_sync 0 --query "
DROP TABLE IF EXISTS t_mutation_partitions;
DROP TABLE IF EXISTS t_mutation_partitions_keys;
CREATE TABLE t_mutation_partitions_keys (p UInt8) ENGINE = MergeTree ORDER BY p;
CREATE TABLE t_mutation_partitions (p UInt8, x UInt64, flag UInt8 ALIAS p IN t_mutation_partitions_keys)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mutation_partitions', 'r1')
PARTITION BY p ORDER BY x;

INSERT INTO t_mutation_partitions_keys VALUES (1);
INSERT INTO t_mutation_partitions VALUES (1, 1), (2, 2), (3, 3);

SYSTEM STOP REPLICATION QUEUES t_mutation_partitions;
ALTER TABLE t_mutation_partitions DELETE WHERE flag;
SYSTEM SYNC REPLICA t_mutation_partitions PULL;

-- Every partition is in the scope of the mutation.
SELECT \`block_numbers.partition_id\` FROM system.mutations
WHERE database = currentDatabase() AND table = 't_mutation_partitions';

-- The right-hand side changes between the submission and the execution.
INSERT INTO t_mutation_partitions_keys VALUES (2);
SYSTEM START REPLICATION QUEUES t_mutation_partitions;
"

for _ in {1..600}
do
    done_count=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_mutation_partitions' AND is_done")
    [ "$done_count" = "1" ] && break
    sleep 0.1
done

${CLICKHOUSE_CLIENT} --query "
SELECT p, x FROM t_mutation_partitions ORDER BY p;
SELECT is_done, latest_fail_reason FROM system.mutations WHERE database = currentDatabase() AND table = 't_mutation_partitions';
DROP TABLE t_mutation_partitions;
DROP TABLE t_mutation_partitions_keys;
"
