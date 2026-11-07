#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-async-insert
# Tag no-replicated-database: Fails due to additional replicas or shards
# Tag no-shared-merge-tree: No quorum
# Tag no-async-insert: async inserts are not supported with non-parallel quorum inserts

# A dependent materialized view with `SQL SECURITY DEFINER` writes into its target with the context of
# its definer, whose settings may make the write a non-parallel quorum insert (insert_quorum >= 2,
# insert_quorum_parallel = 0) even if the INSERT query leaves both settings at their defaults. The
# serialization such a write needs is derived from the context of every branch: the INSERT SELECT
# fan-out is kept single-stream, and two such views converging on one replicated table are pushed
# sequentially with parallel_view_processing = 1.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pin max_threads and disable the memory-based thread clamping so that the number of
# parallel insert streams is deterministic regardless of the machine.
SETTINGS="--max_threads=8 --max_insert_threads=4 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0 --async_insert=0 --insert_keeper_fault_injection_probability=0"

user="user_05457_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_05457_mv_plain;
    DROP TABLE IF EXISTS t_05457_mv_1;
    DROP TABLE IF EXISTS t_05457_mv_2;
    DROP TABLE IF EXISTS t_05457_plain;
    DROP TABLE IF EXISTS t_05457_source;
    DROP TABLE IF EXISTS t_05457_target_1;
    DROP TABLE IF EXISTS t_05457_target_2;
    DROP USER IF EXISTS $user;

    CREATE USER $user IDENTIFIED WITH no_password SETTINGS insert_quorum = 2, insert_quorum_parallel = 0;

    CREATE TABLE t_05457_target_1 (x UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_05457/target', '1') ORDER BY x;
    CREATE TABLE t_05457_target_2 (x UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_05457/target', '2') ORDER BY x;
    CREATE TABLE t_05457_plain (x UInt32) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE t_05457_source (x UInt32) ENGINE = Null;

    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05457_plain TO $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05457_source TO $user;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.t_05457_target_1 TO $user;

    CREATE MATERIALIZED VIEW t_05457_mv_plain TO t_05457_target_1 DEFINER = $user SQL SECURITY DEFINER
        AS SELECT x FROM t_05457_plain;
"

# The view writes quorum parts through the context of its definer, so the fan-out of an INSERT SELECT
# into its source drops to a single stream: one sink for the source table plus one for the view target.
$CLICKHOUSE_CLIENT $SETTINGS --parallel_view_processing=1 -q \
    "EXPLAIN PIPELINE INSERT INTO t_05457_plain SELECT number FROM numbers(4)" | grep -c "MergeTreeSink"

# Two such views converging on one replicated target: the view branches are pushed sequentially, so
# each branch's quorum is satisfied before the next branch commits its part.
$CLICKHOUSE_CLIENT -q "
    CREATE MATERIALIZED VIEW t_05457_mv_1 TO t_05457_target_1 DEFINER = $user SQL SECURITY DEFINER
        AS SELECT x FROM t_05457_source;
    CREATE MATERIALIZED VIEW t_05457_mv_2 TO t_05457_target_1 DEFINER = $user SQL SECURITY DEFINER
        AS SELECT x + 100 AS x FROM t_05457_source;
"

for x in 1 2 3 4 5 6; do
    $CLICKHOUSE_CLIENT $SETTINGS --parallel_view_processing=1 -q "INSERT INTO t_05457_source VALUES ($x)"
done

$CLICKHOUSE_CLIENT --select_sequential_consistency=1 -q "SELECT count(), sum(x), min(x), max(x) FROM t_05457_target_1"
$CLICKHOUSE_CLIENT --select_sequential_consistency=1 -q "SELECT count(), sum(x), min(x), max(x) FROM t_05457_target_2"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE t_05457_mv_plain;
    DROP TABLE t_05457_mv_1;
    DROP TABLE t_05457_mv_2;
    DROP TABLE t_05457_plain;
    DROP TABLE t_05457_source;
    DROP TABLE t_05457_target_1;
    DROP TABLE t_05457_target_2;
    DROP USER $user;
"
