#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-async-insert
# Tag no-replicated-database: Fails due to additional replicas or shards
# Tag no-shared-merge-tree: No quorum
# Tag no-async-insert: async inserts are not supported with non-parallel quorum inserts

# The non-parallel quorum write may be hidden behind several forwarding steps: an `Alias` over a
# materialized view that is inserted into directly, whose target is a `Null` table with a dependent
# `SQL SECURITY DEFINER` view writing into a replicated table with the quorum settings of its definer.
# The nested INSERT into the first view expands the dependent views of its target, so the fan-out of
# the INSERT into the `Alias` is kept single-stream even though the INSERT itself leaves the quorum
# settings at their defaults.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Pin max_threads and disable the memory-based thread clamping so that the number of
# parallel insert streams is deterministic regardless of the machine.
SETTINGS="--max_threads=8 --max_insert_threads=4 --max_threads_min_free_memory_per_thread=0 --max_insert_threads_min_free_memory_per_thread=0 --async_insert=0 --insert_keeper_fault_injection_probability=0"

user="user_05461_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_05461_alias;
    DROP TABLE IF EXISTS t_05461_mv_definer;
    DROP TABLE IF EXISTS t_05461_mv_outer;
    DROP TABLE IF EXISTS t_05461_source;
    DROP TABLE IF EXISTS t_05461_middle;
    DROP TABLE IF EXISTS t_05461_target;
    DROP USER IF EXISTS $user;

    CREATE USER $user IDENTIFIED WITH no_password SETTINGS insert_quorum = 2, insert_quorum_parallel = 0;

    CREATE TABLE t_05461_target (x UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_05461/target', '1') ORDER BY x;
    CREATE TABLE t_05461_middle (x UInt32) ENGINE = Null;
    CREATE TABLE t_05461_source (x UInt32) ENGINE = Null;

    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05461_middle TO $user;
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.t_05461_target TO $user;

    CREATE MATERIALIZED VIEW t_05461_mv_outer TO t_05461_middle AS SELECT x FROM t_05461_source;
    CREATE MATERIALIZED VIEW t_05461_mv_definer TO t_05461_target DEFINER = $user SQL SECURITY DEFINER
        AS SELECT x FROM t_05461_middle;

    CREATE TABLE t_05461_alias ENGINE = Alias('t_05461_mv_outer');
"

$CLICKHOUSE_CLIENT $SETTINGS --parallel_view_processing=1 -q \
    "EXPLAIN PIPELINE INSERT INTO t_05461_alias SELECT number FROM numbers(4)" | grep -c "AliasSink"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE t_05461_alias;
    DROP TABLE t_05461_mv_definer;
    DROP TABLE t_05461_mv_outer;
    DROP TABLE t_05461_source;
    DROP TABLE t_05461_middle;
    DROP TABLE t_05461_target;
    DROP USER $user;
"
