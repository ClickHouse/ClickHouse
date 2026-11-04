#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-merge-tree, zookeeper
# no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.
# no-shared-merge-tree: the table engine is replaced, and it keeps parts elsewhere.

# With `lazy_load_tables` the catalog holds a stand-in for a table until something uses it. Every operation
# below is run on a stand-in, and each of them reaches the real storage through a cast or a hook that the
# stand-in has to forward: an unforwarded one answers for the stand-in, which has no parts, no keys and no
# settings, and either refuses the operation or silently does nothing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_lazy"

function query()
{
    $CLICKHOUSE_CLIENT -q "USE \`$DB\`; $1"
}

# Re-reading the metadata is what turns the tables back into stand-ins.
function reload()
{
    # A background task, such as the end of a mutation, can hold a table for a moment longer, and the database
    # refuses to be detached until it lets go.
    local error
    for _ in {1..600}; do
        error=$($CLICKHOUSE_CLIENT -q "DETACH DATABASE \`$DB\`" 2>&1) && break
        if [[ $error != *DATABASE_NOT_EMPTY* ]]; then
            echo "$error" >&2
            break
        fi
        sleep 0.1
    done
    $CLICKHOUSE_CLIENT -q "ATTACH DATABASE \`$DB\`"
}

function loaded()
{
    query "SELECT is_loaded FROM system.tables WHERE database = currentDatabase() AND name = '$1'"
}

for engine in MergeTree ReplicatedMergeTree; do
    echo "--- $engine"
    if [ "$engine" = ReplicatedMergeTree ]; then
        engine_def="ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/{database}/{table}', 'r1')"
    else
        engine_def="MergeTree"
    fi

    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS \`$DB\` SYNC"
    $CLICKHOUSE_CLIENT -q "CREATE DATABASE \`$DB\` ENGINE = Atomic SETTINGS lazy_load_tables = 1"

    echo "- mutations"
    query "
        CREATE TABLE t (d Date, id UInt64, v String, n UInt32,
            INDEX idx_v v TYPE bloom_filter GRANULARITY 1,
            PROJECTION proj (SELECT n, count() GROUP BY n))
        ENGINE = $engine_def PARTITION BY toYYYYMM(d) ORDER BY id
        SETTINGS lightweight_mutation_projection_mode = 'rebuild';
        INSERT INTO t SELECT '2026-01-01', number, toString(number), number % 5 FROM numbers(10);
        INSERT INTO t SELECT '2026-02-01', number + 10, toString(number), number % 5 FROM numbers(10);"
    reload
    loaded t
    # `IN PARTITION` resolves the partition through a `MergeTreeData` cast.
    query "ALTER TABLE t DELETE IN PARTITION 202601 WHERE id = 1 SETTINGS mutations_sync = 2"
    reload
    query "ALTER TABLE t MATERIALIZE INDEX idx_v SETTINGS mutations_sync = 2"
    reload
    query "ALTER TABLE t MATERIALIZE PROJECTION proj SETTINGS mutations_sync = 2"
    reload
    # `supportsTTL` gates `ALTER MODIFY TTL`.
    query "ALTER TABLE t MODIFY TTL d + INTERVAL 100 YEAR"
    query "SELECT count(), countIf(id = 1) FROM t"
    query "SELECT 'has TTL', create_table_query LIKE '%TTL d + toIntervalYear(100)%' FROM system.tables WHERE database = currentDatabase() AND name = 't'"
    query "DROP TABLE t SYNC"

    echo "- create as"
    # `CREATE TABLE ... AS` copies indices, projections, constraints and the comment out of the metadata of
    # the source, which a stand-in reports as columns only.
    query "
        CREATE TABLE src (id UInt64, n UInt32, CONSTRAINT c_n CHECK n > 0,
            INDEX idx_n n TYPE minmax GRANULARITY 1,
            PROJECTION p (SELECT n, count() GROUP BY n))
        ENGINE = $engine_def ORDER BY id COMMENT 'source comment'"
    reload
    loaded src
    # An explicit engine, since the stored one of the source carries its own coordination path.
    query "CREATE TABLE copy AS src ENGINE = $engine_def ORDER BY id"
    query "SELECT
        (SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'copy'),
        (SELECT count() FROM system.projections WHERE database = currentDatabase() AND table = 'copy'),
        (SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = 'copy')"
    query "INSERT INTO copy VALUES (1, 0)" 2>&1 | grep -o -m1 VIOLATED_CONSTRAINT
    query "DROP TABLE src SYNC; DROP TABLE copy SYNC"

    echo "- system tables"
    query "
        CREATE TABLE t (id UInt64, v String, INDEX idx_v v TYPE bloom_filter GRANULARITY 1)
        ENGINE = $engine_def ORDER BY id;
        INSERT INTO t SELECT number, toString(number) FROM numbers(1000);"
    reload
    # The system tables that walk every table see the real storage without loading it, so they report
    # nothing until something else loads it.
    query "SELECT
        (SELECT count() FROM system.parts WHERE database = currentDatabase()),
        (SELECT count() FROM system.data_skipping_indices WHERE database = currentDatabase()),
        (SELECT sum(data_compressed_bytes) FROM system.columns WHERE database = currentDatabase())"
    loaded t
    query "SELECT count() FROM t"
    query "SELECT
        (SELECT count() FROM system.parts WHERE database = currentDatabase() AND active),
        (SELECT sum(rows) FROM system.parts WHERE database = currentDatabase() AND active),
        (SELECT total_rows FROM system.tables WHERE database = currentDatabase() AND name = 't'),
        (SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase()),
        (SELECT data_compressed_bytes > 0 AND marks_bytes > 0 FROM system.data_skipping_indices WHERE database = currentDatabase())"
    query "DROP TABLE t SYNC"

    echo "- show columns and indexes"
    # `SHOW COLUMNS` and `SHOW INDEXES` name a single table, so they load it instead of reporting the empty
    # keys and indices that the passive system tables have for a stand-in.
    query "
        CREATE TABLE t (d Date, id UInt64, v String, INDEX idx_v v TYPE bloom_filter GRANULARITY 1)
        ENGINE = $engine_def PARTITION BY toYYYYMM(d) ORDER BY id"
    reload
    query "SHOW COLUMNS FROM t" | grep -c 'PRI SOR'
    loaded t
    reload
    query "SHOW INDEXES FROM t" | cut -f3,5 | LC_ALL=C sort
    loaded t
    query "DROP TABLE t SYNC"

    echo "- system commands"
    query "
        CREATE TABLE t (id UInt64) ENGINE = $engine_def ORDER BY id SETTINGS merge_selector_algorithm = 'Manual';
        INSERT INTO t SELECT number FROM numbers(10);
        INSERT INTO t SELECT number + 10 FROM numbers(10);"
    parts=$(query "SELECT arrayStringConcat(groupArray(concat('''', name, '''')), ', ') FROM (SELECT name FROM system.parts WHERE database = currentDatabase() AND active ORDER BY name) FORMAT TSVRaw")
    reload
    loaded t
    query "OPTIMIZE TABLE t DRY RUN PARTS $parts"
    # Merging the parts last, since it invalidates their names.
    reload
    query "SYSTEM SCHEDULE MERGE t PARTS $parts"
    query "OPTIMIZE TABLE t FINAL"
    query "SELECT count(), sum(rows) FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active"
    query "DROP TABLE t SYNC"

    echo "- table functions"
    # The table functions that read parts directly cast the source table to `MergeTreeData`.
    query "
        CREATE TABLE t (id UInt64, n UInt32, PROJECTION p (SELECT n, count() GROUP BY n)) ENGINE = $engine_def ORDER BY id;
        INSERT INTO t VALUES (1, 7);"
    reload
    loaded t
    query "SELECT
        (SELECT count() > 0 FROM mergeTreeIndex(currentDatabase(), 't')),
        (SELECT count() > 0 FROM mergeTreeProjection(currentDatabase(), 't', 'p')),
        (SELECT count() > 0 FROM mergeTreeCodecBlockCounts(currentDatabase(), 't'))"
    query "DROP TABLE t SYNC"

    echo "- nested columns"
    # `share_nested_offsets` comes from the engine settings, and defaulting it to true would allow alters
    # of the whole `Nested` group that the setting rules out.
    query "
        CREATE TABLE t (id UInt64, nest Nested(a UInt8, b String)) ENGINE = $engine_def ORDER BY id
        SETTINGS share_nested_offsets = 0;
        INSERT INTO t VALUES (1, [1], ['x']);"
    reload
    loaded t
    query "ALTER TABLE t RENAME COLUMN nest TO other" 2>&1 | grep -o -m1 NOT_FOUND_COLUMN_IN_BLOCK
    reload
    query "ALTER TABLE t DROP COLUMN nest" 2>&1 | grep -o -m1 NOT_FOUND_COLUMN_IN_BLOCK
    query "DROP TABLE t SYNC"

    echo "- async insert"
    # The table-level `async_insert` setting is read through `areAsynchronousInsertsEnabled`.
    query "CREATE TABLE t (id UInt64) ENGINE = $engine_def ORDER BY id SETTINGS async_insert = 1"
    reload
    loaded t
    query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_async_insert_$engine"
    $CLICKHOUSE_CLIENT --query_id "$query_id" -q "USE \`$DB\`; INSERT INTO t SETTINGS async_insert = 0, wait_for_async_insert = 1 VALUES (1)"
    query "SYSTEM FLUSH LOGS asynchronous_insert_log"
    query "SELECT count() FROM system.asynchronous_insert_log WHERE query_id = '$query_id'"
    query "DROP TABLE t SYNC"

    echo "- planner"
    # The planner asks the storage whether it is a `MergeTree` and whether trivial count applies.
    query "
        CREATE TABLE t (id UInt64, v UInt64) ENGINE = $engine_def ORDER BY id;
        INSERT INTO t SELECT number, number * 2 FROM numbers(1000);"
    reload
    query "EXPLAIN SELECT count() FROM t SETTINGS optimize_trivial_count_query = 1" | grep -c 'Optimized trivial count'
    reload
    # `automatic_parallel_replicas_mode` would let the planner decide on its own and skip parallel replicas.
    query "EXPLAIN SELECT sum(v) FROM t SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
        cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
        parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0,
        parallel_replicas_local_plan = 0, automatic_parallel_replicas_mode = 0" | grep -c -m1 'ParallelReplicas'
    query "DROP TABLE t SYNC"

    echo "- populate"
    # `POPULATE` pins a snapshot of the source, and the snapshot of a stand-in has none of its data.
    query "
        CREATE TABLE src (id UInt64, n UInt32) ENGINE = $engine_def ORDER BY id;
        INSERT INTO src SELECT number, number % 5 FROM numbers(100);"
    reload
    loaded src
    query "CREATE MATERIALIZED VIEW mv ENGINE = $engine_def ORDER BY n POPULATE AS SELECT n, count() AS c FROM src GROUP BY n"
    query "SELECT sum(c) FROM mv"
    query "DROP TABLE mv SYNC; DROP TABLE src SYNC"

    echo "- backup"
    # `IStorage::backupData` does nothing, so a stand-in that does not forward it backs up nothing and the
    # incremental backup then diffs against an empty base.
    query "
        CREATE TABLE t (d Date, id UInt64) ENGINE = $engine_def PARTITION BY toYYYYMM(d) ORDER BY id;
        INSERT INTO t SELECT '2024-01-01', number FROM numbers(100);
        INSERT INTO t SELECT '2024-02-01', number FROM numbers(30);"
    reload
    loaded t
    base="${CLICKHOUSE_TEST_UNIQUE_NAME}_${engine}_base"
    incremental="${CLICKHOUSE_TEST_UNIQUE_NAME}_${engine}_incremental"
    partition="${CLICKHOUSE_TEST_UNIQUE_NAME}_${engine}_partition"
    whole_database="${CLICKHOUSE_TEST_UNIQUE_NAME}_${engine}_database"
    query "BACKUP TABLE t TO Disk('backups', '$base') FORMAT Null"
    query "INSERT INTO t SELECT '2024-01-01', number + 100 FROM numbers(50)"
    reload
    query "BACKUP TABLE t TO Disk('backups', '$incremental') SETTINGS base_backup = Disk('backups', '$base') FORMAT Null"
    reload
    # Partition-level backup also needs `supportsBackupPartition` from the real storage.
    query "BACKUP TABLE t PARTITIONS '202401' TO Disk('backups', '$partition') FORMAT Null"
    reload
    $CLICKHOUSE_CLIENT -q "BACKUP DATABASE \`$DB\` TO Disk('backups', '$whole_database') FORMAT Null"
    # Restoring under another name would reuse the coordination path of the replicated table.
    query "DROP TABLE t SYNC"
    query "RESTORE TABLE t FROM Disk('backups', '$incremental') FORMAT Null"
    query "SELECT 'incremental', count() FROM t"
    query "DROP TABLE t SYNC"
    query "RESTORE TABLE t FROM Disk('backups', '$partition') FORMAT Null"
    query "SELECT 'partition', count() FROM t"
    $CLICKHOUSE_CLIENT -q "DROP DATABASE \`$DB\` SYNC"
    $CLICKHOUSE_CLIENT -q "RESTORE DATABASE \`$DB\` FROM Disk('backups', '$whole_database') FORMAT Null"
    query "SELECT 'database', count() FROM t"

    $CLICKHOUSE_CLIENT -q "DROP DATABASE \`$DB\` SYNC"
done
