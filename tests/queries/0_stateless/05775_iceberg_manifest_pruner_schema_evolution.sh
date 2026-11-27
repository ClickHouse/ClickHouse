#!/usr/bin/env bash
# Tags: no-fasttest
# IcebergLocal requires Avro and Parquet support.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PINS="--query_plan_optimize_join_order_randomize=0 --query_plan_optimize_join_order_limit=10
    --query_plan_optimize_join_order_algorithm=greedy --query_plan_join_swap_table=auto
    --use_hash_table_stats_for_join_reordering=0 --collect_hash_table_stats_during_joins=0
    --enable_join_runtime_filters=0 --enable_parallel_replicas=0 --enable_join_transitive_predicates=0
    --query_plan_propagate_predicate_across_join=0 --use_statistics=1 --materialize_statistics_on_insert=1
    --explain_query_plan_default=legacy --max_insert_threads=1 --max_threads=1 --max_block_size=1000000
    --allow_insert_into_iceberg=1"

ON="--use_datalake_metadata_statistics=1"
PRUNING="--use_iceberg_partition_pruning=1 --use_iceberg_manifest_list_partition_pruning=0"

LAKE="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"
trap 'rm -rf "${LAKE}"' EXIT

# The two INSERTs create manifests written under different schemas.
# The partition column k remains unchanged. Column r is widened from Int32
# to Int64 and then renamed, preserving its Iceberg field ID across schemas.
${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE p (k Int32, v Int64, r Int32)
        ENGINE = IcebergLocal('${LAKE}/p')
        PARTITION BY (icebergBucket(4, k));

    INSERT INTO p SELECT number, number, number FROM numbers(1000);

    ALTER TABLE p MODIFY COLUMN r Int64;
    ALTER TABLE p RENAME COLUMN r TO renamed_r;

    INSERT INTO p SELECT number, number, number + 1000 FROM numbers(1000);

    CREATE TABLE mt (k Int32, x Int64) ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 8192, auto_statistics_types = 'uniq';

    INSERT INTO mt SELECT number, number FROM numbers(1000);
"

echo '--- data files across both schema versions'
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), sum(record_count)
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 'p' AND content = 'DATA'"

Q="SELECT count() FROM mt AS m JOIN p ON m.x = p.v
    WHERE p.k = 500 AND p.renamed_r = 500"

echo '--- logical-plan estimate; manifest-list pruning disabled'
${CLICKHOUSE_CLIENT} ${PINS} ${ON} ${PRUNING} --query "
    SELECT trimLeft(explain)
    FROM (EXPLAIN keep_logical_steps = 1, actions = 1 ${Q})
    WHERE explain LIKE '%Join: %' OR explain LIKE '%ResultRows: %'"

echo '--- actual rows'
${CLICKHOUSE_CLIENT} ${PINS} ${ON} ${PRUNING} --query "${Q}"
