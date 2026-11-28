#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: needs Parquet and Delta Lake
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so `deltaLakeLocal` is absent.

# JOIN runtime filters prune the Parquet row groups of a Delta Lake table with column mapping only by a column
# stored under its own name, not by a column stored under the name of another column.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DELTA_DIR="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_05833"
rm -rf "${DELTA_DIR}"
mkdir -p "${DELTA_DIR}/_delta_log"
trap 'rm -rf "${DELTA_DIR}"' EXIT

# The data file stores `k` = number + 100000, `v` = number and `w` = number in 16 row groups of 100 rows: the
# table's `k_old`, `k` and `w` (column mapping enabled on an existing table, then `k` renamed to `k_old` and `v` to `k`).
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DELTA_DIR}/data.parquet', Parquet, 'k Int64, v Int64, w Int64')
    SELECT number + 100000, number, number FROM numbers(1600) SETTINGS output_format_parquet_row_group_size = 100;
    CREATE TABLE dim (k Int64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO dim VALUES (705), (710);" < /dev/null
DELTA_SCHEMA='{\"type\":\"struct\",\"fields\":[{\"name\":\"k_old\",\"type\":\"long\",\"nullable\":false,\"metadata\":{\"delta.columnMapping.id\":1,\"delta.columnMapping.physicalName\":\"k\"}},{\"name\":\"k\",\"type\":\"long\",\"nullable\":false,\"metadata\":{\"delta.columnMapping.id\":2,\"delta.columnMapping.physicalName\":\"v\"}},{\"name\":\"w\",\"type\":\"long\",\"nullable\":false,\"metadata\":{\"delta.columnMapping.id\":3,\"delta.columnMapping.physicalName\":\"w\"}}]}'
cat > "${DELTA_DIR}/_delta_log/00000000000000000000.json" <<LOG
{"protocol":{"minReaderVersion":2,"minWriterVersion":5}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}_05833","format":{"provider":"parquet","options":{}},"schemaString":"${DELTA_SCHEMA}","partitionColumns":[],"configuration":{"delta.columnMapping.mode":"name","delta.columnMapping.maxColumnId":"3"},"createdTime":1700000000000}}
{"add":{"path":"data.parquet","partitionValues":{},"size":1,"modificationTime":1700000000000,"dataChange":true}}
LOG
DELTA="deltaLakeLocal('${DELTA_DIR}')"

# Every setting that decides whether the probe side gets a runtime filter, which side is probed, or whether row
# groups are pruned by statistics is pinned.
SETTINGS="enable_join_runtime_filters = 1, join_algorithm = 'hash', enable_join_runtime_filters_index_analysis = 1,
    join_runtime_filter_min_probe_rows = 0, query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0,
    use_hash_table_stats_for_join_reordering = 0, enable_parallel_replicas = 0, input_format_parquet_filter_push_down = 1,
    use_query_condition_cache = 0, max_threads = 1"

# Runs the query with the pruning on and off: both must return the same rows.
probe() { # probe <tag> <query>
    ${CLICKHOUSE_CLIENT} --query "
        $2 SETTINGS ${SETTINGS}, use_join_runtime_filters_for_iceberg_pruning = 1, log_comment = '${CLICKHOUSE_DATABASE}_$1_on';
        $2 SETTINGS ${SETTINGS}, use_join_runtime_filters_for_iceberg_pruning = 0, log_comment = '${CLICKHOUSE_DATABASE}_$1_off';"
}

# The number of row groups the probe pruned by the runtime filter, with the pruning on and off.
report() { # report <tag>
    ${CLICKHOUSE_CLIENT} --query "
        SYSTEM FLUSH LOGS query_log;
        SELECT replaceOne(log_comment, '${CLICKHOUSE_DATABASE}_', ''), ProfileEvents['ParquetRuntimeFilterPrunedRowGroups']
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish'
            AND log_comment IN ('${CLICKHOUSE_DATABASE}_$1_on', '${CLICKHOUSE_DATABASE}_$1_off')
        ORDER BY log_comment DESC
        SETTINGS enable_parallel_replicas = 0"
}

echo '--- C a column stored under the name of another column ---'
probe c "SELECT count(), sum(k_old) FROM ${DELTA} AS f INNER JOIN dim AS d ON f.k = d.k"
report c

echo '--- W a column stored under its own name ---'
probe w "SELECT count(), sum(k) FROM ${DELTA} AS f INNER JOIN dim AS d ON f.w = d.k"
report w

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS dim SYNC"
