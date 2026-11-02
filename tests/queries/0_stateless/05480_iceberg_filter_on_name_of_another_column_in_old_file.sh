#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option).
# - no-parallel-replicas: see 04611_iceberg_parquet_schema_evolution_prewhere_rename.sh.
#
# A filter on a column whose name another column had when an older data file was written
# must prune that file's row groups / stripes by the column it means now.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ICEBERG_ROOT="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_name_moved"
ROT_PATH="${ICEBERG_ROOT}/rot"
ROT_ORC_PATH="${ICEBERG_ROOT}/rot_orc"
REUSED_PATH="${ICEBERG_ROOT}/reused"
TUPLE_PATH="${ICEBERG_ROOT}/tuple"

READ_SETTINGS="input_format_parquet_filter_push_down = 1, input_format_parquet_bloom_filter_push_down = 1, input_format_parquet_page_filter_push_down = 1, input_format_orc_filter_push_down = 1, use_query_condition_cache = 0"
CREATE_SETTINGS="--input_format_parquet_filter_push_down=1 --input_format_parquet_bloom_filter_push_down=1 --input_format_parquet_page_filter_push_down=1 --input_format_orc_filter_push_down=1"

rm -rf "${ICEBERG_ROOT}"

# Rotate the names: a -> tmp, b -> a, c -> b, tmp -> c. The data file keeps (a, b, c) = (1, 2, 'x').
for format in Parquet ORC; do
    if [ "$format" = "Parquet" ]; then path="${ROT_PATH}"; table="t_rot"; else path="${ROT_ORC_PATH}"; table="t_rot_orc"; fi
    ${CLICKHOUSE_CLIENT} ${CREATE_SETTINGS} --allow_experimental_insert_into_iceberg=1 --allow_insert_into_iceberg=1 --query "
        CREATE TABLE ${table} (a Nullable(Int32), b Nullable(Int32), c Nullable(String)) ENGINE = IcebergLocal('${path}', '${format}');
        INSERT INTO ${table} VALUES (1, 2, 'x'), (NULL, 3, 'y');
        ALTER TABLE ${table} RENAME COLUMN a TO tmp;
        ALTER TABLE ${table} RENAME COLUMN b TO a;
        ALTER TABLE ${table} RENAME COLUMN c TO b;
        ALTER TABLE ${table} RENAME COLUMN tmp TO c;
    "
done

echo "-- Parquet, rotated names"
for condition in "c = 1" "a = 2" "b = 'x'" "c IS NULL" "NOT (c != 1)" "indexHint(c = 1)" "indexHint(indexHint(c = 1))"; do
    echo "${condition}"
    ${CLICKHOUSE_CLIENT} --query "SELECT c, a, b FROM icebergLocal('${ROT_PATH}') WHERE ${condition} ORDER BY a SETTINGS ${READ_SETTINGS}"
done
echo "engine table: c = 1"
${CLICKHOUSE_CLIENT} --query "SELECT c, a, b FROM t_rot WHERE c = 1 ORDER BY a SETTINGS use_query_condition_cache = 0"

echo "-- ORC, rotated names"
for condition in "c = 1" "b = 'x'"; do
    echo "${condition}"
    ${CLICKHOUSE_CLIENT} --query "SELECT c, a, b FROM icebergLocal('${ROT_ORC_PATH}') WHERE ${condition} ORDER BY a SETTINGS ${READ_SETTINGS}"
done

echo "-- Parquet, a column renamed onto a dropped name, and its old name added again"
${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg=1 --allow_insert_into_iceberg=1 --query "
    CREATE TABLE t_reused (id Int64, a Nullable(Int32), b Nullable(Int32)) ENGINE = IcebergLocal('${REUSED_PATH}', 'Parquet');
    INSERT INTO t_reused VALUES (0, 1, 2);
    ALTER TABLE t_reused DROP COLUMN b;
    ALTER TABLE t_reused RENAME COLUMN a TO b;
    ALTER TABLE t_reused ADD COLUMN a Nullable(Int32);
"
for condition in "b = 1" "a IS NULL" "a = 1"; do
    echo "${condition}"
    ${CLICKHOUSE_CLIENT} --query "SELECT id, b, a FROM icebergLocal('${REUSED_PATH}') WHERE ${condition} ORDER BY id SETTINGS ${READ_SETTINGS}"
done

echo "-- Row groups are still pruned"
${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg=1 --allow_insert_into_iceberg=1 --query "
    CREATE TABLE t_tuple (id Int32, t Tuple(x Int32, y Int32)) ENGINE = IcebergLocal('${TUPLE_PATH}', 'Parquet');
    INSERT INTO t_tuple VALUES (1, (10, 20));
    ALTER TABLE t_tuple ADD COLUMN z Nullable(Int32);
"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM icebergLocal('${ROT_PATH}') WHERE c = 999 SETTINGS ${READ_SETTINGS}, use_iceberg_partition_pruning = 0, log_comment = '${CLICKHOUSE_DATABASE}_prune_rot'"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM icebergLocal('${ROT_PATH}') WHERE a = 999 AND indexHint(c = 1) SETTINGS ${READ_SETTINGS}, use_iceberg_partition_pruning = 0, log_comment = '${CLICKHOUSE_DATABASE}_prune_hint'"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM icebergLocal('${TUPLE_PATH}') WHERE id = 999 AND t.x = 999 SETTINGS ${READ_SETTINGS}, use_iceberg_partition_pruning = 0, log_comment = '${CLICKHOUSE_DATABASE}_prune_tuple'"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
for suffix in rot hint tuple; do
    ${CLICKHOUSE_CLIENT} --query "
        SELECT '${suffix}', ProfileEvents['ParquetPrunedRowGroups'] > 0 FROM system.query_log
        WHERE current_database = currentDatabase() AND log_comment = '${CLICKHOUSE_DATABASE}_prune_${suffix}' AND type = 'QueryFinish'
    "
done

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_rot; DROP TABLE IF EXISTS t_rot_orc; DROP TABLE IF EXISTS t_reused; DROP TABLE IF EXISTS t_tuple"
rm -rf "${ICEBERG_ROOT}"
