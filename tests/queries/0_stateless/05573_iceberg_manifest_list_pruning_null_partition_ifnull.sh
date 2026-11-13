#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
#
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/130040: manifest-list pruning skipped a
# manifest whose partition summary contains NULL when the filter maps NULL to a value inside the set or range,
# as `ifNull(b, 0) IN (0)` does, so the NULL-partition rows were silently dropped.
#
# Every INSERT writes one manifest. Each line prints the condition, the rows read without partition pruning
# (the truth), the rows read with both pruners on, and the number of manifests skipped by manifest-list pruning.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="t_${CLICKHOUSE_DATABASE}_${RANDOM}${RANDOM}"
T_INT="${PREFIX}_int"
T_STR="${PREFIX}_str"
T_DAY="${PREFIX}_day"
T_BUCKET="${PREFIX}_bucket"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS ${T_INT};
        DROP TABLE IF EXISTS ${T_STR};
        DROP TABLE IF EXISTS ${T_DAY};
        DROP TABLE IF EXISTS ${T_BUCKET};" >/dev/null 2>&1
    rm -rf "${USER_FILES_PATH:?}/${T_INT}/" "${USER_FILES_PATH:?}/${T_STR}/" "${USER_FILES_PATH:?}/${T_DAY}/" "${USER_FILES_PATH:?}/${T_BUCKET}/"
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${T_INT} (b Nullable(Int64), v Int32)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${T_INT}/', 'Parquet') PARTITION BY b;
    CREATE TABLE ${T_STR} (s Nullable(String), v Int32)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${T_STR}/', 'Parquet') PARTITION BY s;
    CREATE TABLE ${T_DAY} (ts Nullable(DateTime64(6, 'UTC')), v Int32)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${T_DAY}/', 'Parquet') PARTITION BY toRelativeDayNum(ts);
    CREATE TABLE ${T_BUCKET} (b Nullable(Int64), v Int32)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${T_BUCKET}/', 'Parquet') PARTITION BY icebergBucket(4, b);"

# One manifest per INSERT: the first t_int manifest holds 5 and NULL, the second 7 and 8.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_insert_single_manifest_for_all_partitions=1 --max_insert_threads=1 --query "
    INSERT INTO ${T_INT} VALUES (5, 1), (NULL, 2);
    INSERT INTO ${T_INT} VALUES (7, 3), (8, 4);
    INSERT INTO ${T_STR} VALUES ('x', 1), (NULL, 2);
    INSERT INTO ${T_DAY} VALUES ('2024-01-02 03:04:05', 1), (NULL, 2);
    INSERT INTO ${T_BUCKET} VALUES (5, 1), (NULL, 2);"

LINES=()
run_arm()
{
    local arm=$1 table=$2 condition=$3 extra=$4
    local truth pruned
    truth=$(${CLICKHOUSE_CLIENT} --query "
        SELECT arraySort(groupArray(v)) FROM ${table} WHERE ${condition}
        SETTINGS use_iceberg_partition_pruning = 0${extra}")
    pruned=$(${CLICKHOUSE_CLIENT} --query_id "${PREFIX}_${arm}" --query "
        SELECT arraySort(groupArray(v)) FROM ${table} WHERE ${condition}
        SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 1${extra}")
    LINES+=("${arm}"$'\t'"${table#"${PREFIX}_"} ${condition}${extra:+ (${extra#, })}"$'\t'"${truth}"$'\t'"${pruned}")
}

run_arm 1 "${T_INT}" "ifNull(b, 0) IN (0)"
run_arm 2 "${T_INT}" "coalesce(b, 0) IN (0, 1)"
run_arm 3 "${T_INT}" "assumeNotNull(b) IN (0)"
run_arm 4 "${T_INT}" "ifNull(b, 0) = 0" ", allow_key_condition_coalesce_rewrite = 0"
run_arm 5 "${T_INT}" "NOT (ifNull(b, 0) >= 5)"
# NULL-rejecting conditions keep pruning on the lower bound of the NULL-bearing manifest.
run_arm 6 "${T_INT}" "b IS NULL"
run_arm 7 "${T_INT}" "b = 1"
run_arm 8 "${T_INT}" "b < 6"
run_arm 9 "${T_STR}" "ifNull(s, '') IN ('')"
run_arm 10 "${T_DAY}" "ts < '2000-01-01 00:00:00'"
run_arm 11 "${T_DAY}" "ts IS NULL"
run_arm 12 "${T_BUCKET}" "b = 5"
run_arm 13 "${T_BUCKET}" "b IS NULL"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

declare -A PRUNED_MANIFESTS
while IFS=$'\t' read -r arm manifests; do
    PRUNED_MANIFESTS[${arm}]=${manifests}
done < <(${CLICKHOUSE_CLIENT} --query "
    SELECT substring(query_id, length('${PREFIX}_') + 1), ProfileEvents['IcebergPartitionPrunedManifestFiles']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND startsWith(query_id, '${PREFIX}_')
    SETTINGS enable_parallel_replicas = 0")

for line in "${LINES[@]}"; do
    arm=${line%%$'\t'*}
    echo "${line#*$'\t'}"$'\t'"${PRUNED_MANIFESTS[${arm}]:-missing}"
done
