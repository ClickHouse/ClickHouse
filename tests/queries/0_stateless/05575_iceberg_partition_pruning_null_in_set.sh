#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
#
# Data-file partition pruning must keep the data file of the NULL partition when the filter's set holds NULL,
# as in `has([NULL, 1], b)`, and must keep pruning every other file as before.
# Every query switches the manifest-list pruner off, to isolate the data-file pruner.
#
# Table t has one data file per partition 5, NULL, 7, 8; table n has 5, 7, 8 and no NULL partition.
# A row arm prints the condition, the rows read without and with partition pruning, and the number of data
# files pruned. A count arm prints only the condition and the number of data files pruned: the rows of these
# filters also depend on the Parquet reader.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="t_${CLICKHOUSE_DATABASE}_${RANDOM}${RANDOM}"
T="${PREFIX}_t"
N="${PREFIX}_n"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${T}; DROP TABLE IF EXISTS ${N}" >/dev/null 2>&1
    rm -rf "${USER_FILES_PATH:?}/${T}/" "${USER_FILES_PATH:?}/${N}/"
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${T} (b Nullable(Int64), v Int32)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${T}/', 'Parquet') PARTITION BY b;
    CREATE TABLE ${N} (b Nullable(Int64), v Int32)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${N}/', 'Parquet') PARTITION BY b;"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --max_insert_threads=1 --query "
    INSERT INTO ${T} VALUES (5, 1), (NULL, 2);
    INSERT INTO ${T} VALUES (7, 3), (8, 4);
    INSERT INTO ${N} VALUES (5, 1);
    INSERT INTO ${N} VALUES (7, 3), (8, 4);"

LINES=()
header()
{
    LINES+=("-"$'\t'"$1")
}

run_arm()
{
    local arm=$1 table=$2 condition=$3
    local truth pruned
    truth=$(${CLICKHOUSE_CLIENT} --query "
        SELECT arraySort(groupArray(v)) FROM ${table} WHERE ${condition}
        SETTINGS use_iceberg_partition_pruning = 0, use_iceberg_manifest_list_partition_pruning = 0")
    pruned=$(${CLICKHOUSE_CLIENT} --query_id "${PREFIX}_${arm}" --query "
        SELECT arraySort(groupArray(v)) FROM ${table} WHERE ${condition}
        SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 0")
    LINES+=("${arm}"$'\t'"${condition}"$'\t'"${truth}"$'\t'"${pruned}")
}

count_arm()
{
    local arm=$1 table=$2 condition=$3 settings=$4
    ${CLICKHOUSE_CLIENT} --query_id "${PREFIX}_${arm}" --query "
        SELECT v FROM ${table} WHERE ${condition}
        SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 0${settings}
        FORMAT Null"
    LINES+=("${arm}"$'\t'"${condition}${settings}")
}

header "t: partitions 5, NULL, 7, 8"
run_arm a1 "${T}" "has([NULL, 1], b)"
run_arm a2 "${T}" "has([NULL, 5], b)"
run_arm a3 "${T}" "NOT has([NULL, 1], b)"
# NULL-rejecting conditions keep pruning the NULL-partition file.
run_arm s1 "${T}" "b IS NULL"
run_arm s2 "${T}" "b = 7"
run_arm s3 "${T}" "b < 6"
run_arm s4 "${T}" "b IS NOT NULL"
count_arm c1 "${T}" "b IN (NULL, 1)" ", transform_null_in = 1"
count_arm t1 "${T}" "has([NULL, 1]::Array(Nullable(Int64)), b)" ""
count_arm t2 "${T}" "NOT has([NULL, 1]::Array(Nullable(Int64)), b)" ""

header "n: partitions 5, 7, 8"
run_arm n1 "${N}" "has([NULL, 5], b)"
run_arm n2 "${N}" "NOT has([NULL, 5], b)"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

declare -A PRUNED_FILES
while IFS=$'\t' read -r arm files; do
    PRUNED_FILES[${arm}]=${files}
done < <(${CLICKHOUSE_CLIENT} --query "
    SELECT substring(query_id, length('${PREFIX}_') + 1), ProfileEvents['IcebergPartitionPrunedFiles']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND startsWith(query_id, '${PREFIX}_')
    SETTINGS enable_parallel_replicas = 0")

for line in "${LINES[@]}"; do
    arm=${line%%$'\t'*}
    if [ "${arm}" = "-" ]; then
        echo "${line#*$'\t'}"
    else
        echo "${line#*$'\t'}"$'\t'"${PRUNED_FILES[${arm}]:-missing}"
    fi
done
