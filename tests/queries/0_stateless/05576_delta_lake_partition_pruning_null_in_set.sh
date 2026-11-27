#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.
# Partition pruning must keep the data file of the NULL partition when the filter's set holds NULL, and prune the other files as before.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

ROOT="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_delta_null_set"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"

# A hand-written Delta table (a partitioned INSERT of a NULL value is not supported on this branch): a v0 log with protocol + metaData.
bootstrap() {
    local table=$1 schema=$2 partition_columns=$3
    mkdir -p "${ROOT}/${table}/_delta_log"
    cat > "${ROOT}/${table}/_delta_log/00000000000000000000.json" <<EOF
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}-${table}","format":{"provider":"parquet","options":{}},"schemaString":"${schema}","partitionColumns":${partition_columns},"configuration":{},"createdTime":1700000000000}}
EOF
}

SCHEMA_P='{\"type\":\"struct\",\"fields\":[{\"name\":\"p\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"v\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}]}'
SCHEMA_AB='{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"b\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"v\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}]}'

bootstrap t "${SCHEMA_P}" '["p"]'
bootstrap n "${SCHEMA_P}" '["p"]'
bootstrap t2 "${SCHEMA_AB}" '["a","b"]'

QUERIES=""

# add_file <table> <partition directory> <partitionValues> <v>: one data file holding only `v`, committed in the v0 log.
add_file() {
    local table=$1 dir=$2 values=$3 v=$4
    echo "{\"add\":{\"path\":\"${dir}/data.parquet\",\"partitionValues\":${values},\"size\":1,\"modificationTime\":1700000000000,\"dataChange\":true}}" \
        >> "${ROOT}/${table}/_delta_log/00000000000000000000.json"
    QUERIES+="
    INSERT INTO FUNCTION file('${ROOT}/${table}/${dir}/data.parquet', Parquet, 'v Int32') VALUES (${v});"
}

# One data file per partition. Only t and t2 have a NULL partition (`__HIVE_DEFAULT_PARTITION__`, a JSON null).
add_file t 'p=5' '{"p":"5"}' 1
add_file t 'p=__HIVE_DEFAULT_PARTITION__' '{"p":null}' 2
add_file t 'p=7' '{"p":"7"}' 3
add_file t 'p=8' '{"p":"8"}' 4
add_file n 'p=5' '{"p":"5"}' 1
add_file n 'p=7' '{"p":"7"}' 3
add_file n 'p=8' '{"p":"8"}' 4
add_file t2 'a=__HIVE_DEFAULT_PARTITION__/b=3' '{"a":null,"b":"3"}' 1
add_file t2 'a=1/b=3' '{"a":"1","b":"3"}' 2
add_file t2 'a=1/b=5' '{"a":"1","b":"5"}' 3

LABELS=()

# arm <table> <condition> [settings]: prints the rows read with no filter at the source (LIMIT blocks the push-down),
# the rows read with partition pruning, and the number of data files pruned by the second query.
arm() {
    local table=$1 condition=$2 settings=${3:+ SETTINGS $3}
    LABELS+=("${table}: ${condition}${3:+, $3}")
    QUERIES+="
    SELECT arraySort(groupArray(v)) FROM (SELECT * FROM deltaLakeLocal('${ROOT}/${table}') LIMIT 1000) WHERE ${condition}${settings};
    SELECT sumIf(value, event = 'DeltaLakePartitionPrunedFiles') FROM system.events;
    SELECT arraySort(groupArray(v)) FROM deltaLakeLocal('${ROOT}/${table}') WHERE ${condition}${settings};
    SELECT sumIf(value, event = 'DeltaLakePartitionPrunedFiles') FROM system.events;"
}

arm t "has([NULL, 1], p)"
arm t "has([NULL, 5], p)"
arm t "NOT has([NULL, 1], p)"
arm t "has([NULL, 1]::Array(Nullable(Int64)), p)"
arm t "NOT has([NULL, 1]::Array(Nullable(Int64)), p)"
arm t "p IN (NULL, 1)" "transform_null_in = 1"
# The engine predicate would skip these files before the partition pruner counts them.
arm t "p IS NULL" "delta_lake_enable_engine_predicate = 0"
arm t "p = 7" "delta_lake_enable_engine_predicate = 0"
arm t "p < 6" "delta_lake_enable_engine_predicate = 0"
arm t "p IS NOT NULL" "delta_lake_enable_engine_predicate = 0"
arm t "has([5, 7], p)"
arm n "has([NULL, 5], p)"
arm n "NOT has([NULL, 5], p)"
arm t2 "has([NULL, 1], a)"

mapfile -t LINES < <(${CLICKHOUSE_LOCAL} --query "${QUERIES}" < /dev/null)
if [[ "${#LINES[@]}" != "$(( 4 * ${#LABELS[@]} ))" ]]; then
    printf 'Expected %s lines, got %s:\n%s\n' "$(( 4 * ${#LABELS[@]} ))" "${#LINES[@]}" "$(printf '%s\n' "${LINES[@]}")" >&2
    exit 1
fi

for i in "${!LABELS[@]}"; do
    printf '%s\t%s\t%s\t%s\n' "${LABELS[$i]}" "${LINES[4 * i]}" "${LINES[4 * i + 2]}" "$(( LINES[4 * i + 3] - LINES[4 * i + 1] ))"
done
