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

# An empty Delta table: a v0 log with only protocol + metaData, as ClickHouse cannot create a Delta log itself.
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
SCHEMA_S='{\"type\":\"struct\",\"fields\":[{\"name\":\"s\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"v\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}]}'

bootstrap t "${SCHEMA_P}" '["p"]'
bootstrap n "${SCHEMA_P}" '["p"]'
bootstrap t2 "${SCHEMA_AB}" '["a","b"]'
bootstrap s "${SCHEMA_S}" '["s"]'

# One data file per partition. Only t, t2 and s have a NULL partition (`__HIVE_DEFAULT_PARTITION__`).
QUERIES="
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/t') VALUES (5, 1), (NULL, 2), (7, 3), (8, 4);
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/n') VALUES (5, 1), (7, 3), (8, 4);
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/t2') VALUES (NULL, 3, 1), (1, 3, 2), (1, 5, 3);
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/s') VALUES ('a', 1), (NULL, 2), ('b', 3);"

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
arm t "ifNull(p, 0) IN (0)"
arm n "has([NULL, 5], p)"
arm n "NOT has([NULL, 5], p)"
arm t2 "has([NULL, 1], a)"
arm s "has([NULL, 'a'], s)"

mapfile -t LINES < <(${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "${QUERIES}" < /dev/null)
if [[ "${#LINES[@]}" != "$(( 4 * ${#LABELS[@]} ))" ]]; then
    printf 'Expected %s lines, got %s:\n%s\n' "$(( 4 * ${#LABELS[@]} ))" "${#LINES[@]}" "$(printf '%s\n' "${LINES[@]}")" >&2
    exit 1
fi

for i in "${!LABELS[@]}"; do
    printf '%s\t%s\t%s\t%s\n' "${LABELS[$i]}" "${LINES[4 * i]}" "${LINES[4 * i + 2]}" "$(( LINES[4 * i + 3] - LINES[4 * i + 1] ))"
done
