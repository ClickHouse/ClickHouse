#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# A non-UTF-8 partition value is rejected before commit: the Delta log is UTF-8 JSON, and an
# Iceberg manifest stores a `string` partition value as an Avro string, which is UTF-8 too.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_lake_non_utf8"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"

# Create an empty partitioned Delta table at $1 with the given JSON schema string ($2) and
# partitionColumns array ($3), using a minimal v0 transaction log (what delta-rs writes for an
# empty overwrite).
bootstrap() {
    local path="$1"
    local schema="$2"
    local partition_cols="$3"
    mkdir -p "${path}/_delta_log"
    cat > "${path}/_delta_log/00000000000000000000.json" <<EOF
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}-$(basename "${path}")","format":{"provider":"parquet","options":{}},"schemaString":"${schema}","partitionColumns":${partition_cols},"configuration":{},"createdTime":1700000000000}}
EOF
}

SCHEMA_STR='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}'
SCHEMA_BIN='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"p\",\"type\":\"binary\",\"nullable\":true,\"metadata\":{}}]}'

echo "-- string partition column: the INSERT with a non-UTF-8 value is rejected, the table stays readable"
bootstrap "${ROOT}/str" "${SCHEMA_STR}" '["p"]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/str') SELECT 1 AS id, 'ok' AS p"
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/str') SELECT number AS id, if(number = 3, unhex('FF'), 'fine') AS p FROM numbers(2, 2)" 2>&1 | grep -o -m1 'INCORRECT_DATA'
echo "log versions: $(ls "${ROOT}/str/_delta_log" | grep -c '\.json$')"
echo "data files: $(find "${ROOT}/str" -name '*.parquet' | wc -l)"
${CLICKHOUSE_LOCAL} --query "SELECT id, p FROM deltaLakeLocal('${ROOT}/str') ORDER BY id"

echo "-- binary partition column: a non-UTF-8 value is rejected too"
bootstrap "${ROOT}/bin" "${SCHEMA_BIN}" '["p"]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/bin') SELECT 1 AS id, unhex('FF') AS p" 2>&1 | grep -o -m1 'INCORRECT_DATA'
echo "log versions: $(ls "${ROOT}/bin/_delta_log" | grep -c '\.json$')"

echo "-- Iceberg: an INSERT or ALTER UPDATE that would write a non-UTF-8 partition value is rejected and leaves no files"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ice (id Int32, p String) ENGINE = IcebergLocal('${ROOT}/ice/') PARTITION BY p"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ice VALUES (1, 'ok')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ice VALUES (2, 'ok')"
find "${ROOT}/ice" -type f | sort > "${ROOT}/ice_files_before"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ice SELECT 3, unhex('FF')" 2>&1 | grep -o -m1 'INCORRECT_DATA'
# Only row 2 gets the bad value, so the UPDATE fails after it has written files for row 1.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ice UPDATE p = if(id = 2, unhex('FF'), 'new') WHERE 1" 2>&1 | grep -o -m1 'INCORRECT_DATA'
echo "new files: $(find "${ROOT}/ice" -type f | sort | comm -13 "${ROOT}/ice_files_before" - | wc -l)"
echo "metadata versions: $(ls "${ROOT}/ice/metadata" | grep -c '\.metadata\.json$')"
${CLICKHOUSE_CLIENT} --query "SELECT id, p FROM ice ORDER BY id"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ice"
