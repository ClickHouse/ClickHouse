#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet and Iceberg writes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PARQUET_FILE="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_DATABASE}_variant_edge.parquet"
TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_variant_edge"
REQUIRED_TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_variant_edge_required"
V2_TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_variant_edge_v2"
rm -rf "${PARQUET_FILE}" "${TABLE_PATH}" "${REQUIRED_TABLE_PATH}" "${V2_TABLE_PATH}"

write_and_read()
{
    ${CLICKHOUSE_CLIENT} "${@:3}" --query "
        INSERT INTO FUNCTION file('${PARQUET_FILE}', Parquet, 'n UInt32, v Dynamic') $1
        SETTINGS engine_file_truncate_on_insert = 1;
        SELECT n, dynamicType(v), $2 FROM file('${PARQUET_FILE}', Parquet) ORDER BY n;
    "
}

echo "--- Timestamps are instants"
write_and_read "
    SELECT 1, ('2024-01-02 12:00:00'::DateTime)::Dynamic
    UNION ALL SELECT 2, ('2024-01-02 12:00:00.123456'::DateTime64(6))::Dynamic
    UNION ALL SELECT 3, ('2024-01-02 12:00:00.123456789'::DateTime64(9))::Dynamic
" "toUnixTimestamp64Nano(v::DateTime64(9, 'UTC'))" --session_timezone=Europe/Berlin

echo "--- Nanosecond range"
write_and_read "
    SELECT 1, ('2262-04-11 00:00:00'::DateTime64(7, 'UTC'))::Dynamic
    UNION ALL SELECT 2, ('2262-04-11 00:00:00'::DateTime64(8, 'UTC'))::Dynamic
" "v"
for scale in 7 8; do
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION file('${PARQUET_FILE}', Parquet, 'v Dynamic') SELECT ('2299-01-01 00:00:00'::DateTime64(${scale}, 'UTC'))::Dynamic
        SETTINGS engine_file_truncate_on_insert = 1
    " 2>&1 | grep -o -m1 "it is out of the range of a variant timestamp with 9 digits of fractional seconds"
done

echo "--- Bytes that are not UTF-8"
write_and_read "
    SELECT 1, (unhex('FF'))::Dynamic
    UNION ALL SELECT 2, (concat(repeat('x', 100), unhex('FF')))::Dynamic
    UNION ALL SELECT 3, ('текст')::Dynamic
" "hex(v::String)"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${PARQUET_FILE}', Parquet, 'v Dynamic') SELECT (map(unhex('FF'), 1))::Dynamic
    SETTINGS engine_file_truncate_on_insert = 1
" 2>&1 | grep -o -m1 "the keys of a variant object must be valid UTF-8"

echo "--- Escaped dots in JSON keys"
write_and_read "
    SELECT 1, ('{\"a.b\": 1, \"c\": {\"d\": 2}}'::JSON)::Dynamic
" "v" --json_type_escape_dots_in_keys=1

echo "--- Deep JSON path"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${PARQUET_FILE}', Parquet, 'v Dynamic')
    SELECT (concat('{\"', repeat('a.', 100000), 'x\": 1}')::JSON)::Dynamic
    SETTINGS engine_file_truncate_on_insert = 1
" 2>&1 | grep -o -m1 "TOO_DEEP_RECURSION"

echo "--- Variants need format version 3"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    CREATE TABLE t_variant_edge_v2 (id Int32, m Map(String, Dynamic)) ENGINE = IcebergLocal('${V2_TABLE_PATH}')
" 2>&1 | grep -o -m1 "the Iceberg type \`variant\` requires format version 3"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    CREATE TABLE t_variant_edge_v2 (id Int32) ENGINE = IcebergLocal('${V2_TABLE_PATH}');
    ALTER TABLE t_variant_edge_v2 ADD COLUMN d Dynamic;
" 2>&1 | grep -o -m1 "the Iceberg type \`variant\` requires format version 3"

echo "--- Adding a variant column"
${CLICKHOUSE_CLIENT} --query "
    SET allow_insert_into_iceberg = 1;
    CREATE TABLE t_variant_edge (id Int32) ENGINE = IcebergLocal('${TABLE_PATH}') SETTINGS iceberg_format_version = 3;
    INSERT INTO t_variant_edge VALUES (1);
    ALTER TABLE t_variant_edge ADD COLUMN d Dynamic;
    INSERT INTO t_variant_edge VALUES (2, 'x');
    SELECT id, dynamicType(d), d FROM t_variant_edge ORDER BY id;
    SELECT id, dynamicType(d), d FROM icebergLocal('${TABLE_PATH}') ORDER BY id;
"

echo "--- Null counts of a required variant"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    CREATE TABLE t_variant_edge_required (id Int32, d Dynamic)
        ENGINE = IcebergLocal('${REQUIRED_TABLE_PATH}') SETTINGS iceberg_format_version = 3;
"
python3 - "${REQUIRED_TABLE_PATH}/metadata" <<'PY'
import glob, json, os, sys
[path] = glob.glob(os.path.join(sys.argv[1], "v*.metadata.json"))
with open(path) as f:
    metadata = json.load(f)
for field in metadata["schemas"][-1]["fields"]:
    if field["name"] == "d":
        field["required"] = True
with open(path, "w") as f:
    json.dump(metadata, f)
PY
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION icebergLocal('${REQUIRED_TABLE_PATH}') VALUES (1, NULL), (2, 3)
    SETTINGS allow_insert_into_iceberg = 1, use_iceberg_metadata_files_cache = 0;
    SELECT id, dynamicType(d), d FROM icebergLocal('${REQUIRED_TABLE_PATH}') ORDER BY id
    SETTINGS use_iceberg_metadata_files_cache = 0;
"
for manifest in "${REQUIRED_TABLE_PATH}"/metadata/*.avro; do
    case "$(basename "${manifest}")" in snap-*) continue ;; esac
    ${CLICKHOUSE_LOCAL} --query "
        SELECT data_file.record_count, arraySort(data_file.null_value_counts) FROM file('${manifest}', Avro)
    "
done

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_variant_edge;
    DROP TABLE IF EXISTS t_variant_edge_v2;
    DROP TABLE IF EXISTS t_variant_edge_required;
"
rm -rf "${PARQUET_FILE}" "${TABLE_PATH}" "${REQUIRED_TABLE_PATH}" "${V2_TABLE_PATH}"
