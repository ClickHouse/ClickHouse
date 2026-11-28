#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet and Iceberg writes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PARQUET_FILE="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_DATABASE}_variant_write.parquet"
TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_variant_write"
rm -rf "${PARQUET_FILE}" "${TABLE_PATH}"

echo "--- Parquet round trip"
${CLICKHOUSE_CLIENT} --query "
    SET session_timezone = 'UTC', allow_suspicious_types_in_order_by = 1;
    INSERT INTO FUNCTION file('${PARQUET_FILE}', Parquet, 'n UInt32, v Dynamic')
    SELECT 0, (NULL)::Dynamic
    UNION ALL SELECT 1, (true)::Dynamic
    UNION ALL SELECT 2, (-5::Int8)::Dynamic
    UNION ALL SELECT 3, (300::Int16)::Dynamic
    UNION ALL SELECT 4, (70000::Int32)::Dynamic
    UNION ALL SELECT 5, (-9000000000::Int64)::Dynamic
    UNION ALL SELECT 6, (200::UInt8)::Dynamic
    UNION ALL SELECT 7, (4000000000::UInt32)::Dynamic
    UNION ALL SELECT 8, (1.5::Float32)::Dynamic
    UNION ALL SELECT 9, (2.25::Float64)::Dynamic
    UNION ALL SELECT 10, (12.345::Decimal(9, 3))::Dynamic
    UNION ALL SELECT 11, (-123456789.123::Decimal(18, 3))::Dynamic
    UNION ALL SELECT 12, (1234567890123456789.12::Decimal(38, 2))::Dynamic
    UNION ALL SELECT 13, ('2024-02-29'::Date)::Dynamic
    UNION ALL SELECT 14, ('1900-01-01'::Date32)::Dynamic
    UNION ALL SELECT 15, ('2024-01-02 03:04:05.123456'::DateTime64(6, 'UTC'))::Dynamic
    UNION ALL SELECT 16, ('2024-01-02 03:04:05.123456789'::DateTime64(9))::Dynamic
    UNION ALL SELECT 17, ('2024-01-02 03:04:05'::DateTime('UTC'))::Dynamic
    UNION ALL SELECT 18, ('12:34:56.789'::Time64(3))::Dynamic
    UNION ALL SELECT 19, ('short')::Dynamic
    UNION ALL SELECT 20, (repeat('x', 100))::Dynamic
    UNION ALL SELECT 21, (toFixedString('ab', 3))::Dynamic
    UNION ALL SELECT 22, ('00112233-4455-6677-8899-aabbccddeeff'::UUID)::Dynamic
    UNION ALL SELECT 23, ([1, 2, 3]::Array(Int64))::Dynamic
    UNION ALL SELECT 24, (tuple(1::Int64, 'b')::Tuple(x Int64, a String))::Dynamic
    UNION ALL SELECT 25, (tuple(1::Int64, 'b')::Tuple(Int64, String))::Dynamic
    UNION ALL SELECT 26, (map('k2', 2, 'k1', 1)::Map(String, Int64))::Dynamic
    UNION ALL SELECT 27, ('{\"a\": {\"b\": 1, \"c\": [true, null]}, \"d\": \"e\"}'::JSON)::Dynamic
    UNION ALL SELECT 28, ([map('x', [1::Int64]), map()]::Array(Map(String, Array(Int64))))::Dynamic
    UNION ALL SELECT 29, ('one'::Enum8('one' = 1))::Dynamic
    UNION ALL SELECT 30, ('abc'::LowCardinality(String))::Dynamic
    UNION ALL SELECT 31, ([NULL, 1]::Array(Nullable(Int32)))::Dynamic
    UNION ALL SELECT 32, (42::Variant(Int64, String))::Dynamic
    SETTINGS engine_file_truncate_on_insert = 1;

    SELECT n, dynamicType(v), v FROM file('${PARQUET_FILE}', Parquet) ORDER BY n;
"

echo "--- Iceberg schema"
${CLICKHOUSE_CLIENT} --query "
    SET allow_insert_into_iceberg = 1;
    CREATE TABLE t_variant_write (id Int32, d Dynamic, v Variant(Int64, String, Array(Int64)))
        ENGINE = IcebergLocal('${TABLE_PATH}') SETTINGS iceberg_format_version = 3;
    INSERT INTO t_variant_write VALUES (1, 42, 'a'), (2, 'str', [1, 2]), (3, NULL, NULL), (4, [1.5, 2.5], 7);
    INSERT INTO t_variant_write SELECT 5, '{\"k\": [1, 2]}'::JSON, NULL;
"
python3 - "${TABLE_PATH}/metadata" <<'PY'
import glob, json, os, re, sys
latest = max(glob.glob(os.path.join(sys.argv[1], "v*.metadata.json")),
             key=lambda path: int(re.search(r"/v(\d+)\.metadata\.json$", path).group(1)))
with open(latest) as f:
    metadata = json.load(f)
for field in metadata["schemas"][-1]["fields"]:
    print(field["name"], field["type"], field["required"])
PY

echo "--- Iceberg data"
${CLICKHOUSE_CLIENT} --query "SELECT id, dynamicType(d), d, dynamicType(v), v FROM icebergLocal('${TABLE_PATH}') ORDER BY id"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM icebergLocal('${TABLE_PATH}') WHERE d IS NULL"

echo "--- Null counts"
for manifest in "${TABLE_PATH}"/metadata/*.avro; do
    case "$(basename "${manifest}")" in snap-*) continue ;; esac
    ${CLICKHOUSE_LOCAL} --query "
        SELECT data_file.record_count, arraySort(data_file.null_value_counts) FROM file('${manifest}', Avro)
    "
done | sort

echo "--- Errors"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${PARQUET_FILE}', Parquet, 'v Dynamic') SELECT 18446744073709551615::UInt64
    SETTINGS engine_file_truncate_on_insert = 1
" 2>&1 | grep -o -m1 "the widest variant integer is a signed 64-bit integer"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${PARQUET_FILE}', Parquet, 'v Dynamic') SELECT map('a', 1, 'a', 2)
    SETTINGS engine_file_truncate_on_insert = 1
" 2>&1 | grep -o -m1 "the keys of a variant object must be unique"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${PARQUET_FILE}', Parquet, 'v Dynamic') SELECT toIPv4('1.2.3.4')
    SETTINGS engine_file_truncate_on_insert = 1
" 2>&1 | grep -o -m1 "Cannot write a value of type IPv4 into the Parquet variant column 'v'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_variant_write"
rm -rf "${PARQUET_FILE}" "${TABLE_PATH}"
