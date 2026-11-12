#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.
# Tag no-msan: the wasmtime engine behind WASM UDFs is not instrumented for MSan.

# An UPDATE of an Iceberg partition column refuses a non-deterministic condition and accepts a deterministic one,
# also when the condition calls an EXECUTABLE or WASM user-defined function.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

module_name="iceberg_udf_${CLICKHOUSE_TEST_UNIQUE_NAME}"
det="iceberg_udf_det_${CLICKHOUSE_TEST_UNIQUE_NAME}"
nondet="iceberg_udf_nondet_${CLICKHOUSE_TEST_UNIQUE_NAME}"

rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"
${CLICKHOUSE_CLIENT} --query "
    DROP FUNCTION IF EXISTS ${det};
    DROP FUNCTION IF EXISTS ${nondet};
    DELETE FROM system.webassembly_modules WHERE name = '${module_name}';"

${CLICKHOUSE_CLIENT} \
    --query "INSERT INTO system.webassembly_modules (name, code) SELECT '${module_name}', code FROM input('code String') FORMAT RawBlob" \
    < "${CUR_DIR}/wasm/identity_int.wasm"

${CLICKHOUSE_CLIENT} --query "
    CREATE FUNCTION ${nondet} LANGUAGE WASM FROM '${module_name}' :: 'identity_msgpack_i32' ARGUMENTS (x Int32) RETURNS Int32 ABI BUFFERED_V1;
    CREATE FUNCTION ${det} LANGUAGE WASM FROM '${module_name}' :: 'identity_msgpack_i32' ARGUMENTS (x Int32) RETURNS Int32 ABI BUFFERED_V1 DETERMINISTIC;"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    CREATE TABLE tp (p Int32, id Int32, v Int32) ENGINE = IcebergLocal('${CLICKHOUSE_USER_FILES_UNIQUE}/tp', 'Parquet') PARTITION BY p;
    INSERT INTO tp SELECT number % 4, number, 0 FROM numbers(1000);"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    ALTER TABLE tp UPDATE p = p + 4 WHERE test_function(toUInt64(id), toUInt64(0)) % 2 = 0" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED'
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    ALTER TABLE tp UPDATE p = p + 4 WHERE ${nondet}(id) % 2 = 0" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED'
${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(id), max(p) FROM tp"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE tp UPDATE p = p + 4 WHERE ${det}(id) % 2 = 0"
${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(id), countIf(p >= 4), countIf(p % 4 != id % 4) FROM tp"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE tp;
    DROP FUNCTION ${det};
    DROP FUNCTION ${nondet};
    DELETE FROM system.webassembly_modules WHERE name = '${module_name}';"
rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"
