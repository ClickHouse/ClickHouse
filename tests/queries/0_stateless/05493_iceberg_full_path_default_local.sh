#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires `IcebergLocal` (USE_AVRO build option) and Iceberg writes.

# `write_full_path_in_iceberg_metadata` is enabled by default: a table created without touching the setting
# records a fully-qualified `file://` location, and the manifest list and data file paths inherit the scheme.
# Scheme-less paths such as `/table/data/...` are not valid before Iceberg format version 4 and cannot be
# opened by external readers.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/iceberg_full_path_default_${CLICKHOUSE_TEST_UNIQUE_NAME}"
LOCAL_PATH="${WORK_DIR}/db"
TABLE_ROOT="${WORK_DIR}/iceberg/t0"

rm -rf "${WORK_DIR}"
mkdir -p "${LOCAL_PATH}" "${TABLE_ROOT}"
trap 'rm -rf "${WORK_DIR}"' EXIT

${CLICKHOUSE_LOCAL} \
    --path "${LOCAL_PATH}" \
    --allow_insert_into_iceberg=1 \
    --iceberg_metadata_compression_method='' \
    --multiquery \
    --query "
        CREATE TABLE t0 (x Int32) ENGINE = IcebergLocal('${TABLE_ROOT}/');
        INSERT INTO t0 VALUES (1), (2);
        SELECT sum(x) FROM t0;
    " -- --user_files_path="${WORK_DIR}"

${CLICKHOUSE_LOCAL} \
    --multiquery \
    --query "
        SELECT 'location', _file, replaceOne(JSONExtractString(c, 'location'), '${TABLE_ROOT}', '<root>')
        FROM file('${TABLE_ROOT}/metadata/*.metadata.json', 'RawBLOB', 'c String')
        ORDER BY _file;

        SELECT 'manifest-list', replaceRegexpOne(replaceOne(JSONExtractString(s, 'manifest-list'), '${TABLE_ROOT}', '<root>'), 'snap-.*\\\\.avro', 'snap-<id>.avro')
        FROM file('${TABLE_ROOT}/metadata/v2.metadata.json', 'RawBLOB', 'c String')
        ARRAY JOIN JSONExtractArrayRaw(c, 'snapshots') AS s;

        SELECT 'data-file', replaceRegexpAll(replaceOne(p, '${TABLE_ROOT}', '<root>'), 'data-[0-9a-f-]+\\\\.parquet', 'data-<uuid>.parquet')
        FROM file('${TABLE_ROOT}/metadata/*.avro', 'RawBLOB', 'c String')
        ARRAY JOIN extractAll(c, 'file://[ -~]+?\\\\.parquet') AS p
        WHERE _file NOT LIKE 'snap-%'
        ORDER BY p;
    " -- --user_files_path="${WORK_DIR}"
