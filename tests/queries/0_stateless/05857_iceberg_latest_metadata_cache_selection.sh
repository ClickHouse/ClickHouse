#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires MinIO and the Avro library (Iceberg)

# Under `iceberg_metadata_staleness_ms`, the latest metadata file cached for a table is reused only by a read
# that chooses the latest file the same way: by version, by `last-updated-ms`, by the version hint, or among
# the `v` files only.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=1 --iceberg_metadata_staleness_ms=600000"
TABLE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/"
rm -rf "${TABLE_PATH}"
trap 'rm -rf "${TABLE_PATH}"' EXIT

# Written by another process, so the server has cached nothing of it: v1 is empty, v2 holds 1 and is the newest
# by `last-updated-ms`, v3 holds 1 and 2, and the version hint names v1.
${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 -m -q "
    CREATE TABLE t (c0 Int32) ENGINE = IcebergLocal('${TABLE_PATH}');
    INSERT INTO t VALUES (1);
    INSERT INTO t VALUES (2);"
sed -i -E 's/"last-updated-ms" : [0-9]+/"last-updated-ms" : 9999999999999/' "${TABLE_PATH}metadata/v2.metadata.json"
echo -n 'v1.metadata.json' > "${TABLE_PATH}metadata/version-hint.text"

BY_MS=", SETTINGS iceberg_recent_metadata_file_by_last_updated_ms_field = 1"
HINT=", SETTINGS iceberg_use_version_hint = 1"
read_sum()
{
    ${CLIENT} -q "SELECT '$1', sum(c0) FROM icebergLocal('${TABLE_PATH}'$2)"
}

read_sum by-ms "${BY_MS}"
read_sum by-version ""
read_sum hint "${HINT}"
read_sum by-ms "${BY_MS}"
read_sum by-version ""

# Within the window a read is served from the cache: the hint now names v3, but the cached v1 is read.
echo -n 'v3.metadata.json' > "${TABLE_PATH}metadata/version-hint.text"
read_sum hint "${HINT}"

# A commit through the server drops the cached files of every selection. It also points the hint at its v4.
${CLIENT} -q "CREATE TABLE t ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLIENT} -q "INSERT INTO t VALUES (4)"
read_sum by-version-after-insert ""
read_sum hint-after-insert "${HINT}"
${CLIENT} -q "DROP TABLE t"

# On S3, `iceberg_list_only_v_prefixed_metadata_files` keeps only the `v` files as candidates: v3 against a copy
# of v2 named `00005-<uuid>.metadata.json`, which has the highest version.
S3_PATH="05857_iceberg_latest_metadata_cache_selection/${CLICKHOUSE_TEST_UNIQUE_NAME}"
${CLIENT} -q "
    CREATE TABLE t_s3 (c0 Int32) ENGINE = IcebergS3(s3_conn, filename = '${S3_PATH}');
    INSERT INTO t_s3 VALUES (1);
    INSERT INTO t_s3 VALUES (2);"
${CLIENT} -q "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${S3_PATH}/metadata/00005-6f6f6407-c6a5-465f-a808-ea8900e35a38.metadata.json', format = 'RawBLOB')
    SELECT * FROM s3(s3_conn, filename = '${S3_PATH}/metadata/v2.metadata.json', format = 'RawBLOB')"

read_s3_sum()
{
    ${CLIENT} --iceberg_list_only_v_prefixed_metadata_files="$2" --send_logs_level=error \
        -q "SELECT '$1', sum(c0) FROM icebergS3(s3_conn, filename = '${S3_PATH}'$3)"
}

read_s3_sum v-files 1 ""
read_s3_sum all-files 0 ""

# A hint naming no file falls back (with a warning) to listing the files of the scheme it spells, here the `v`
# files, and that listing does not replace the file cached for a listing of all files.
${CLIENT} --s3_truncate_on_insert=1 -q "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${S3_PATH}/metadata/version-hint.text', format = 'RawBLOB') SELECT '9'"
read_s3_sum hint-fallback 0 "${HINT}"
read_s3_sum all-files 0 ""

${CLIENT} -q "DROP TABLE t_s3"
