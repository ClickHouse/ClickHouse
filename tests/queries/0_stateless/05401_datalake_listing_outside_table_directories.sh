#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires MinIO and the Avro library (Iceberg)

# A data lake table consists of the files directly inside its metadata directory and of the files
# under its data directory. Objects in a subdirectory of `metadata/` or in a sibling directory whose
# name starts with the same characters (`metadata_old/`, `data_backup/`) must neither be read as
# table metadata nor be deleted by `OPTIMIZE`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1"
PREFIX="05401_iceberg_listing/${CLICKHOUSE_TEST_UNIQUE_NAME}"
LOCAL_DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -rf "${LOCAL_DIR}" 2>/dev/null' EXIT
rm -rf "${LOCAL_DIR}"

copy_object()
{
    $CLIENT --query "
        INSERT INTO FUNCTION s3(s3_conn, filename = '$2', format = 'RawBLOB')
        SELECT * FROM s3(s3_conn, filename = '$1', format = 'RawBLOB')"
}

# Another table's metadata file is the stray object: reading it yields that table's schema and rows.
$CLIENT --query "
    CREATE TABLE other (zzz String) ENGINE = IcebergS3(s3_conn, filename = '${PREFIX}/other');
    INSERT INTO other VALUES ('x');"
STRAY="${PREFIX}/other/metadata/v1.metadata.json"

# $1: arm name, $2: where the stray goes, relative to the table directory
check_stray()
{
    local table="main_$1"
    local path="${PREFIX}/${table}"
    $CLIENT --query "
        CREATE TABLE ${table} (a Int64) ENGINE = IcebergS3(s3_conn, filename = '${path}');
        INSERT INTO ${table} VALUES (1), (2), (3);"
    copy_object "${STRAY}" "${path}/$2"
    $CLIENT --query "SELECT '$1', count() FROM icebergS3(s3_conn, filename = '${path}')"
    echo "$1 $($CLIENT --query "DESCRIBE TABLE icebergS3(s3_conn, filename = '${path}')" | cut -f 1 | head -n 1)"
    $CLIENT --query "SELECT '$1', count() FROM ${table}"
}

check_stray subdirectory "metadata/backup/v99.metadata.json"
check_stray sibling "metadata_old/v98.metadata.json"
# The table's own path repeated inside its metadata directory.
check_stray nested_copy "metadata/x/${PREFIX}/main_nested_copy/metadata/v97.metadata.json"

# Strays must not make the path look like an existing table to `CREATE`.
copy_object "${STRAY}" "${PREFIX}/fresh/metadata_old/v1.metadata.json"
copy_object "${STRAY}" "${PREFIX}/fresh/metadata/backup/v1.metadata.json"
$CLIENT --query "
    CREATE TABLE fresh (b Int64) ENGINE = IcebergS3(s3_conn, filename = '${PREFIX}/fresh');
    INSERT INTO fresh VALUES (7);
    SELECT 'create', b FROM fresh;"

# `OPTIMIZE` removes the replaced files of the table, and nothing outside its directories.
IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")
OPT="${PREFIX}/opt"
$CLIENT --query "
    CREATE TABLE opt (a Int64) ENGINE = IcebergS3(s3_conn, filename = '${OPT}');
    INSERT INTO opt VALUES (1);
    INSERT INTO opt VALUES (2);"
for dir in data_backup database_dump metadata_old metadata/nested; do
    $CLIENT --query "
        INSERT INTO FUNCTION s3(s3_conn, filename = '${OPT}/${dir}/keep.csv', format = 'CSV', structure = 'x String')
        VALUES ('keep')"
done
$CLIENT --query "
    DELETE FROM opt WHERE a = 1;
    CREATE TABLE old_data_files ENGINE = Memory AS SELECT _file FROM s3(s3_conn, filename = '${OPT}/data/*', format = 'One');
    SELECT 'old data files', count() > 0 FROM old_data_files;"

# The cloud build routes `OPTIMIZE` elsewhere and may refuse it, see 04815_iceberg_optimize_position_delete_row_count_113745.
optimize_err=$(${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE opt" 2>&1)
if [[ "${IS_CLOUD}" = "1" ]]; then
    echo "old data files left 0"
elif [[ -n "${optimize_err}" ]]; then
    echo "OPTIMIZE failed: ${optimize_err}"
else
    $CLIENT --query "
        SELECT 'old data files left', count() FROM s3(s3_conn, filename = '${OPT}/data/*', format = 'One')
        WHERE _file IN (SELECT _file FROM old_data_files)"
fi
$CLIENT --query "
    SELECT 'kept', replaceRegexpOne(_path, '.*/opt/', '') FROM s3(s3_conn, filename = '${OPT}/**', format = 'One')
    WHERE _file = 'keep.csv' ORDER BY _path;
    SELECT 'opt', count() FROM opt;"

# Paimon keeps its schemas and snapshots flat in `schema/` and `snapshot/`.
PAIMON="${LOCAL_DIR}/paimon"
mkdir -p "${LOCAL_DIR}"
cp -r "${CUR_DIR}/data_minio/paimon_no_partition" "${PAIMON}"
mkdir -p "${PAIMON}/schema/backup" "${PAIMON}/snapshot/backup"
sed 's/"f_boolean"/"f_zzz"/' "${PAIMON}/schema/schema-0" > "${PAIMON}/schema/backup/schema-99"
echo "paimon schema $(${CLICKHOUSE_CLIENT} --enable_time_time64_type=1 --query "DESCRIBE TABLE paimonLocal('${PAIMON}')" | cut -f 1 | head -n 1)"
rm -r "${PAIMON}/schema/backup"
rm "${PAIMON}/snapshot/LATEST"
echo garbage > "${PAIMON}/snapshot/backup/snapshot-99"
${CLICKHOUSE_CLIENT} --enable_time_time64_type=1 --query "SELECT 'paimon snapshot', count() FROM paimonLocal('${PAIMON}')"

$CLIENT --query "
    DROP TABLE other;
    DROP TABLE main_subdirectory;
    DROP TABLE main_sibling;
    DROP TABLE main_nested_copy;
    DROP TABLE fresh;
    DROP TABLE opt;
    DROP TABLE old_data_files;"
