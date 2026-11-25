#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3 (minio) and on the Avro and Parquet libraries.

# A table copied without rewriting its metadata keeps the source `location`, and its file paths are re-rooted
# at the copy. A path may spell that `location` differently: `s3a://` for `s3://`, or `file:///` for
# `file://localhost/`. The copy must read its own files, so the source data files are emptied after copying.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "S3"

S3_DIR="05843_iceberg_copied/${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg 1 --write_full_path_in_iceberg_metadata 1 -q "
    DROP TABLE IF EXISTS t_src;
    CREATE TABLE t_src (c0 Int) ENGINE = IcebergS3(s3_conn, filename = '${S3_DIR}/src');
    INSERT INTO t_src VALUES (1), (2);
    DROP TABLE t_src;
"

# The file paths stay `s3://` while `location` becomes `s3a://`.
${CLICKHOUSE_CLIENT} --input_format_parallel_parsing 0 --output_format_parallel_formatting 0 -q "
    SELECT * FROM s3(s3_conn, filename = '${S3_DIR}/src/metadata/v2.metadata.json', structure = 'line String', format = 'LineAsString')
" | python3 -c "
import json, sys
m = json.load(sys.stdin)
assert m['location'].startswith('s3://'), m['location']
m['location'] = 's3a://' + m['location'][len('s3://'):]
print(json.dumps(m))
" | ${CLICKHOUSE_CLIENT} -q "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${S3_DIR}/src/metadata/v2.metadata.json', structure = 'line String', format = 'LineAsString')
    SETTINGS s3_truncate_on_insert = 1
    SELECT * FROM input('line String') FORMAT LineAsString
"

for path in $(${CLICKHOUSE_CLIENT} -q "SELECT _path FROM s3(s3_conn, filename = '${S3_DIR}/src/**', format = 'One')"); do
    file="${path#*"${S3_DIR}/src/"}"
    ${CLICKHOUSE_CLIENT} -q "
        INSERT INTO FUNCTION s3(s3_conn, filename = '${S3_DIR}/copy/${file}', format = 'RawBLOB') SETTINGS s3_truncate_on_insert = 1
        SELECT * FROM s3(s3_conn, filename = '${S3_DIR}/src/${file}', format = 'RawBLOB')"
    if [[ "${file}" == data/* ]]; then
        ${CLICKHOUSE_CLIENT} -q "
            INSERT INTO FUNCTION s3(s3_conn, filename = '${S3_DIR}/src/${file}', format = 'RawBLOB') SETTINGS s3_truncate_on_insert = 1
            SELECT ''"
    fi
done

${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache 0 -q "
    SELECT * FROM icebergS3(s3_conn, filename = '${S3_DIR}/copy') ORDER BY c0;
"

echo "Local"

# `IcebergLocal` of `clickhouse local` refuses a path outside its working directory.
LOCAL_DIR="${PWD}/${CLICKHOUSE_TEST_UNIQUE_NAME}_iceberg_copied"
rm -rf "${LOCAL_DIR}"
mkdir -p "${LOCAL_DIR}"

${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}/db" --allow_insert_into_iceberg 1 -q "
    CREATE TABLE t_src (c0 Int) ENGINE = IcebergLocal('${LOCAL_DIR}/src/');
    INSERT INTO t_src VALUES (1), (2);
"

# `location` names the `localhost` authority, the manifest list an empty one; other file paths have no scheme.
python3 -c "
import json, sys
path = sys.argv[1]
with open(path) as f:
    m = json.load(f)
assert m['location'].startswith('/'), m['location']
m['location'] = 'file://localhost' + m['location']
for s in m['snapshots']:
    assert s['manifest-list'].startswith('/'), s['manifest-list']
    s['manifest-list'] = 'file://' + s['manifest-list']
with open(path, 'w') as f:
    json.dump(m, f)
" "${LOCAL_DIR}/src/metadata/v2.metadata.json"

cp -r "${LOCAL_DIR}/src" "${LOCAL_DIR}/copy"
for file in "${LOCAL_DIR}"/src/data/*; do
    : > "${file}"
done

${CLICKHOUSE_LOCAL} -q "SELECT * FROM icebergLocal('${LOCAL_DIR}/copy/') ORDER BY c0"

rm -rf "${LOCAL_DIR}"
