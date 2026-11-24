#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro.

# gzip-compressed metadata files are named with the Iceberg extension `.gz.metadata.json`, not the
# Content-Encoding token `.gzip.metadata.json`, which Spark / Hadoop-catalog readers cannot find.
# Files named `.gzip.metadata.json` by older versions are still found through `version-hint.text`.
# https://github.com/ClickHouse/ClickHouse/issues/109801

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${ROOT:?}"
trap 'rm -rf "${ROOT:?}"' EXIT

list_metadata_extensions()
{
    ls "$1/metadata" | grep -o -e '\.gz\.metadata\.json$' -e '\.gzip\.metadata\.json$' | sort | uniq -c | awk '{print $2, $1}'
}

create_and_insert()
{
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
        CREATE TABLE $1 (x String, y Int64) ENGINE = IcebergLocal('$2', 'Parquet')
        SETTINGS iceberg_use_version_hint = 1, iceberg_format_version = $3, iceberg_metadata_compression_method = 'gzip';
        SELECT * FROM $1 ORDER BY ALL;
        INSERT INTO $1 SETTINGS iceberg_metadata_compression_method = 'gzip' VALUES ('123', 1);
        SELECT * FROM $1 ORDER BY ALL;"
}

for fv in 1 2; do
    echo "--- written with format version ${fv}"
    TABLE_PATH="${ROOT}/t_fv${fv}"
    create_and_insert "t_fv${fv}" "${TABLE_PATH}" "${fv}"
    list_metadata_extensions "${TABLE_PATH}"
done

echo "--- legacy .gzip.metadata.json"
TABLE_PATH="${ROOT}/t_legacy"
create_and_insert t_legacy "${TABLE_PATH}" 2
for f in "${TABLE_PATH}"/metadata/v*.gz.metadata.json; do
    mv "$f" "${f%.gz.metadata.json}.gzip.metadata.json"
done
list_metadata_extensions "${TABLE_PATH}"
# `version-hint.text` keeps a bare version number, so the lookup goes through the legacy fallback.
grep -cxE '[0-9]+' "${TABLE_PATH}/metadata/version-hint.text"
# A fresh table function resolves the metadata file from the version hint.
${CLICKHOUSE_CLIENT} --iceberg_metadata_compression_method=gzip --query "
    SELECT * FROM icebergLocal('${TABLE_PATH}', SETTINGS iceberg_use_version_hint = 1) ORDER BY ALL"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_fv1; DROP TABLE t_fv2; DROP TABLE t_legacy;"
