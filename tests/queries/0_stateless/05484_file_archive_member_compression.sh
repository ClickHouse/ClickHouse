#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the `zip` utility and libarchive.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The compression method of `file()` (explicit or detected from the name) applies to a file
# inside an archive the same way as to a plain file.

FILE_PREFIX="${CLICKHOUSE_TEST_UNIQUE_NAME}"
MEMBERS_DIR="${CLICKHOUSE_TMP}/${FILE_PREFIX}_members"
mkdir -p "${MEMBERS_DIR}"

printf '{"a":1}\n{"a":2}\n' | gzip > "${MEMBERS_DIR}/gz.json"
printf '{"a":1}\n{"a":2}\n' | gzip > "${MEMBERS_DIR}/gz.data"
printf '{"a":3}\n' | gzip > "${MEMBERS_DIR}/auto1.json.gz"
printf '{"a":4}\n' | gzip > "${MEMBERS_DIR}/auto2.json.gz"
printf '{"a":5}\n' > "${MEMBERS_DIR}/plain.json"
printf '{"a":6}\n' > "${MEMBERS_DIR}/plain.data"
printf '1\n2\n' | gzip -n > "${MEMBERS_DIR}/lines.gz"

MEMBERS="gz.json gz.data auto1.json.gz auto2.json.gz plain.json plain.data lines.gz"
TAR_ARCHIVE="${USER_FILES_PATH}/${FILE_PREFIX}.tar"
ZIP_ARCHIVE="${USER_FILES_PATH}/${FILE_PREFIX}.zip"
# shellcheck disable=SC2086
tar -cf "${TAR_ARCHIVE}" -C "${MEMBERS_DIR}" ${MEMBERS}
# shellcheck disable=SC2086
(cd "${MEMBERS_DIR}" && zip -q "${ZIP_ARCHIVE}" ${MEMBERS})
# An old modification time keeps the schema/count cache entries for these archives valid
# for the whole test, so a later read cannot hide behind a changed archive.
touch -d '2000-01-01' "${TAR_ARCHIVE}" "${ZIP_ARCHIVE}"

for ARCHIVE in "${TAR_ARCHIVE}" "${ZIP_ARCHIVE}"
do
    echo "${ARCHIVE##*.}"
    echo "explicit gzip"
    ${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('${ARCHIVE} :: gz.json', JSONEachRow, 'a Int64', 'gzip') ORDER BY a"
    echo "auto by the name of the file in the archive"
    ${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('${ARCHIVE} :: auto1.json.gz', JSONEachRow, 'a Int64') ORDER BY a"
    ${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('${ARCHIVE} :: auto1.json.gz', JSONEachRow, 'a Int64', 'auto') ORDER BY a"
    echo "schema inference"
    ${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('${ARCHIVE} :: auto1.json.gz') ORDER BY a"
    ${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('${ARCHIVE} :: gz.json', JSONEachRow, 'auto', 'gzip') ORDER BY a"
    echo "format detection"
    ${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('${ARCHIVE} :: gz.data', 'auto', 'auto', 'gzip') ORDER BY a"
    echo "glob"
    ${CLICKHOUSE_CLIENT} -q "SELECT _file, a FROM file('${ARCHIVE} :: auto*.json.gz', JSONEachRow, 'a Int64') ORDER BY a"
    ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM file('${ARCHIVE} :: auto*.json.gz', JSONEachRow, 'a Int64')"
    ${CLICKHOUSE_CLIENT} -q "SELECT _file, a FROM file('${ARCHIVE} :: {auto1.json.gz,plain.json}', JSONEachRow, 'a Int64') ORDER BY a"
    echo "glob with schema inference"
    ${CLICKHOUSE_CLIENT} -q "SELECT _file, a FROM file('${ARCHIVE} :: auto*.json.gz') ORDER BY a SETTINGS schema_inference_mode = 'union'"
    # Format detection tries several formats, and every retry must read the file in the archive from its beginning.
    ${CLICKHOUSE_CLIENT} -q "SELECT _file, a FROM file('${ARCHIVE} :: gz.d*', 'auto', 'auto', 'gzip') ORDER BY a SETTINGS schema_inference_use_cache_for_file = 0"
    ${CLICKHOUSE_CLIENT} -q "SELECT _file, a FROM file('${ARCHIVE} :: gz.d*', 'auto', 'auto', 'gzip') ORDER BY a"
    ${CLICKHOUSE_CLIENT} -q "SELECT _file, a FROM file('${ARCHIVE} :: plain.d*', 'auto') ORDER BY a SETTINGS schema_inference_use_cache_for_file = 0"
    echo "not compressed"
    ${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('${ARCHIVE} :: plain.json', JSONEachRow, 'a Int64') ORDER BY a"
    ${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('${ARCHIVE} :: plain.json') ORDER BY a"
    echo "count cache is separated by member compression"
    ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM file('${ARCHIVE} :: lines.gz', LineAsString, 's String', 'gzip')"
    ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM file('${ARCHIVE} :: lines.gz', LineAsString, 's String', 'none')"
    ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM file('${ARCHIVE} :: lines.gz', LineAsString, 's String', 'none') SETTINGS use_cache_for_count_from_files = 0"
    ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM file('${ARCHIVE} :: lines.gz', LineAsString, 's String', 'gzip')"
    ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM file('${ARCHIVE} :: lines.*', LineAsString, 's String', 'gzip')"
    ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM file('${ARCHIVE} :: lines.*', LineAsString, 's String', 'none')"
done

rm -rf "${MEMBERS_DIR}" "${TAR_ARCHIVE}" "${ZIP_ARCHIVE}"
