#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: writes into user_files

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A dictionary created by DDL may read only files inside user_files. The check folded a `..` lexically,
# while the source opened the path as written, so the kernel resolved the `..` against the target of a
# preceding symlink. With `escw` a symlink from user_files to `${OUTSIDE_DIR}/inner`, the check saw
# `<user_files>/.../secret.*` and the source read `${OUTSIDE_DIR}/secret.*`.

DATA_DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
OUTSIDE_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_outside"
function cleanup()
{
    rm -rf "${DATA_DIR}" "${OUTSIDE_DIR}"
}
trap cleanup EXIT
cleanup

mkdir -p "${DATA_DIR}/sub" "${OUTSIDE_DIR}/inner"
ln -s "${OUTSIDE_DIR}/inner" "${DATA_DIR}/escw"

for file in "${OUTSIDE_DIR}/secret" "${OUTSIDE_DIR}/inner/via_symlink" "${DATA_DIR}/back_inside"
do
    value=$(basename "${file}" | tr '[:lower:]' '[:upper:]')
    printf -- "- regexp: 'key'\n  name: '%s'\n" "${value}" > "${file}.yaml"
    printf '1\t%s\n' "${value}" > "${file}.tsv"
done

dictionary_number=0

function check()
{
    local source=$1
    local path=$2
    dictionary_number=$((dictionary_number + 1))
    local dictionary="${CLICKHOUSE_DATABASE}.dict_${dictionary_number}"

    if [ "${source}" = "YAMLRegExpTree" ]
    then
        ${CLICKHOUSE_CLIENT} --query "
            CREATE DICTIONARY ${dictionary} (regexp String, name String)
            PRIMARY KEY regexp
            SOURCE(YAMLRegExpTree(PATH '${path}.yaml'))
            LAYOUT(regexp_tree) LIFETIME(0)
            SETTINGS(regexp_dict_allow_hyperscan = false)"
        key="'key'"
    else
        ${CLICKHOUSE_CLIENT} --query "
            CREATE DICTIONARY ${dictionary} (id UInt64, name String)
            PRIMARY KEY id
            SOURCE(FILE(PATH '${path}.tsv' FORMAT 'TabSeparated'))
            LAYOUT(FLAT()) LIFETIME(0)"
        key="toUInt64(1)"
    fi

    echo -n "${source} ${path#"${DATA_DIR}/"}: "
    ${CLICKHOUSE_CLIENT} --query "SELECT dictGet('${dictionary}', 'name', ${key})" 2>&1 \
        | grep -o -m1 "is not inside\|SECRET\|VIA_SYMLINK\|BACK_INSIDE" || echo "UNEXPECTED"
    ${CLICKHOUSE_CLIENT} --query "DROP DICTIONARY ${dictionary}"
}

for source in YAMLRegExpTree File
do
    # A `..` behind a symlink that leaves user_files is resolved against the symlink target.
    check "${source}" "${DATA_DIR}/escw/../secret"
    # A symlink inside user_files that leaves it is still followed.
    check "${source}" "${DATA_DIR}/escw/via_symlink"
    # A `..` that stays inside user_files is still allowed.
    check "${source}" "${DATA_DIR}/sub/../back_inside"
done
