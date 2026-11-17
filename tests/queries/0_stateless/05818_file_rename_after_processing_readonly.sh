#!/usr/bin/env bash
# `rename_files_after_processing` renames the files a SELECT has read, a write to the source,
# so such a read is refused in readonly mode, which every HTTP GET request implies.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILES_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}"
mkdir -p "${FILES_DIR}"
USER_NAME="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"
RENAME="rename_files_after_processing = 'processed_%a'"

for name in readonly_2 http_get cluster_readonly_2 describe_readonly_2 no_setting_readonly_2 readonly_0; do
    echo 7 > "${FILES_DIR}/${name}.csv"
done

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${USER_NAME};
CREATE USER ${USER_NAME} IDENTIFIED WITH no_password;
GRANT CREATE TEMPORARY TABLE ON *.* TO ${USER_NAME};
GRANT READ, WRITE ON FILE TO ${USER_NAME};
"

# Prints what became of one input file. The no-side-effect half of every denial is asserted with it:
# a refusal that still renamed the file would regress silently.
file_state() {
    if [ -f "${FILES_DIR}/processed_$1.csv" ] && [ ! -f "${FILES_DIR}/$1.csv" ]; then
        echo "renamed"
    elif [ -f "${FILES_DIR}/$1.csv" ] && [ ! -f "${FILES_DIR}/processed_$1.csv" ]; then
        echo "intact"
    else
        echo "unexpected"
    fi
}

renaming_select() { echo "SELECT * FROM file('${FILES_DIR}/$1.csv', 'CSV', 'x UInt8') SETTINGS ${RENAME}"; }

echo '--- readonly = 2'
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" --readonly 2 -q "$(renaming_select readonly_2)" 2>&1 | grep -o -m1 '(READONLY)'
file_state readonly_2

echo '--- HTTP GET'
${CLICKHOUSE_CURL} -sS --get "${CLICKHOUSE_URL}&user=${USER_NAME}" --data-urlencode "query=$(renaming_select http_get)" |
    grep -o -m1 'method GET implies readonly'
file_state http_get

echo '--- fileCluster, refused on the initiator'
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" --readonly 2 -q \
    "SELECT * FROM fileCluster('test_shard_localhost', '${FILES_DIR}/cluster_readonly_2.csv', 'CSV', 'x UInt8') SETTINGS ${RENAME}" 2>&1 |
    grep -o -m1 '(READONLY)'
file_state cluster_readonly_2

echo '--- DESCRIBE builds no reader and is allowed'
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" --readonly 2 -q \
    "DESCRIBE TABLE file('${FILES_DIR}/describe_readonly_2.csv', 'CSV', 'x UInt8') SETTINGS ${RENAME} FORMAT TSV" | cut -f1,2
file_state describe_readonly_2

echo '--- a read without the setting is allowed'
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" --readonly 2 -q "SELECT * FROM file('${FILES_DIR}/no_setting_readonly_2.csv', 'CSV', 'x UInt8')"
file_state no_setting_readonly_2

echo '--- without readonly the file is renamed'
${CLICKHOUSE_CLIENT} --user "${USER_NAME}" -q "$(renaming_select readonly_0)"
file_state readonly_0

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER_NAME}"
rm -f "${FILES_DIR}"/*.csv
