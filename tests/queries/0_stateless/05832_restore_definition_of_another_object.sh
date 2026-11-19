#!/usr/bin/env bash
# Tags: no-darwin
# no-darwin: `information_schema.tables` and `information_schema.TABLES` collide in a `File()` backup on case-insensitive APFS, https://github.com/ClickHouse/ClickHouse/issues/127645

# `RESTORE` authorizes the object named by the path of a definition in the backup, so a definition
# that creates another object must be rejected before anything is created. The backups are edited
# on disk because `BACKUP` never writes such a definition.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR=${CLICKHOUSE_TMP}/05832_restore_definition_of_another_object_${CLICKHOUSE_DATABASE}
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}/backups"

CONFIG="${WORK_DIR}/config.xml"
cat > "${CONFIG}" <<EOF
<clickhouse>
    <backups>
        <allowed_path>${WORK_DIR}/backups</allowed_path>
    </backups>
</clickhouse>
EOF

B=${WORK_DIR}/backups

${CLICKHOUSE_LOCAL} --config-file "${CONFIG}" --path "${WORK_DIR}/data" -q "
CREATE DATABASE a;
CREATE TABLE a.t (x UInt8) ENGINE = Memory;
CREATE TEMPORARY TABLE tmp (x UInt8) ENGINE = Memory;
BACKUP TABLE a.t TO File('${B}/table') FORMAT Null;
BACKUP TABLE a.t TO File('${B}/table_honest') FORMAT Null;
BACKUP TEMPORARY TABLE tmp TO File('${B}/temporary') FORMAT Null;
BACKUP DATABASE a TO File('${B}/database') FORMAT Null;
BACKUP DATABASE information_schema TO File('${B}/predefined') FORMAT Null;
"

function local_restored()
{
    ${CLICKHOUSE_LOCAL} --config-file "${CONFIG}" --path "${WORK_DIR}/restored" -q "$1"
}

# Prints the case number and either OK or the error name of the restore.
function restore()
{
    local case_number=$1 file=$2 definition=$3 query=$4 output
    if [ -n "${file}" ]; then
        printf '%s' "${definition}" > "${B}/${file}"
    fi
    if output=$(local_restored "${query}" 2>&1); then
        echo "${case_number} OK"
    else
        echo "${case_number} $(echo "${output}" | grep -oE '\((CANNOT_RESTORE|UNKNOWN)_[A-Z_]+\)' | head -1 | tr -d '()')"
    fi
}

local_restored "CREATE DATABASE a; CREATE DATABASE b;"

restore 1 table/metadata/a/t.sql "CREATE TABLE b.t (x UInt8) ENGINE = Memory" \
    "RESTORE TABLE a.t FROM File('${B}/table') FORMAT Null"
restore 2 table/metadata/a/t.sql "CREATE TABLE a.injected_2 (x UInt8) ENGINE = Memory" \
    "RESTORE TABLE a.t FROM File('${B}/table') FORMAT Null"
restore 3 table/metadata/a/t.sql "CREATE DATABASE injected_3 ENGINE = Memory" \
    "RESTORE TABLE a.t FROM File('${B}/table') FORMAT Null"
restore 4 temporary/temporary_tables/metadata/tmp.sql "CREATE TABLE b.injected_4 (x UInt8) ENGINE = Memory" \
    "RESTORE TEMPORARY TABLE tmp FROM File('${B}/temporary') FORMAT Null"
restore 5 temporary/temporary_tables/metadata/tmp.sql "CREATE TABLE _temporary_and_external_tables.tmp (x UInt8) ENGINE = Memory" \
    "RESTORE TEMPORARY TABLE tmp FROM File('${B}/temporary') FORMAT Null"
restore 6 "" "" "RESTORE TABLE a.t AS b.t2 FROM File('${B}/table_honest') FORMAT Null"

# `RESTORE DATABASE a` must create `a`, otherwise it fails on the existing database for any definition.
local_restored "SELECT count() FROM system.tables WHERE database = 'a'; DROP DATABASE a"

restore 7 database/metadata/a.sql "CREATE DATABASE injected_7 ENGINE = Memory" \
    "RESTORE DATABASE a FROM File('${B}/database') FORMAT Null"
restore 8 database/metadata/a.sql "CREATE TABLE b.injected_8 (x UInt8) ENGINE = Memory" \
    "RESTORE DATABASE a FROM File('${B}/database') FORMAT Null"
restore 9 database/metadata/a.sql "CREATE TABLE a.injected_9 (x UInt8) ENGINE = Memory" \
    "RESTORE DATABASE a FROM File('${B}/database') FORMAT Null"

# Predefined tables and databases are never created from their definitions, so mismatched ones are still restorable.
PREDEFINED=${B}/predefined/metadata/information_schema/tables.sql
sed -i '1s/^CREATE VIEW information_schema\.tables /CREATE VIEW INFORMATION_SCHEMA.TABLES /' "${PREDEFINED}"
if ! grep -q '^CREATE VIEW INFORMATION_SCHEMA\.TABLES ' "${PREDEFINED}"; then
    echo "The definition of information_schema.tables was not edited" >&2
fi
PREDEFINED_DB=${B}/predefined/metadata/information_schema.sql
sed -i '1s/^CREATE DATABASE information_schema /CREATE DATABASE INFORMATION_SCHEMA /' "${PREDEFINED_DB}"
if ! grep -q '^CREATE DATABASE INFORMATION_SCHEMA ' "${PREDEFINED_DB}"; then
    echo "The definition of information_schema was not edited" >&2
fi
restore 10 "" "" "RESTORE DATABASE information_schema FROM File('${B}/predefined') FORMAT Null"

local_restored "
SELECT
    (SELECT count() FROM system.tables WHERE database = 'b' AND (name = 't' OR name LIKE 'injected%')),
    (SELECT count() FROM system.databases WHERE name LIKE 'injected%');
SELECT count() FROM system.tables WHERE database = 'b' AND name = 't2';
"

rm -rf "${WORK_DIR}"
