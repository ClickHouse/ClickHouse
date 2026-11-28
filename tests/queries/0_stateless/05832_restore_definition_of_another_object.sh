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

# One backup per case, named by the case number.
${CLICKHOUSE_LOCAL} --config-file "${CONFIG}" --path "${WORK_DIR}/data" -q "
CREATE DATABASE a;
CREATE TABLE a.t (x UInt8) ENGINE = Memory;
CREATE TEMPORARY TABLE tmp (x UInt8) ENGINE = Memory;
BACKUP TABLE a.t TO File('${B}/1') FORMAT Null;
BACKUP TABLE a.t TO File('${B}/2') FORMAT Null;
BACKUP TABLE a.t TO File('${B}/3') FORMAT Null;
BACKUP TEMPORARY TABLE tmp TO File('${B}/4') FORMAT Null;
BACKUP TEMPORARY TABLE tmp TO File('${B}/5') FORMAT Null;
BACKUP TABLE a.t TO File('${B}/6') FORMAT Null;
BACKUP DATABASE a TO File('${B}/7') FORMAT Null;
BACKUP DATABASE a TO File('${B}/8') FORMAT Null;
BACKUP DATABASE a TO File('${B}/9') FORMAT Null;
BACKUP DATABASE information_schema TO File('${B}/10') FORMAT Null;
"

function edit()
{
    printf '%s' "$2" > "${B}/$1"
}

edit 1/metadata/a/t.sql "CREATE TABLE b.t (x UInt8) ENGINE = Memory"
edit 2/metadata/a/t.sql "CREATE TABLE a.injected_2 (x UInt8) ENGINE = Memory"
edit 3/metadata/a/t.sql "CREATE DATABASE injected_3 ENGINE = Memory"
edit 4/temporary_tables/metadata/tmp.sql "CREATE TABLE b.injected_4 (x UInt8) ENGINE = Memory"
edit 5/temporary_tables/metadata/tmp.sql "CREATE TABLE _temporary_and_external_tables.tmp (x UInt8) ENGINE = Memory"
edit 7/metadata/a.sql "CREATE DATABASE injected_7 ENGINE = Memory"
edit 8/metadata/a.sql "CREATE TABLE b.injected_8 (x UInt8) ENGINE = Memory"
edit 9/metadata/a.sql "CREATE TABLE a.injected_9 (x UInt8) ENGINE = Memory"

# Predefined tables and databases are never created from their definitions, so mismatched ones are still restorable.
PREDEFINED=${B}/10/metadata/information_schema/tables.sql
sed -i '1s/^CREATE VIEW information_schema\.tables /CREATE VIEW INFORMATION_SCHEMA.TABLES /' "${PREDEFINED}"
if ! grep -q '^CREATE VIEW INFORMATION_SCHEMA\.TABLES ' "${PREDEFINED}"; then
    echo "The definition of information_schema.tables was not edited" >&2
fi
PREDEFINED_DB=${B}/10/metadata/information_schema.sql
sed -i '1s/^CREATE DATABASE information_schema /CREATE DATABASE INFORMATION_SCHEMA /' "${PREDEFINED_DB}"
if ! grep -q '^CREATE DATABASE INFORMATION_SCHEMA ' "${PREDEFINED_DB}"; then
    echo "The definition of information_schema was not edited" >&2
fi

# All restores run in one process: `--ignore-error` continues after a refused one, and `system.backups` has the
# outcome of each. `RESTORE DATABASE a` must create `a`, otherwise it fails on the existing database for any
# definition. The last line counts the failed statements, which must be exactly the refused restores.
${CLICKHOUSE_LOCAL} --config-file "${CONFIG}" --path "${WORK_DIR}/restored" --ignore-error -q "
CREATE DATABASE a;
CREATE DATABASE b;
RESTORE TABLE a.t FROM File('${B}/1') SETTINGS id = '1' FORMAT Null;
RESTORE TABLE a.t FROM File('${B}/2') SETTINGS id = '2' FORMAT Null;
RESTORE TABLE a.t FROM File('${B}/3') SETTINGS id = '3' FORMAT Null;
RESTORE TEMPORARY TABLE tmp FROM File('${B}/4') SETTINGS id = '4' FORMAT Null;
RESTORE TEMPORARY TABLE tmp FROM File('${B}/5') SETTINGS id = '5' FORMAT Null;
RESTORE TABLE a.t AS b.t2 FROM File('${B}/6') SETTINGS id = '6' FORMAT Null;
SELECT count() FROM system.tables WHERE database = 'a';
DROP DATABASE a;
RESTORE DATABASE a FROM File('${B}/7') SETTINGS id = '7' FORMAT Null;
RESTORE DATABASE a FROM File('${B}/8') SETTINGS id = '8' FORMAT Null;
RESTORE DATABASE a FROM File('${B}/9') SETTINGS id = '9' FORMAT Null;
RESTORE DATABASE information_schema FROM File('${B}/10') SETTINGS id = '10' FORMAT Null;
SELECT id, if(status = 'RESTORED', 'OK', extract(error, '[(]((CANNOT_RESTORE|UNKNOWN)_[A-Z_]+)[)]'))
FROM system.backups
ORDER BY toUInt32(id);
SELECT
    (SELECT count() FROM system.tables WHERE database = 'b' AND (name = 't' OR name LIKE 'injected%')),
    (SELECT count() FROM system.databases WHERE name LIKE 'injected%');
SELECT count() FROM system.tables WHERE database = 'b' AND name = 't2';
" 2> "${WORK_DIR}/stderr"
grep -c '^Received exception' "${WORK_DIR}/stderr"

rm -rf "${WORK_DIR}"
