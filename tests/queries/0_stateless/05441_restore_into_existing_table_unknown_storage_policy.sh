#!/usr/bin/env bash
# `RESTORE` into an existing table must not resolve the storage policy named in the backup: nothing is
# created for it. Two configurations, because only the source defines `extra_policy`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -rf "${WORK_DIR}"' EXIT
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}/backups"

cat > "${WORK_DIR}/source.xml" <<XML
<clickhouse>
    <storage_configuration>
        <disks>
            <extra><type>local</type><path>${WORK_DIR}/extra/</path></extra>
        </disks>
        <policies>
            <extra_policy><volumes><main><disk>extra</disk></main></volumes></extra_policy>
        </policies>
    </storage_configuration>
    <backups><allowed_path>${WORK_DIR}/backups</allowed_path></backups>
</clickhouse>
XML

cat > "${WORK_DIR}/target.xml" <<XML
<clickhouse>
    <backups><allowed_path>${WORK_DIR}/backups</allowed_path></backups>
</clickhouse>
XML

${CLICKHOUSE_LOCAL} --config-file "${WORK_DIR}/source.xml" --path "${WORK_DIR}/source" -m -q "
    CREATE DATABASE db ENGINE = Atomic;
    CREATE TABLE db.t (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 'extra_policy';
    INSERT INTO db.t VALUES (1), (2), (3);
    BACKUP TABLE db.t TO File('${WORK_DIR}/backups/b') FORMAT Null;
"

echo "restore into an existing table whose policy this host does not configure"
${CLICKHOUSE_LOCAL} --config-file "${WORK_DIR}/target.xml" --path "${WORK_DIR}/target" -m -q "
    CREATE DATABASE db ENGINE = Atomic;
    CREATE TABLE db.t (x UInt64) ENGINE = MergeTree ORDER BY x;
    RESTORE TABLE db.t FROM File('${WORK_DIR}/backups/b') SETTINGS allow_different_table_def = 1 FORMAT Null;
    SELECT count(), sum(x) FROM db.t;
"

echo "creating the table still reports the unknown policy"
${CLICKHOUSE_LOCAL} --config-file "${WORK_DIR}/target.xml" --path "${WORK_DIR}/target_create" -m -q "
    CREATE DATABASE db ENGINE = Atomic;
    RESTORE TABLE db.t FROM File('${WORK_DIR}/backups/b') FORMAT Null;
" 2>&1 | grep -c -m1 "UNKNOWN_POLICY"
