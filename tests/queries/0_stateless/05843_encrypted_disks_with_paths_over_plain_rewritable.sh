#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: an `encrypted` disk needs OpenSSL, which the fast test build leaves out

# An `encrypted` disk forwards the metadata type and the object storage of the disk it wraps. With `plain_rewritable`
# metadata the root of a disk is the common key prefix of the object storage, so the `path` of the `encrypted` disk
# has to be added to it: two `encrypted` disks with different `path` over one disk keep their files under different
# prefixes, and so does an `encrypted` disk with a `path` beside the disk it wraps. A storage policy holding them is
# valid. Without a `path`, the `encrypted` disk keeps its files where the wrapped disk does, and the policy is rejected.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -rf "${WORK_DIR}"' EXIT
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}"

DISKS=$(cat <<XML
            <base>
                <type>object_storage</type>
                <object_storage_type>local</object_storage_type>
                <metadata_type>plain_rewritable</metadata_type>
                <path>${WORK_DIR}/objects/</path>
            </base>
            <enc1>
                <type>encrypted</type>
                <disk>base</disk>
                <path>enc1/</path>
                <key>1234567812345678</key>
            </enc1>
            <enc2>
                <type>encrypted</type>
                <disk>base</disk>
                <path>enc2/</path>
                <key>8765432187654321</key>
            </enc2>
            <enc_no_path>
                <type>encrypted</type>
                <disk>base</disk>
                <key>1234567887654321</key>
            </enc_no_path>
XML
)

cat > "${WORK_DIR}/config.xml" <<XML
<clickhouse>
    <storage_configuration>
        <disks>
${DISKS}
        </disks>
        <policies>
            <two_encrypted>
                <volumes>
                    <first><disk>enc1</disk></first>
                    <second><disk>enc2</disk></second>
                </volumes>
            </two_encrypted>
            <encrypted_beside_base>
                <volumes>
                    <first><disk>enc1</disk></first>
                    <second><disk>base</disk></second>
                </volumes>
            </encrypted_beside_base>
        </policies>
    </storage_configuration>
</clickhouse>
XML

# A policy that fails to load makes every policy unavailable, so the invalid one has a configuration of its own.
cat > "${WORK_DIR}/rejected.xml" <<XML
<clickhouse>
    <storage_configuration>
        <disks>
${DISKS}
        </disks>
        <policies>
            <encrypted_without_path_beside_base>
                <volumes>
                    <first><disk>enc_no_path</disk></first>
                    <second><disk>base</disk></second>
                </volumes>
            </encrypted_without_path_beside_base>
        </policies>
    </storage_configuration>
</clickhouse>
XML

for policy in two_encrypted encrypted_beside_base
do
    echo "${policy}"
    ${CLICKHOUSE_LOCAL} --config-file "${WORK_DIR}/config.xml" --path "${WORK_DIR}/${policy}" -m -q "
        CREATE DATABASE db ENGINE = Atomic;
        CREATE TABLE db.t (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = '${policy}';
        INSERT INTO db.t VALUES (1), (2), (3);
        ALTER TABLE db.t MOVE PARTITION tuple() TO VOLUME 'second';
        INSERT INTO db.t VALUES (4);
        SELECT disk_name, sum(rows) FROM system.parts WHERE database = 'db' AND table = 't' AND active GROUP BY disk_name ORDER BY disk_name;
        DETACH TABLE db.t;
        ATTACH TABLE db.t;
        SELECT count(), sum(x) FROM db.t;
    "
done

echo "encrypted_without_path_beside_base"
${CLICKHOUSE_LOCAL} --config-file "${WORK_DIR}/rejected.xml" --path "${WORK_DIR}/rejected" -m -q "
    CREATE DATABASE db ENGINE = Atomic;
    CREATE TABLE db.t (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 'encrypted_without_path_beside_base';
" 2>&1 | grep -c -m1 "Disks \`enc_no_path\` and \`base\` in storage policy \`encrypted_without_path_beside_base\`"
