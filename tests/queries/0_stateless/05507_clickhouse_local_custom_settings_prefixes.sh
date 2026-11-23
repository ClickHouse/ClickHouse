#!/usr/bin/env bash
# `clickhouse-local` applies `custom_settings_prefixes` of its config before it loads the users config,
# so a settings profile there may set a custom setting, as on the server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${DIR}"

cat <<EOF >"${DIR}/config.xml"
<clickhouse>
    <custom_settings_prefixes>custom_</custom_settings_prefixes>
    <user_directories>
        <users_xml>
            <path>users.xml</path>
        </users_xml>
    </user_directories>
</clickhouse>
EOF

cat <<EOF >"${DIR}/users.xml"
<clickhouse>
    <profiles>
        <default>
            <custom_local_marker>'profile'</custom_local_marker>
        </default>
    </profiles>
    <users>
        <default>
            <password></password>
            <networks>
                <ip>::/0</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
        </default>
    </users>
    <quotas>
        <default></default>
    </quotas>
</clickhouse>
EOF

${CLICKHOUSE_LOCAL} --config-file "${DIR}/config.xml" --query "SELECT getSetting('custom_local_marker')"

rm -rf "${DIR}"
