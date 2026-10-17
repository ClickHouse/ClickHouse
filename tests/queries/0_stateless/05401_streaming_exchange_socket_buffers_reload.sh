#!/usr/bin/env bash
# Tags: no-parallel
# `SYSTEM RELOAD CONFIG` rewrites global server state, so this test cannot run beside other tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A file of our own, removed on exit, so a failed run does not leave the buffers capped for later tests.
config="${CLICKHOUSE_CONFIG_DIR}/config.d/${CLICKHOUSE_TEST_UNIQUE_NAME}.xml"

reload_config()
{
    # `listen_try` can report 'Address already in use' while the reload itself succeeds.
    $CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG" |& grep -v -e 'Address already in use' || :
}

cleanup()
{
    rm -f "$config"
    reload_config
}
trap cleanup EXIT

# `system.server_settings` and `getServerSetting` must both report the sizes published for the next
# exchange socket. Those are the values `applySocketBufferSizes` reads, including 0 (kernel autotuning).
show_settings()
{
    $CLICKHOUSE_CLIENT --query "
        SELECT name, value, changeable_without_restart
        FROM system.server_settings
        WHERE name IN ('streaming_exchange_sndbuf', 'streaming_exchange_rcvbuf')
        ORDER BY name"
    $CLICKHOUSE_CLIENT --query "
        SELECT
            toString(getServerSetting('streaming_exchange_sndbuf'))
                = (SELECT value FROM system.server_settings WHERE name = 'streaming_exchange_sndbuf'),
            toString(getServerSetting('streaming_exchange_rcvbuf'))
                = (SELECT value FROM system.server_settings WHERE name = 'streaming_exchange_rcvbuf')"
}

echo "default"
show_settings

cat > "$config" <<EOF
<clickhouse>
    <streaming_exchange_sndbuf>65536</streaming_exchange_sndbuf>
    <streaming_exchange_rcvbuf>131072</streaming_exchange_rcvbuf>
</clickhouse>
EOF
reload_config

echo "explicit"
show_settings

rm -f "$config"
reload_config

echo "restored"
show_settings
