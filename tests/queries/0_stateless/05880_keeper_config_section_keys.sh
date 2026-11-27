#!/usr/bin/env bash

# Which keys the `<zookeeper>` config section accepts, and what it rejects.
#
# `zkutil::ZooKeeperArgs::initFromKeeperSection` walks the keys of the section one by one and throws
# on anything it does not recognise, so an unknown key is a configuration error rather than a silently
# ignored line. The section is documented in
# docs/reference/settings/server-settings/settings/zookeeper.mdx (`## zookeeper`); the derived server
# setting this test reads back is documented in
# docs/reference/settings/server-settings/settings/keeper.mdx (`## keeper_hosts`).
#
# `system.server_settings` builds the args from the live `<zookeeper>` config every time it is read,
# to report `keeper_hosts` (`collectChangeableServerSettings`, src/Core/ServerSettings.cpp). Only the
# config is parsed: no session is opened, so `clickhouse-local` with a config file of its own is
# enough and the test needs no [Zoo]Keeper. The endpoints below are never connected to.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QUERY="SELECT value FROM system.server_settings WHERE name = 'keeper_hosts'"

# Both helpers take the label of the case and read the config from standard input.

# An accepted section: print the endpoint list it decodes to. Standard error is deliberately not
# redirected, so a section that stops being accepted fails the test instead of printing nothing.
zk_accepts()
{
    local cfg="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_$1.xml"
    cat > "$cfg"
    echo "$1: $(${CLICKHOUSE_LOCAL} --config-file="$cfg" --query "$QUERY")"
    rm -f "$cfg"
}

# A rejected section: print the name of the error code only. The message text is not asserted.
zk_rejects()
{
    local cfg="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_$1.xml"
    cat > "$cfg"
    echo "$1: $(${CLICKHOUSE_LOCAL} --config-file="$cfg" --query "$QUERY" 2>&1 >/dev/null \
        | grep -o -m1 KEEPER_EXCEPTION)"
    rm -f "$cfg"
}

# Every optional key of the section at once, so one accepted result covers them all: the timeouts, the
# four fault injection knobs, the session path, the availability zone switch, a password of exactly the
# 16 characters the limit allows, and two ACL entries whose permission lists name every permission
# word and the `all` shorthand. `<secure>` turns the second endpoint into a `secure://` one, which is
# how the decoded list tells the two endpoints apart.
zk_accepts accepted <<'EOF'
<clickhouse>
    <zookeeper>
        <node><host>localhost</host><port>2181</port></node>
        <node><host>localhost</host><port>2281</port><secure>1</secure></node>
        <session_timeout_ms>30000</session_timeout_ms>
        <operation_timeout_ms>10000</operation_timeout_ms>
        <send_sleep_probability>0.5</send_sleep_probability>
        <recv_sleep_probability>0.25</recv_sleep_probability>
        <send_sleep_ms>7</send_sleep_ms>
        <recv_sleep_ms>11</recv_sleep_ms>
        <sessions_path>/clickhouse/sessions</sessions_path>
        <availability_zone_autodetect>false</availability_zone_autodetect>
        <password>0123456789abcdef</password>
        <path_acls>
            <named>
                <path>/named</path><scheme>world</scheme><id>anyone</id>
                <permissions>read,write,create,delete,admin</permissions>
            </named>
            <inherited>
                <path>/inherited</path><scheme>world</scheme><id>anyone</id>
                <permissions>all</permissions>
                <apply_to_children>true</apply_to_children>
            </inherited>
        </path_acls>
    </zookeeper>
</clickhouse>
EOF

# A key the section does not know is rejected rather than ignored.
zk_rejects unknown_key <<'EOF'
<clickhouse>
    <zookeeper>
        <node><host>localhost</host><port>2181</port></node>
        <no_such_key>1</no_such_key>
    </zookeeper>
</clickhouse>
EOF

# A permission word outside the list above is rejected, even next to valid ones.
zk_rejects unknown_permission <<'EOF'
<clickhouse>
    <zookeeper>
        <node><host>localhost</host><port>2181</port></node>
        <path_acls>
            <wrong>
                <path>/wrong</path><scheme>world</scheme><id>anyone</id>
                <permissions>read,bogus</permissions>
            </wrong>
        </path_acls>
    </zookeeper>
</clickhouse>
EOF

# One character past the limit the accepted section sits on.
zk_rejects password_too_long <<'EOF'
<clickhouse>
    <zookeeper>
        <node><host>localhost</host><port>2181</port></node>
        <password>0123456789abcdefg</password>
    </zookeeper>
</clickhouse>
EOF
