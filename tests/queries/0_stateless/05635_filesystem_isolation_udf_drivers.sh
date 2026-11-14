#!/usr/bin/env bash
# Tags: no-fasttest
# The container of the fast test is not privileged, so it does not allow the namespaces of the isolation.

# A server with `filesystem_isolation` refuses to create an executable user defined function with a driver, rather
# than to create a function that cannot run: the program that the driver generates is in
# `dynamic_user_defined_executable_functions_path`, which is writable and therefore not executable in the isolated
# filesystem. The refusal comes before the driver runs. The query runs as a startup script of the server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXPECTED="started with an isolated filesystem
driver refused: yes
driver ran: no"

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ]; then
    echo "$EXPECTED"
    exit 0
fi

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_udf_drivers"
rm -rf "$DIR"
mkdir -p "$DIR/conf" "$DIR/data" "$DIR/log" "$DIR/dynamic"
DIR=$(cd "$DIR" && pwd -P)

# The driver would leave a mark if it ran.
cat > "$DIR/conf/create.sh" <<EOF
#!/bin/sh
touch "$DIR/dynamic/driver_ran"
EOF
chmod +x "$DIR/conf/create.sh"

cat > "$DIR/conf/test_driver.xml" <<EOF
<clickhouse>
    <driver>
        <name>TestDriver</name>
        <create_command>$DIR/conf/create.sh</create_command>
        <drop_command>$DIR/conf/create.sh</drop_command>
    </driver>
</clickhouse>
EOF

cat > "$DIR/conf/config.xml" <<EOF
<clickhouse>
    <logger>
        <level>information</level>
        <log>$DIR/log/server.log</log>
        <errorlog>$DIR/log/server.err.log</errorlog>
    </logger>
    <path>$DIR/data/</path>
    <allow_experimental_executable_udf_drivers>1</allow_experimental_executable_udf_drivers>
    <user_defined_executable_function_drivers_config>$DIR/conf/*_driver.xml</user_defined_executable_function_drivers_config>
    <dynamic_user_defined_executable_functions_path>$DIR/dynamic/</dynamic_user_defined_executable_functions_path>
    <startup_scripts>
        <scripts>
            <query>CREATE FUNCTION test_filesystem_isolation_driver ARGUMENTS (x UInt64) RETURNS UInt64 ENGINE = TestDriver() AS 'x'</query>
        </scripts>
    </startup_scripts>
    <!-- Any free port: nothing connects to this server. -->
    <tcp_port>0</tcp_port>
    <filesystem_isolation>enforce</filesystem_isolation>
    <users><default><password></password><profile>default</profile><quota>default</quota></default></users>
    <profiles><default/></profiles>
    <quotas><default/></quotas>
</clickhouse>
EOF

PID=
function finish()
{
    [ -n "$PID" ] && kill "$PID" 2>/dev/null
    wait
}
trap finish EXIT

$CLICKHOUSE_BINARY server --config-file "$DIR/conf/config.xml" > "$DIR/server.out" 2>&1 &
PID=$!

for _ in {1..600}; do
    grep -q 'Ready for connections' "$DIR/log/server.log" 2>/dev/null && break
    kill -0 "$PID" 2>/dev/null || break
    sleep 0.1
done

if ! grep -q 'Ready for connections' "$DIR/log/server.log" 2>/dev/null || ! grep -q 'Isolated the filesystem' "$DIR/log/server.log"; then
    echo "The server did not start with an isolated filesystem"
    cat "$DIR/server.out" "$DIR/log/server.err.log" >&2
    exit 1
fi
echo "started with an isolated filesystem"

echo "driver refused: $(grep -q 'are not supported with the server setting `filesystem_isolation`' "$DIR/log/server.err.log" && echo yes || echo no)"
echo "driver ran: $([ -e "$DIR/dynamic/driver_ran" ] && echo yes || echo no)"
