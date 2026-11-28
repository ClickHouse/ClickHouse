#!/usr/bin/env bash
# Tags: no-fasttest
# The container of the fast test is not privileged, so it does not allow the namespaces of the isolation.

# A server with `filesystem_isolation` runs an executable user defined function created with a driver, when
# `dynamic_user_defined_executable_functions_path`, where the driver generates the program of the function, is declared
# in `filesystem_isolation_paths` as `read_write_executable`. Without it, the directory is `noexec` and the driver is
# refused (see `05635_filesystem_isolation_udf_drivers`). The queries run as startup scripts of the server, and the
# result of the function is written to a file.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXPECTED="started with an isolated filesystem
the dynamic directory is read-write and executable: yes
result: 42"

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ]; then
    echo "$EXPECTED"
    exit 0
fi

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_udf_drivers_executable_path"
rm -rf "$DIR"
mkdir -p "$DIR/conf" "$DIR/data" "$DIR/log" "$DIR/dynamic"
DIR=$(cd "$DIR" && pwd -P)

# The driver runs in the working directory of the function, and generates there a program that returns its input.
cat > "$DIR/conf/create.sh" <<'EOF'
#!/bin/sh
name=
while [ $# -gt 0 ]; do
    if [ "$1" = "--name" ]; then
        name=$2
    fi
    shift
done
cat > /dev/null
printf '#!/bin/sh\nexec cat\n' > user_func
chmod +x user_func
echo "<functions><function><type>executable</type><name>$name</name><return_type>UInt64</return_type><argument><type>UInt64</type></argument><format>TabSeparated</format><command>user_func</command></function></functions>"
EOF
chmod +x "$DIR/conf/create.sh"

cat > "$DIR/conf/test_driver.xml" <<EOF
<clickhouse>
    <driver>
        <name>TestDriver</name>
        <create_command>$DIR/conf/create.sh</create_command>
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
            <query>CREATE FUNCTION test_filesystem_isolation_driver_executable ARGUMENTS (x UInt64) RETURNS UInt64 ENGINE = TestDriver() AS 'x'</query>
        </scripts>
        <scripts>
            <query>INSERT INTO FUNCTION file('result.tsv', TSV) SELECT test_filesystem_isolation_driver_executable(42)</query>
        </scripts>
    </startup_scripts>
    <!-- Any free port: nothing connects to this server. -->
    <tcp_port>0</tcp_port>
    <filesystem_isolation>enforce</filesystem_isolation>
    <filesystem_isolation_paths>
        <!-- The shell, \`env\` that starts the driver, \`cat\`, and the libraries they need. -->
        <read_only>/bin</read_only>
        <read_only>/usr/bin</read_only>
        <read_only>/lib</read_only>
        <read_only>/lib64</read_only>
        <read_only>/usr/lib</read_only>
        <read_write_executable>$DIR/dynamic</read_write_executable>
    </filesystem_isolation_paths>
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

echo "the dynamic directory is read-write and executable: $(grep -qF "$DIR/dynamic (read-write, executable," "$DIR/log/server.log" && echo yes || echo no)"

# The startup scripts can run after the server is ready, as with a slow sanitizer.
RESULT="$DIR/data/user_files/result.tsv"
for _ in {1..600}; do
    [ -s "$RESULT" ] && break
    sleep 0.1
done
if [ -s "$RESULT" ]; then
    echo "result: $(cat "$RESULT")"
else
    echo "no result"
    cat "$DIR/log/server.err.log" >&2
fi

kill "$PID"
wait "$PID"
PID=
