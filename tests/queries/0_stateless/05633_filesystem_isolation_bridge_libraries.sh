#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: the container of the fast test is not privileged, so it does not allow the namespaces of the
#   isolation, and it has no `cc` to build the stand-in for the bridge.

# A server with `filesystem_isolation` keeps visible the dynamic loader and the shared libraries of
# `clickhouse-odbc-bridge` next to its executable, which it starts after the isolation, also the ones that the server
# itself does not map, and the ones that the dynamic loader finds through the `DT_RUNPATH` of the bridge. A program
# built here stands for the bridge: it needs a private library in `../lib`, found through `$ORIGIN/../lib`, and it is
# run by the isolated server as an executable user defined function.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXPECTED="libraries of the bridge: visible
result of the bridge: 42"

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ] \
    || ! command -v cc > /dev/null 2>&1; then
    echo "$EXPECTED"
    exit 0
fi

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_bridge_libraries"
rm -rf "$DIR"
mkdir -p "$DIR/bin" "$DIR/lib" "$DIR/src" "$DIR/conf" "$DIR/data" "$DIR/log"
DIR=$(cd "$DIR" && pwd -P)

# The server looks for the bridge in the directory of its executable as it is named in `argv[0]`.
ln -s "$(command -v "$CLICKHOUSE_BINARY")" "$DIR/bin/clickhouse-server"

cat > "$DIR/src/private.c" <<'EOF'
int private_value(void) { return 42; }
EOF
cat > "$DIR/src/bridge.c" <<'EOF'
#include <stdio.h>
int private_value(void);
int main(void)
{
    char line[256];
    while (fgets(line, sizeof(line), stdin))
        printf("%d\n", private_value());
    return 0;
}
EOF
cc -shared -fPIC -o "$DIR/lib/libprivate.so" "$DIR/src/private.c"
# shellcheck disable=SC2016
cc -o "$DIR/bin/clickhouse-odbc-bridge" "$DIR/src/bridge.c" -L"$DIR/lib" -lprivate -Wl,--enable-new-dtags -Wl,-rpath,'$ORIGIN/../lib'

# The loader and the libraries of the stand-in, without their links.
LIBRARIES=$(ldd "$DIR/bin/clickhouse-odbc-bridge" | grep -o -E '/[^ ]+' | xargs -n1 readlink -f | sort -u)

# The stand-in is also the program of a function, so that the isolated server runs it.
cat > "$DIR/conf/bridge_function.xml" <<EOF
<functions>
    <function>
        <type>executable</type>
        <name>test_filesystem_isolation_bridge</name>
        <return_type>UInt64</return_type>
        <argument><type>UInt64</type></argument>
        <format>TabSeparated</format>
        <command>clickhouse-odbc-bridge</command>
        <execute_direct>1</execute_direct>
    </function>
</functions>
EOF

cat > "$DIR/conf/config.xml" <<EOF
<clickhouse>
    <logger>
        <level>information</level>
        <log>$DIR/log/server.log</log>
        <errorlog>$DIR/log/server.err.log</errorlog>
    </logger>
    <path>$DIR/data/</path>
    <user_scripts_path>$DIR/bin/</user_scripts_path>
    <user_defined_executable_functions_config>$DIR/conf/*_function.xml</user_defined_executable_functions_config>
    <startup_scripts>
        <scripts>
            <query>INSERT INTO FUNCTION file('bridge_result.tsv', 'TSV', 'x UInt64') SELECT test_filesystem_isolation_bridge(1)</query>
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

"$DIR/bin/clickhouse-server" --config-file "$DIR/conf/config.xml" > "$DIR/server.out" 2>&1 &
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

MISSING=
for library in $LIBRARIES; do
    [ -e "/proc/$PID/root$library" ] || MISSING="$MISSING $library"
done
if [ -z "$MISSING" ]; then
    echo "libraries of the bridge: visible"
else
    echo "libraries of the bridge: missing$MISSING"
fi

echo "result of the bridge: $(cat "$DIR/data/user_files/bridge_result.tsv" 2>/dev/null || grep -m1 'test_filesystem_isolation_bridge' "$DIR/log/server.err.log")"
