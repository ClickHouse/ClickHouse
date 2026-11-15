#!/usr/bin/env bash
# Tags: no-fasttest
# The container of the fast test is not privileged, so it does not allow the namespaces of the isolation.

# A server with `filesystem_isolation` starts while another process writes through one of its new mounts, which it
# reaches through `/proc/<pid>/root`. Making that mount read-only waits for the writer instead of failing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

STARTS=3
EXPECTED=$(for start in $(seq 1 $STARTS); do echo "start $start: isolated"; done)

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ]; then
    echo "$EXPECTED"
    exit 0
fi

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_remount_busy"
rm -rf "$DIR"
mkdir -p "$DIR/conf" "$DIR/log" "$DIR/data" "$DIR/work" "$DIR/read_only"
touch "$DIR/read_only/file"
mkfifo "$DIR/ready"

cat > "$DIR/conf/config.xml" <<EOF
<clickhouse>
    <logger>
        <level>information</level>
        <log>$DIR/log/server.log</log>
        <errorlog>$DIR/log/server.err.log</errorlog>
    </logger>
    <path>$DIR/data/</path>
    <!-- Any free port: nothing connects to this server. -->
    <tcp_port>0</tcp_port>
    <filesystem_isolation>enforce</filesystem_isolation>
    <filesystem_isolation_paths><read_only>$DIR/read_only</read_only></filesystem_isolation_paths>
    <users><default><password></password><profile>default</profile><quota>default</quota></default></users>
    <profiles><default/></profiles>
    <quotas><default/></quotas>
</clickhouse>
EOF

PID=
WRITER=
function finish()
{
    [ -n "$WRITER" ] && kill "$WRITER" 2>/dev/null
    [ -n "$PID" ] && kill "$PID" 2>/dev/null
    wait
}
trap finish EXIT

for start in $(seq 1 $STARTS); do
    rm -f "$DIR/log/server.log"
    (cd "$DIR/work" && read -r _ < "$DIR/ready" && exec $CLICKHOUSE_BINARY server --config-file "$DIR/conf/config.xml") > "$DIR/server.out" 2>&1 &
    PID=$!

    # Opens the file for writing through the root of the server until it is read-only there, and keeps it open for a
    # moment when it is reached through the server's own mount of its directory. The server starts only once the
    # writer is ready, and the writer fails if it never sees the file read-only.
    python3 - "$PID" "$DIR/read_only/file" "$DIR/ready" <<'EOF' &
import errno, os, sys, time
pid, path = sys.argv[1], os.path.realpath(sys.argv[2])
directory = os.path.dirname(path)
def mount_id(fd):
    with open(f"/proc/self/fdinfo/{fd}") as f:
        return next(line.split()[1] for line in f if line.startswith("mnt_id:"))
def is_bind_of_server(mid):
    try:
        with open(f"/proc/{pid}/mountinfo") as f:
            return any(line.split()[0] == mid and line.split()[4] == directory for line in f)
    except OSError:
        return False
fd = os.open(path, os.O_RDONLY)
own = mount_id(fd)
os.close(fd)
with open(sys.argv[3], "w") as ready:
    ready.write("\n")
bind = {}
deadline = time.monotonic() + 60
while time.monotonic() < deadline and os.path.exists(f"/proc/{pid}"):
    try:
        fd = os.open(f"/proc/{pid}/root{path}", os.O_WRONLY)
    except OSError as e:
        if e.errno == errno.EROFS:
            sys.exit(0)
        continue
    mid = mount_id(fd)
    if mid != own and mid not in bind:
        bind[mid] = is_bind_of_server(mid)
    held = mid != own and bind[mid]
    if held:
        time.sleep(0.005)
    os.close(fd)
    if held:
        time.sleep(0.005)
sys.exit(1)
EOF
    WRITER=$!

    for _ in {1..600}; do
        grep -q 'Ready for connections' "$DIR/log/server.log" 2>/dev/null && break
        kill -0 "$PID" 2>/dev/null || break
        sleep 0.1
    done

    if grep -q 'Ready for connections' "$DIR/log/server.log" 2>/dev/null && grep -q 'Isolated the filesystem' "$DIR/log/server.log"; then
        echo "start $start: isolated"
    else
        echo "start $start: the server did not start with an isolated filesystem"
        cat "$DIR/server.out" "$DIR/log/server.err.log" >&2
        exit 1
    fi

    if ! wait "$WRITER"; then
        echo "start $start: the file did not become read-only through the root of the server"
        exit 1
    fi
    WRITER=
    kill "$PID"
    wait "$PID"
    PID=
done
