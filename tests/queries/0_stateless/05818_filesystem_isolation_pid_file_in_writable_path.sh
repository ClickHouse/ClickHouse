#!/usr/bin/env bash
# Tags: no-fasttest
# The container of the fast test is not privileged, so it does not allow the namespaces of the isolation.

# The supervisor outside of the isolation creates and removes the pid file. If the server could change a directory on
# the way to the pid file, code inside could replace it with a symbolic link to a directory outside, and make the
# supervisor overwrite or remove a file there at the next start. Such a pid file stops the startup with `enforce`, and
# the server runs without the isolation with `best_effort`: one in a path writable inside, one behind a symbolic link in
# such a path, even if the link leads outside of it, one under another name of such a path (a bind mount on the host, the
# upper directory of an overlay), and the pid file itself made writable inside. A symbolic link on the way is followed
# only to a directory, and only in a directory that nobody but root can write, because the directory of the link can be
# writable inside under a name that is not known; `..` after it is the parent of its target. Where the
# server does start, the test does what such code would do, and restarts it, and the file outside stays as it is.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

EXPECTED="enforce, in path: refused
best_effort, in path: not isolated
enforce, through a link in path: refused
enforce, through a bind mount of path: refused
enforce, in a mount inside path: refused
enforce, pid file writable inside: refused
enforce, through the upper directory of an overlay of path: refused
enforce, in an overlay of path as its upper directory: refused
enforce, through the upper directory of an overlay of path given by a link: refused
enforce, through a link outside of path: isolated
enforce, through a link and .. to path: refused
enforce, through a link in a directory writable by others: refused
enforce, pid file is a link: refused
marker: unchanged"

# The configuration of the tests enables the isolation only where the container allows the namespaces: see
# `tests/config/install.sh`.
# The supervisor and the new PID namespace are used with `CAP_SYS_ADMIN` (bit 21), as the server started here has it.
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'filesystem_isolation'")" != "enforce" ] \
    || ! (( (16#$(awk '$1 == "CapEff:" { print $2 }' /proc/self/status) >> 21) & 1 )); then
    echo "$EXPECTED"
    exit 0
fi

DIR="${CLICKHOUSE_TMP}/filesystem_isolation_pid_file_in_writable_path"
rm -rf "$DIR"
mkdir -p "$DIR/conf" "$DIR/data/run" "$DIR/log" "$DIR/outside" "$DIR/outside_first" "$DIR/alias" "$DIR/pid_files" \
    "$DIR/overlay" "$DIR/merged" "$DIR/pid_directory" "$DIR/pid_bound" "$DIR/data/runtime"
DIR=$(cd "$DIR" && pwd -P)
# Only root can write the directories of the links, unless a case says otherwise.
chmod 755 "$DIR"
ln -s "$DIR/pid_directory" "$DIR/pid_link"
ln -s "$DIR/data/run" "$DIR/data_run_link"
mkdir -m 777 "$DIR/shared"
ln -s "$DIR/pid_directory" "$DIR/shared/pid_link"
ln -s "$DIR/outside/server.pid" "$DIR/pid_file_link"
echo 0 > "$DIR/pid_files/server.pid"
echo "outside" > "$DIR/outside/server.pid"
ln -s "$DIR/outside_first" "$DIR/data/link"

PID=
function finish()
{
    [ -n "$PID" ] && kill -9 "$PID" 2>/dev/null
    wait
    for mount_point in "$DIR/alias" "$DIR/data/runtime" "$DIR/merged" "$DIR/overlay"; do
        mountpoint -q "$mount_point" && umount "$mount_point"
    done
}
trap finish EXIT

# Starts the server, and prints `refused`, `not isolated`, `isolated` or `failed`.
function run()
{
    local mode=$1 pid_file=$2 action=$3 extra=${4:-} data=${5:-$DIR/data}
    cat > "$DIR/conf/config.xml" <<EOF
<clickhouse>
    <logger>
        <level>information</level>
        <log>$DIR/log/server.log</log>
        <errorlog>$DIR/log/server.err.log</errorlog>
    </logger>
    <path>$data/</path>
    <pid>$pid_file</pid>
    <!-- Any free port: nothing connects to this server. -->
    <tcp_port>0</tcp_port>
    <filesystem_isolation>$mode</filesystem_isolation>
    $extra
    <users><default><password></password><profile>default</profile><quota>default</quota></default></users>
    <profiles><default/></profiles>
    <quotas><default/></quotas>
</clickhouse>
EOF
    rm -f "$DIR/log/server.log" "$DIR/log/server.err.log"

    CLICKHOUSE_WATCHDOG_ENABLE=0 $CLICKHOUSE_BINARY server --config-file "$DIR/conf/config.xml" > "$DIR/server.out" 2>&1 &
    PID=$!
    for _ in {1..600}; do
        grep -q 'Ready for connections' "$DIR/log/server.log" 2>/dev/null && break
        kill -0 "$PID" 2>/dev/null || break
        sleep 0.1
    done

    if grep -q 'Cannot isolate the filesystem: the pid file' "$DIR/server.out" && ! kill -0 "$PID" 2>/dev/null; then
        echo refused
    elif grep -q 'cannot be isolated: the pid file' "$DIR/log/server.log" 2>/dev/null; then
        echo "not isolated"
    elif grep -q 'Isolated the filesystem' "$DIR/log/server.log" 2>/dev/null; then
        echo isolated
        # What code inside the server could do, as it can write `path`.
        $action
    else
        echo failed
        tail -n 5 "$DIR/server.out"
    fi
    kill "$PID" 2>/dev/null
    wait "$PID"
    PID=
}

function replace_directory() { mv "$DIR/data/run" "$DIR/data/run.moved" && ln -s "$DIR/outside" "$DIR/data/run"; }
function replace_link() { ln -sfn "$DIR/outside" "$DIR/data/link"; }

echo "enforce, in path: $(run enforce "$DIR/data/run/server.pid" replace_directory)"
echo "best_effort, in path: $(run best_effort "$DIR/data/run/server.pid" true)"
RESULT=$(run enforce "$DIR/data/link/server.pid" replace_link)
# The start after the change, if the first one was isolated.
[ "$RESULT" = isolated ] && RESULT="$RESULT, then $(run enforce "$DIR/data/link/server.pid" true)"
echo "enforce, through a link in path: $RESULT"

# The same directory as `path` under another name, as a bind mount on the host makes it.
# Undone, if an earlier case started and replaced the directory.
if [ -L "$DIR/data/run" ]; then
    rm -f "$DIR/data/run"
    mv "$DIR/data/run.moved" "$DIR/data/run"
fi
mount --bind "$DIR/data" "$DIR/alias"
RESULT=$(run enforce "$DIR/alias/run/server.pid" replace_directory)
[ "$RESULT" = isolated ] && RESULT="$RESULT, then $(run enforce "$DIR/alias/run/server.pid" true)"
echo "enforce, through a bind mount of path: $RESULT"
umount "$DIR/alias"

# A directory outside mounted inside `path` on the host: its files are writable inside through the recursive bind mount.
mount --bind "$DIR/pid_bound" "$DIR/data/runtime"
echo "enforce, in a mount inside path: $(run enforce "$DIR/pid_bound/server.pid" true)"
umount "$DIR/data/runtime"

echo "enforce, pid file writable inside: $(run enforce "$DIR/pid_files/server.pid" true \
    "<filesystem_isolation_paths><read_write>$DIR/pid_files/server.pid</read_write></filesystem_isolation_paths>")"

# `path` on an overlay, whose writes go to its upper directory, which has another name and another device on the host.
# On a `tmpfs`, which can be the upper directory of an overlay, unlike the overlay of a container.
mount -t tmpfs tmpfs "$DIR/overlay"
mkdir -p "$DIR/overlay/lower" "$DIR/overlay/upper/run" "$DIR/overlay/work"
mount -t overlay overlay -o "lowerdir=$DIR/overlay/lower,upperdir=$DIR/overlay/upper,workdir=$DIR/overlay/work" "$DIR/merged"
function replace_directory_in_overlay() { mv "$DIR/merged/run" "$DIR/merged/run.moved" && ln -s "$DIR/outside" "$DIR/merged/run"; }
RESULT=$(run enforce "$DIR/overlay/upper/run/server.pid" replace_directory_in_overlay "" "$DIR/merged")
[ "$RESULT" = isolated ] && RESULT="$RESULT, then $(run enforce "$DIR/overlay/upper/run/server.pid" true "" "$DIR/merged")"
echo "enforce, through the upper directory of an overlay of path: $RESULT"
# The reverse: `path` is the upper directory, and the pid file is in the overlay, whose files are stored there.
echo "enforce, in an overlay of path as its upper directory: $(run enforce "$DIR/merged/run/server.pid" true "" "$DIR/overlay/upper")"
umount "$DIR/merged"
# The same with the upper directory given to `mount` through a symbolic link, which its options keep.
ln -s "$DIR/overlay/upper" "$DIR/overlay/upper_link"
mount -t overlay overlay -o "lowerdir=$DIR/overlay/lower,upperdir=$DIR/overlay/upper_link,workdir=$DIR/overlay/work" "$DIR/merged"
echo "enforce, through the upper directory of an overlay of path given by a link: $(run enforce "$DIR/overlay/upper/run/server.pid" true "" "$DIR/merged")"
umount "$DIR/merged"
umount "$DIR/overlay"

echo "enforce, through a link outside of path: $(run enforce "$DIR/pid_link/server.pid" true)"
# `..` after the link is the parent of its target, `path`, not the directory of the link.
echo "enforce, through a link and .. to path: $(run enforce "$DIR/data_run_link/../server.pid" true)"
echo "enforce, through a link in a directory writable by others: $(run enforce "$DIR/shared/pid_link/server.pid" true)"
echo "enforce, pid file is a link: $(run enforce "$DIR/pid_file_link" true)"

echo "marker: $([ "$(cat "$DIR/outside/server.pid" 2>/dev/null)" = "outside" ] && echo unchanged || echo changed)"
