#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# With `--daemon`, an error during the startup of the server must be shown to the user, and the process that was
# started must exit with a non-zero code, instead of exiting with zero before the daemon has even read its config.
# None of the cases starts a server: each fails during the initialization.

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_05896"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

function run_daemon()
{
    # The output goes to a pipe: the command must finish when the daemon has failed, with nothing holding the pipe.
    timeout 120 "$CLICKHOUSE_BINARY" server --daemon --config-file="$1" 2>&1 | cat > "$work/output"
    local code=${PIPESTATUS[0]}
    echo "exit code is non-zero: $(( code != 0 )), timed out: $(( code == 124 ))"
    if grep -q -F "$2" "$work/output"; then
        echo "the error is shown"
    else
        cat "$work/output"
    fi
    grep -q -F "The daemon process exited with code" "$work/output" && echo "the exit of the daemon is reported"
}

echo "empty config"
: > "$work/empty.xml"
run_daemon "$work/empty.xml" "No element found"

echo "stderr log is not writable"
cat > "$work/unwritable_stderr.xml" <<EOF
<clickhouse>
    <logger>
        <stderr>$work/no_such_directory/stderr.log</stderr>
    </logger>
</clickhouse>
EOF
run_daemon "$work/unwritable_stderr.xml" "(logger.stderr) is not writable"
