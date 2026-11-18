#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/executable_log_cloexec_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

cat > "${LOCAL_DIR}/holds_log_fd.py" <<'PYTHON'
#!/usr/bin/env python3
import errno
import os

log = os.stat(os.path.join(os.path.dirname(__file__), "local.log"))
for fd in os.listdir("/dev/fd"):
    try:
        st = os.fstat(int(fd))
    except OSError as error:
        if error.errno != errno.EBADF:
            raise
        continue
    if (st.st_dev, st.st_ino) == (log.st_dev, log.st_ino):
        print(1)
        break
else:
    print(0)
PYTHON
chmod +x "${LOCAL_DIR}/holds_log_fd.py"

cat > "${LOCAL_DIR}/config.xml" <<EOF
<clickhouse>
    <logger>
        <level>trace</level>
        <console>false</console>
        <log>${LOCAL_DIR}/local.log</log>
    </logger>
    <user_scripts_path>${LOCAL_DIR}</user_scripts_path>
</clickhouse>
EOF

# Assert that log file is created
$CLICKHOUSE_LOCAL --config-file="${LOCAL_DIR}/config.xml" --query "SELECT 1;" >/dev/null 2>&1

if test -s "${LOCAL_DIR}/local.log"; then
    echo 'Log is nonempty'
else
    echo 'Log is missing or empty'
fi

# Assert that UDF process doesn't hold log FD
$CLICKHOUSE_LOCAL --config-file="${LOCAL_DIR}/config.xml" --query "
    SELECT * FROM executable('holds_log_fd.py', TSV, 'result UInt8', SETTINGS check_exit_code = 1)
" 2>"${LOCAL_DIR}/stderr.log" || {
    status=$?
    cat "${LOCAL_DIR}/stderr.log" >&2
    exit "$status"
}

