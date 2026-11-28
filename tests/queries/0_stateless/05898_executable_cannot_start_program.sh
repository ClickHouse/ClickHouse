#!/usr/bin/env bash
# A program that cannot be started (here: a missing `#!` interpreter) is an error regardless of `check_exit_code`.
# A program that started is still governed by `check_exit_code`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/exec_cannot_start_XXXXXX")
trap 'rm -rf "${WORK_DIR}"' EXIT

printf '#!/nonexistent/python3\nprint(1)\n' > "${WORK_DIR}/bad.py"
printf '#!/bin/sh\nexit 3\n' > "${WORK_DIR}/exit3.sh"
printf '#!/bin/sh\necho 1\nexit 3\n' > "${WORK_DIR}/exit3b.sh"
chmod +x "${WORK_DIR}"/*

cat > "${WORK_DIR}/config.xml" << EOF
<clickhouse>
    <user_scripts_path>${WORK_DIR}/</user_scripts_path>
</clickhouse>
EOF

cat > "${WORK_DIR}/queries.sql" << 'EOF'
SELECT * FROM executable('bad.py', TSV, 'x UInt64'); -- { serverError CANNOT_CREATE_CHILD_PROCESS }
SELECT * FROM executable('bad.py', TSV, 'x UInt64', SETTINGS check_exit_code = 1); -- { serverError CANNOT_CREATE_CHILD_PROCESS }
CREATE TABLE t_executable (x UInt64) ENGINE = Executable('bad.py', TSV);
SELECT count() FROM t_executable; -- { serverError CANNOT_CREATE_CHILD_PROCESS }
CREATE TABLE t_pool (x UInt64) ENGINE = ExecutablePool('bad.py', TSV) SETTINGS pool_size = 1, max_command_execution_time = 1;
SELECT count() FROM t_pool; -- { serverError CANNOT_CREATE_CHILD_PROCESS }
SELECT count() FROM t_pool; -- { serverError CANNOT_CREATE_CHILD_PROCESS }
SELECT * FROM executable('exit3.sh', TSV, 'x UInt64');
SELECT * FROM executable('exit3b.sh', TSV, 'x UInt64');
SELECT * FROM executable('exit3.sh', TSV, 'x UInt64', SETTINGS check_exit_code = 1); -- { serverError CHILD_WAS_NOT_EXITED_NORMALLY }
EOF

$CLICKHOUSE_LOCAL --config-file="${WORK_DIR}/config.xml" --queries-file="${WORK_DIR}/queries.sql" < /dev/null

# The error names the program and the reason.
$CLICKHOUSE_LOCAL --config-file="${WORK_DIR}/config.xml" --query "SELECT * FROM executable('bad.py', TSV, 'x UInt64')" < /dev/null 2>&1 \
    | sed -nE 's/.*(Cannot execute ).*\/(bad\.py).*(errno: 2, strerror: No such file or directory).*/\1\2 \3/p'
