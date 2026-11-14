#!/usr/bin/env bash
# With check_exit_code = 1, a query that stops reading an executable's output early must still return.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/exec_check_exit_code_limit_XXXXXX")
trap 'rm -rf "${WORK_DIR}"' EXIT

cat > "${WORK_DIR}/endless.sh" <<'SCRIPT'
#!/usr/bin/env bash
exec yes 1
SCRIPT

# Replies to each request with its row count and one `x<TAB>request` row per input row.
cat > "${WORK_DIR}/echo_pool.sh" <<'SCRIPT'
#!/usr/bin/env bash
request=0
while IFS= read -r n; do
    request=$((request + 1))
    printf '%s\n' "${n}"
    for ((i = 0; i < n; i++)); do
        IFS= read -r x
        printf '%s\t%s\n' "${x}" "${request}"
    done
done
SCRIPT

cat > "${WORK_DIR}/fail.sh" <<'SCRIPT'
#!/usr/bin/env bash
echo 1
exit 1
SCRIPT

# Announces the whole reply, sends one row of it and exits.
cat > "${WORK_DIR}/crash_pool.sh" <<'SCRIPT'
#!/usr/bin/env bash
IFS= read -r n
for ((i = 0; i < n; i++)); do IFS= read -r x; done
printf '%s\n1\t1\n' "${n}"
exit 1
SCRIPT
chmod +x "${WORK_DIR}"/*.sh

cat > "${WORK_DIR}/config.xml" <<EOF
<clickhouse>
    <user_scripts_path>${WORK_DIR}/</user_scripts_path>
</clickhouse>
EOF

timeout 60 $CLICKHOUSE_LOCAL --config-file="${WORK_DIR}/config.xml" --query "
CREATE TABLE t_endless (x UInt64) ENGINE = Executable('endless.sh', 'TSV') SETTINGS check_exit_code = 1;
SELECT x FROM t_endless LIMIT 1;
" < /dev/null || echo "executable: rc=$?"

timeout 60 $CLICKHOUSE_LOCAL --config-file="${WORK_DIR}/config.xml" --query "
CREATE TABLE t_pool (x UInt64, request UInt64)
ENGINE = ExecutablePool('echo_pool.sh', 'TSV', (SELECT number FROM numbers(100)))
SETTINGS send_chunk_header = 1, pool_size = 1, check_exit_code = 1;
SELECT request FROM t_pool LIMIT 1 SETTINGS input_format_max_block_size_bytes = 16;
SELECT count(), any(request) FROM t_pool;
" < /dev/null || echo "pool: rc=$?"

# A non-zero exit is reported when the output is read to the end, not when the query stops reading early.
cat > "${WORK_DIR}/fail.sql" <<'EOF'
CREATE TABLE t_fail (x UInt64) ENGINE = Executable('fail.sh', 'TSV') SETTINGS check_exit_code = 1;
SELECT count() FROM t_fail; -- { serverError CHILD_WAS_NOT_EXITED_NORMALLY }
SELECT x FROM t_fail LIMIT 1;
CREATE TABLE t_crash (x UInt64, request UInt64)
ENGINE = ExecutablePool('crash_pool.sh', 'TSV', (SELECT number FROM numbers(3)))
SETTINGS send_chunk_header = 1, pool_size = 1, check_exit_code = 1;
SELECT count() FROM t_crash; -- { serverError CHILD_WAS_NOT_EXITED_NORMALLY }
SELECT request FROM t_crash LIMIT 1;
EOF
$CLICKHOUSE_LOCAL --config-file="${WORK_DIR}/config.xml" --queries-file="${WORK_DIR}/fail.sql" < /dev/null
