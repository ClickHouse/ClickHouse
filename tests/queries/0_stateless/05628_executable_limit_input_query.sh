#!/usr/bin/env bash
# A query that stops reading early from an executable fed by an input query larger than the pipe buffers must return its rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/exec_limit_input_query_XXXXXX")
trap 'rm -rf "${WORK_DIR}"' EXIT

cat > "${WORK_DIR}/cat.sh" <<'SCRIPT'
#!/usr/bin/env bash
exec cat
SCRIPT

# Replies to each request with its row count and the rows themselves.
cat > "${WORK_DIR}/echo_pool.sh" <<'SCRIPT'
#!/usr/bin/env bash
while IFS= read -r n; do
    printf '%s\n' "${n}"
    for ((i = 0; i < n; i++)); do
        IFS= read -r x
        printf '%s\n' "${x}"
    done
done
SCRIPT
chmod +x "${WORK_DIR}"/*.sh

cat > "${WORK_DIR}/config.xml" <<EOF
<clickhouse>
    <user_scripts_path>${WORK_DIR}/</user_scripts_path>
</clickhouse>
EOF

$CLICKHOUSE_LOCAL --config-file="${WORK_DIR}/config.xml" --query "
CREATE TABLE t (x UInt64) ENGINE = Executable('cat.sh', 'TSV', (SELECT number FROM numbers(1000000))) SETTINGS command_write_timeout = 1000;
SELECT x FROM t LIMIT 1;
" < /dev/null || echo "executable: rc=$?"

$CLICKHOUSE_LOCAL --config-file="${WORK_DIR}/config.xml" --query "
CREATE TABLE t_pool (x UInt64)
ENGINE = ExecutablePool('echo_pool.sh', 'TSV', (SELECT number FROM numbers(100000)))
SETTINGS send_chunk_header = 1, pool_size = 1, command_write_timeout = 1000;
SELECT x FROM t_pool LIMIT 1 SETTINGS input_format_max_block_size_bytes = 16;
SELECT count() FROM t_pool;
" < /dev/null || echo "pool: rc=$?"
