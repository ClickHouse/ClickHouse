#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

WORK_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/exec_pool_unread_reply_XXXXXX")
trap 'rm -rf "${WORK_DIR}"' EXIT

cat > "${WORK_DIR}/echo_pool.sh" <<'SCRIPT'
#!/usr/bin/env bash
request=0
while IFS= read -r n; do
    request=$((request + 1))
    rows=()
    for ((i = 0; i < n; i++)); do
        IFS= read -r x
        if [[ "${x}" != 0 ]]; then
            rows+=("${x}")
        fi
    done
    printf '%s\n' "${#rows[@]}"
    for x in "${rows[@]}"; do
        printf '%s\t%s\n' "${x}" "${request}"
    done
done
SCRIPT
chmod +x "${WORK_DIR}/echo_pool.sh"

cat > "${WORK_DIR}/config.xml" <<EOF
<clickhouse>
    <user_scripts_path>${WORK_DIR}/</user_scripts_path>
</clickhouse>
EOF

for check_exit_code in 0 1; do
    $CLICKHOUSE_LOCAL --config-file="${WORK_DIR}/config.xml" --query "
CREATE TABLE src (x UInt64) ENGINE = Memory;
CREATE TABLE t_limit (x UInt64, request UInt64)
ENGINE = ExecutablePool('echo_pool.sh', 'TSV', (SELECT x FROM src))
SETTINGS send_chunk_header = 1, pool_size = 1, check_exit_code = ${check_exit_code};
CREATE TABLE t_where AS t_limit;

INSERT INTO src VALUES (1);
SELECT * FROM t_limit LIMIT 0;
SELECT * FROM t_where WHERE 1 = 0;

TRUNCATE TABLE src;
INSERT INTO src VALUES (2), (3);
SELECT '${check_exit_code}: after LIMIT 0', arraySort(groupArray(x)), any(request) FROM t_limit;
SELECT '${check_exit_code}: after WHERE 1 = 0', arraySort(groupArray(x)), any(request) FROM t_where;

TRUNCATE TABLE src;
INSERT INTO src VALUES (4);
SELECT '${check_exit_code}: reused limit worker', arraySort(groupArray(x)), any(request) FROM t_limit;
SELECT '${check_exit_code}: reused where worker', arraySort(groupArray(x)), any(request) FROM t_where;

CREATE TABLE t_empty AS t_limit;
TRUNCATE TABLE src;
INSERT INTO src VALUES (0);
SELECT '${check_exit_code}: empty reply', count() FROM t_empty;
TRUNCATE TABLE src;
INSERT INTO src VALUES (5);
SELECT '${check_exit_code}: reused empty worker', arraySort(groupArray(x)), any(request) FROM t_empty;
" < /dev/null
done
