#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Query parameters in members that are not AST children: substituted for the client, discovered for an HTTP handler.

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
DB="${CLICKHOUSE_DATABASE}"
P="/hqp_${DB}"
HCHECK="hqp_check_${DB}"
HDISK="hqp_disk_${DB}"
HSHOW="hqp_show_${DB}"
HCOLS="hqp_cols_${DB}"
HIDX="hqp_idx_${DB}"

cleanup() {
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`$HCHECK\`; DROP HANDLER IF EXISTS \`$HDISK\`; DROP HANDLER IF EXISTS \`$HSHOW\`; DROP HANDLER IF EXISTS \`$HCOLS\`; DROP HANDLER IF EXISTS \`$HIDX\`"
}
trap cleanup EXIT
cleanup

# BACKUP to Memory and its RESTORE must share a session.
$CLICKHOUSE_CLIENT --param_col=b --param_p=1 -q "
CREATE TABLE t_dedup (a UInt8, b UInt8, c String) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_dedup VALUES (1, 1, 'x');
INSERT INTO t_dedup VALUES (1, 2, 'x');
OPTIMIZE TABLE t_dedup FINAL DEDUPLICATE BY * EXCEPT ({col:Identifier});
SELECT 'optimize', count() FROM t_dedup;
CREATE TABLE t_part (p UInt8, x UInt8) ENGINE = MergeTree PARTITION BY p ORDER BY x;
INSERT INTO t_part VALUES (1, 1), (2, 2), (2, 3);
BACKUP TABLE t_part PARTITION {p:UInt8} TO Memory('b') FORMAT Null;
RESTORE TABLE t_part AS t_restored PARTITION {p:UInt8} FROM Memory('b') FORMAT Null;
SELECT 'restore', count() FROM t_restored;
"

# CHECK prints one row per checked part; the part name starts with the partition ID.
$CLICKHOUSE_CLIENT --param_p=1 -q "CHECK TABLE t_part PARTITION {p:UInt8} SETTINGS check_query_single_value_result = 0" | cut -d_ -f1 | sort -u

$CLICKHOUSE_CLIENT --param_ty=local -q "EXPLAIN SYNTAX CREATE TABLE t_disk (a Int) ENGINE = MergeTree ORDER BY a
    SETTINGS disk = disk(type = cache, max_size = '1Mi', path = 'c/', disk = disk(type = {ty:String}, path = 'd/')) FORMAT TSVRaw" | grep -o "type = 'local'"

$CLICKHOUSE_CLIENT -q "CHECK TABLE t_part PARTITION {unset:UInt8}" 2>&1 | grep -o -m1 'UNKNOWN_QUERY_PARAMETER'

$CLICKHOUSE_CLIENT -q "
CREATE HANDLER \`$HCHECK\` URL '${P}/check' AS CHECK TABLE ${DB}.t_part PARTITION {p:UInt8} SETTINGS check_query_single_value_result = 0;
CREATE HANDLER \`$HSHOW\` URL '${P}/show' AS SHOW TABLES FROM ${DB} WHERE name = {n:String} LIMIT {l:UInt64};
CREATE HANDLER \`$HCOLS\` URL '${P}/cols' AS SHOW COLUMNS FROM ${DB}.t_part WHERE field = {c:String} LIMIT {l:UInt64};
CREATE HANDLER \`$HIDX\` URL '${P}/idx' AS SHOW INDEXES FROM ${DB}.t_part WHERE key_name = {k:String};
CREATE HANDLER \`$HDISK\` URL '${P}/disk' AS EXPLAIN SYNTAX CREATE TABLE t_disk (a Int) ENGINE = MergeTree ORDER BY a
    SETTINGS disk = disk(type = {ty:String}, path = 'd/') FORMAT TSVRaw"

${CLICKHOUSE_CURL} -sS "${BASE}${P}/check?param_p=1" | cut -d_ -f1 | sort -u
${CLICKHOUSE_CURL} -sS "${BASE}${P}/show?param_n=t_part&param_l=1"
${CLICKHOUSE_CURL} -sS "${BASE}${P}/cols?param_c=x&param_l=1" | cut -f1
${CLICKHOUSE_CURL} -sS "${BASE}${P}/idx?param_k=PRIMARY" | cut -f3
${CLICKHOUSE_CURL} -sS "${BASE}${P}/disk?param_ty=local" | grep -o "type = 'local'"

$CLICKHOUSE_CLIENT --param_t=query_log -q "SYSTEM FLUSH LOGS {t:Identifier}" 2>&1 | grep -o -m1 'SYNTAX_ERROR'
$CLICKHOUSE_CLIENT --param_f=CSV -q "INSERT INTO t_dedup FORMAT {f:Identifier} 1,1,x" 2>&1 | grep -o -m1 'SYNTAX_ERROR'
