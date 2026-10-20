#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression: the rewrite-rule matcher treats an equal `getTreeHash(true)` as semantic equality, so
# a tree hash that maps two different statements to the same input lets a rule over-match:
#   * `ASTBackupQuery` hashed adjacent names without their lengths (`a.bc` and `ca.b` both appended
#     `bcabca`) and did not fold the `EXCEPT DATA` selectors;
#   * `ASTSystemQuery` did not fold the `SYSTEM RESET FILELOG` file / offset / `TO END` fields;
#   * `ASTGrantQuery` / `ASTCheckGrantQuery` hash the lossy formatted privilege list (`READ ON FILE`
#     and `WRITE ON FILE` are both formatted as `FILE` unless `enable_read_write_grants` is set), so
#     templates naming such privileges are rejected at `CREATE RULE` time.
# The rule names are suffixed with the test database so the test stays parallel-safe despite the
# global rule registry, and only whether the rule fired (rejection) is asserted, so the statements
# themselves need not actually succeed.

RULE="rule_lossy_hash_${CLICKHOUSE_DATABASE}"

check()
{
    local label="$1"
    local query="$2"
    out=$($CLICKHOUSE_CLIENT --query_rules="${RULE}" --query "$query" 2>&1 || true)
    if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "${label}: rejected"; else echo "${label}: not rejected"; fi
}

DEST="Disk('backups', 'b_${CLICKHOUSE_DATABASE}.zip')"

$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE} AS (BACKUP TABLE a.bc TO ${DEST}) REJECT WITH 'blocked'"
check "backup a.bc" "BACKUP TABLE a.bc TO ${DEST}"
check "backup ca.b" "BACKUP TABLE ca.b TO ${DEST}"
$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE}"

$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE} AS (BACKUP TABLE db.t TO ${DEST}) REJECT WITH 'blocked'"
check "backup with data" "BACKUP TABLE db.t TO ${DEST}"
check "backup except data" "BACKUP TABLE db.t EXCEPT DATA FROM TABLE db.t TO ${DEST}"
$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE}"

$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE} AS (SYSTEM RESET FILELOG db.t FILE 'a.log' OFFSET 0) REJECT WITH 'blocked'"
check "filelog same file" "SYSTEM RESET FILELOG db.t FILE 'a.log' OFFSET 0"
check "filelog other file" "SYSTEM RESET FILELOG db.t FILE 'b.log' OFFSET 0"
check "filelog other offset" "SYSTEM RESET FILELOG db.t FILE 'a.log' OFFSET 1"
check "filelog to end" "SYSTEM RESET FILELOG db.t FILE 'a.log' TO END"
$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE}"

# Templates naming privileges with a lossy formatted form are rejected.
for template in "CHECK GRANT READ ON FILE" "CHECK GRANT WRITE ON FILE" "CHECK GRANT FILE ON *.*" "GRANT READ ON FILE TO u" "GRANT CREATE USER ON *.* TO u"
do
    $CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE} AS (${template}) REJECT WITH 'blocked'" 2>&1 | grep -o -m1 "BAD_ARGUMENTS" || echo "${template}: accepted (unexpected)"
done

# Other privileges are still accepted.
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE} AS (CHECK GRANT SELECT ON db.t) REJECT WITH 'blocked'"
check "check grant select" "CHECK GRANT SELECT ON db.t"
check "check grant insert" "CHECK GRANT INSERT ON db.t"
$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE}"
