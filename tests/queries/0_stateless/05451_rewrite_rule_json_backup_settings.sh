#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ASTBackupQuery::readJSON` attached the `SETTINGS` clause to `children`, while the SQL parser keeps
# it outside, so the tree hash of a `BACKUP ... SETTINGS ...` submitted in the `clickhouse_json`
# dialect differed from that of the same query written in SQL, and a rule stored for that backup did
# not match the JSON form. Both forms must be rejected.
# Rule names are global; make them unique per test database so the test is parallel-safe.

RULE="rule_json_backup_${CLICKHOUSE_DATABASE}"
QUERY="BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO Null SETTINGS structure_only = 1"

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t (x UInt64) ENGINE = Memory;
    CREATE RULE ${RULE} AS (${QUERY}) REJECT WITH 'blocked';
"

$CLICKHOUSE_CLIENT --query_rules "${RULE}" --query "${QUERY}" 2>&1 | grep -o -m1 "REWRITE_RULE_REJECTION" || echo "SQL: not rejected"

query_json=$($CLICKHOUSE_CLIENT --query "SELECT parseQueryToJSON('${QUERY}')")
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&dialect=clickhouse_json&enable_json_ast_dialect=1&query_rules=${RULE}" --data-binary "$query_json" | grep -o -m1 "REWRITE_RULE_REJECTION" || echo "JSON: not rejected"

$CLICKHOUSE_CLIENT --query "
    DROP RULE ${RULE};
    DROP TABLE t;
"
