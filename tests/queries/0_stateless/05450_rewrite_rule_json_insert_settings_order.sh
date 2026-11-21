#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ASTInsertQuery::readJSON` attached the `SETTINGS` clause before the `SELECT` in `children`, while
# the SQL parser attaches it after, so the tree hash of an `INSERT ... SETTINGS ... SELECT` submitted
# in the `clickhouse_json` dialect differed from that of the same query written in SQL, and a rule
# stored for that insert did not match the JSON form. Both forms must be rejected.
# Rule names are global; make them unique per test database so the test is parallel-safe.

RULE="rule_json_insert_${CLICKHOUSE_DATABASE}"
QUERY="INSERT INTO ${CLICKHOUSE_DATABASE}.t SETTINGS max_threads = 1 SELECT 42"

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t (x UInt64) ENGINE = Memory;
    CREATE RULE ${RULE} AS (${QUERY}) REJECT WITH 'blocked';
"

$CLICKHOUSE_CLIENT --query_rules "${RULE}" --query "${QUERY}" 2>&1 | grep -o -m1 "REWRITE_RULE_REJECTION" || echo "SQL: not rejected"

query_json=$($CLICKHOUSE_CLIENT --query "SELECT parseQueryToJSON('${QUERY}')")
$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&dialect=clickhouse_json&enable_json_ast_dialect=1&query_rules=${RULE}" --data-binary "$query_json" | grep -o -m1 "REWRITE_RULE_REJECTION" || echo "JSON: not rejected"

$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM t;
    DROP RULE ${RULE};
    DROP TABLE t;
"
