#!/usr/bin/env bash
# the blending engine check must not take the self-reference of a recursive cte in a view for a table

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_POLICY="u_policy_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
DROP USER IF EXISTS $USER_POLICY;
CREATE TABLE src (k UInt64) ENGINE = SummingMergeTree ORDER BY k;
CREATE VIEW v AS WITH RECURSIVE seq AS (SELECT toUInt64(1) AS k UNION ALL SELECT k + 1 FROM seq WHERE k < 3) SELECT * FROM seq;
CREATE VIEW v_seed AS WITH RECURSIVE seq AS (SELECT k FROM src UNION ALL SELECT k + 1 FROM seq WHERE k < 3) SELECT * FROM seq;
CREATE VIEW v_step AS WITH RECURSIVE seq AS (SELECT toUInt64(1) AS k UNION ALL SELECT k + 1 FROM seq WHERE k IN (SELECT k FROM src)) SELECT * FROM seq;
CREATE USER $USER_POLICY;
GRANT CREATE ROW POLICY, SHOW TABLES ON ${CLICKHOUSE_DATABASE}.v TO $USER_POLICY;
"

echo "policy on a view of a recursive cte, by a user who can see only the view"
$CLICKHOUSE_CLIENT --user "$USER_POLICY" -q "CREATE ROW POLICY p ON v USING k > 0 TO ALL" 2>&1 && echo "ok"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY p ON v"

$CLICKHOUSE_CLIENT -q "CREATE TABLE seq (k UInt64) ENGINE = SummingMergeTree ORDER BY k"
echo "policy on a view of a recursive cte, with a blending table of the same name"
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p ON v USING k > 0 TO ALL" 2>&1 && echo "ok"
$CLICKHOUSE_CLIENT -q "DROP ROW POLICY p ON v"
$CLICKHOUSE_CLIENT -q "SELECT * FROM v ORDER BY k"

echo "policy on a view of a recursive cte which reads a blending table in the seed"
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p ON v_seed USING k > 0 TO ALL" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"
echo "policy on a view of a recursive cte which reads a blending table in the recursive step"
$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY p ON v_step USING k > 0 TO ALL" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

$CLICKHOUSE_CLIENT -q "DROP USER $USER_POLICY"
