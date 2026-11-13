#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Query masking rules are global, so every rule name and every token contains the database name:
# the rules of this test must never match the text of another test.
db=${CLICKHOUSE_DATABASE}
user="u_${db}"

function cleanup()
{
    $CLICKHOUSE_CLIENT -m -q "
        DROP QUERY MASKING RULE IF EXISTS \`${db}_r1\`, \`${db}_r1_renamed\`, \`${db}_cfg\`, \`${db}_ord_1\`, \`${db}_ord_2\`, \`${db}_bad\`, \`${db}_by_user\`, \`${db}_in\`;
        DROP USER IF EXISTS \`${user}\`;
    "
}

function normalize()
{
    sed -e "s/${db}/<db>/g"
}

function trace_logs()
{
    $CLICKHOUSE_CLIENT --allow_repeated_settings --send_logs_level=trace -q "$1" 2>&1 >/dev/null
}

# The error text of a failed query, without the query itself, which the client prints as is.
function error_text()
{
    "$@" 2>&1 | grep -v '^(query: '
}

cleanup

echo "-- masking of the server logs, query_log and exception messages"
$CLICKHOUSE_CLIENT -q "CREATE QUERY MASKING RULE \`${db}_r1\` REGEXP 'qmr_secret_${db}' REPLACE '[qmr_masked]'"
logs=$(trace_logs "SELECT 'qmr_secret_${db}' AS c1 FORMAT Null")
grep -qF "SELECT '[qmr_masked]' AS c1" <<< "$logs" && echo "server log masked"
grep -qF "qmr_secret_${db}" <<< "$logs" && echo "server log leaked"
error_text $CLICKHOUSE_CLIENT -q "SELECT throwIf(1, 'qmr_secret_${db}')" | grep -oF -e "[qmr_masked]" -e "qmr_secret_${db}" | LC_ALL=C sort -u | normalize

# The order of the rules is checked in query_log below: a server log line is masked once more when it is written.
$CLICKHOUSE_CLIENT -m -q "
    CREATE QUERY MASKING RULE \`${db}_cfg\` REGEXP '.hidden._qmr_${db}' REPLACE '[qmr_after_config]';
    SELECT 'TOPSECRET=TOPSECRET_qmr_${db}' AS c3 FORMAT Null;
    CREATE QUERY MASKING RULE \`${db}_ord_2\` REGEXP 'qmr_stage_${db}' REPLACE '[qmr_ordered]';
    CREATE QUERY MASKING RULE \`${db}_ord_1\` REGEXP 'qmr_order_${db}' REPLACE 'qmr_stage_${db}';
    SELECT 'qmr_order_${db}' AS c4 FORMAT Null;
    DROP QUERY MASKING RULE \`${db}_cfg\`, \`${db}_ord_1\`, \`${db}_ord_2\`;
"

echo "-- ALTER"
$CLICKHOUSE_CLIENT -q "ALTER QUERY MASKING RULE \`${db}_r1\` REPLACE '[qmr_new]'"
trace_logs "SELECT 'qmr_secret_${db}' AS c5 FORMAT Null" | grep -oF -e "[qmr_new]" -e "qmr_secret_${db}" | LC_ALL=C sort -u | normalize
$CLICKHOUSE_CLIENT -m -q "
    ALTER QUERY MASKING RULE \`${db}_r1\` RENAME TO \`${db}_r1_renamed\`;
    CREATE QUERY MASKING RULE IF NOT EXISTS \`${db}_r1_renamed\` REGEXP 'qmr_unused_${db}' REPLACE '[qmr_unused]';
    SELECT name, replace FROM system.query_masking_rules WHERE startsWith(name, '${db}_') ORDER BY name;
    CREATE QUERY MASKING RULE OR REPLACE \`${db}_r1_renamed\` REGEXP 'qmr_secret_${db}' REPLACE '[qmr_masked]';
    SELECT name, replace FROM system.query_masking_rules WHERE startsWith(name, '${db}_') ORDER BY name;
" | normalize

echo "-- introspection: the REGEXP is a secret"
$CLICKHOUSE_CLIENT -m -q "
    SELECT name, storage != '', regexp, replace FROM system.query_masking_rules WHERE startsWith(name, '${db}_') ORDER BY name;
    SHOW CREATE QUERY MASKING RULE \`${db}_r1_renamed\` FORMAT TSVRaw;
" | normalize
$CLICKHOUSE_CLIENT -q "SHOW QUERY MASKING RULES" | grep -F "${db}_" | normalize

echo "-- IN <storage> applies at once"
storage=$($CLICKHOUSE_CLIENT -q "SELECT storage FROM system.query_masking_rules WHERE name = '${db}_r1_renamed'")
$CLICKHOUSE_CLIENT -q "CREATE QUERY MASKING RULE \`${db}_in\` IN \`${storage}\` REGEXP 'qmr_in_${db}' REPLACE '[qmr_in]'"
error_text $CLICKHOUSE_CLIENT -q "SELECT throwIf(1, 'qmr_in_${db}')" | grep -oF -e "[qmr_in]" -e "qmr_in_${db}" | LC_ALL=C sort -u | normalize
$CLICKHOUSE_CLIENT -q "ALTER QUERY MASKING RULE \`${db}_in\` IN \`${storage}\` REPLACE '[qmr_in_altered]'"
error_text $CLICKHOUSE_CLIENT -q "SELECT throwIf(1, 'qmr_in_${db}')" | grep -oF -e "[qmr_in_altered]" -e "qmr_in_${db}" | LC_ALL=C sort -u | normalize
$CLICKHOUSE_CLIENT -q "DROP QUERY MASKING RULE \`${db}_in\`"

echo "-- errors"
error_text $CLICKHOUSE_CLIENT -q "CREATE QUERY MASKING RULE \`${db}_bad\` REGEXP '(qmr_frag_${db}'" | grep -oF -e "missing )" -e "CANNOT_COMPILE_REGEXP" -e "qmr_frag_${db}" | LC_ALL=C sort -u | normalize
$CLICKHOUSE_CLIENT -m -q "
    CREATE QUERY MASKING RULE \`${db}_bad\` REGEXP ''; -- { serverError BAD_ARGUMENTS }
    CREATE QUERY MASKING RULE \`${db}_bad\` REGEXP '(qmr_cap_${db})' REPLACE '\\\\2'; -- { serverError BAD_ARGUMENTS }
    DROP QUERY MASKING RULE \`qmr_missing_${db}\`; -- { serverError UNKNOWN_QUERY_MASKING_RULE }
    ALTER QUERY MASKING RULE \`qmr_missing_${db}\` REPLACE 'x'; -- { serverError UNKNOWN_QUERY_MASKING_RULE }
    SELECT count() FROM system.query_masking_rules WHERE name = '${db}_bad';
"

echo "-- privileges"
$CLICKHOUSE_CLIENT -m -q "
    CREATE USER \`${user}\` IDENTIFIED WITH no_password;
    GRANT CREATE USER ON *.* TO \`${user}\`;
"
error_text $CLICKHOUSE_CLIENT --user "${user}" -q "CREATE QUERY MASKING RULE \`${db}_by_user\` REGEXP 'qmr_user_${db}'" | grep -oF -e "ACCESS_DENIED" -e "QUERY MASKING RULE ADMIN" | LC_ALL=C sort -u
error_text $CLICKHOUSE_CLIENT --user "${user}" -q "DROP QUERY MASKING RULE \`${db}_r1_renamed\`" | grep -oF -e "ACCESS_DENIED" -e "QUERY MASKING RULE ADMIN" | LC_ALL=C sort -u
error_text $CLICKHOUSE_CLIENT --user "${user}" -q "SHOW QUERY MASKING RULES" | grep -oF "ACCESS_DENIED" | LC_ALL=C sort -u
error_text $CLICKHOUSE_CLIENT --user "${user}" -q "SELECT count() FROM system.query_masking_rules" | grep -oF "ACCESS_DENIED" | LC_ALL=C sort -u
error_text $CLICKHOUSE_CLIENT --user "${user}" -q "SHOW CREATE QUERY MASKING RULE \`${db}_r1_renamed\`" | grep -oF -e "ACCESS_DENIED" -e "QUERY MASKING RULE ADMIN" | LC_ALL=C sort -u
error_text $CLICKHOUSE_CLIENT --user "${user}" -q "MOVE QUERY MASKING RULE \`${db}_r1_renamed\` TO memory" | grep -oF -e "ACCESS_DENIED" -e "QUERY MASKING RULE ADMIN" | LC_ALL=C sort -u
$CLICKHOUSE_CLIENT -q "GRANT QUERY MASKING RULE ADMIN ON *.* TO \`${user}\`"
$CLICKHOUSE_CLIENT --user "${user}" -m -q "
    CREATE QUERY MASKING RULE \`${db}_by_user\` REGEXP 'qmr_user_${db}';
    SELECT name FROM system.query_masking_rules WHERE name = '${db}_by_user';
    SHOW CREATE QUERY MASKING RULE \`${db}_by_user\` FORMAT TSVRaw;
    DROP QUERY MASKING RULE \`${db}_by_user\`;
" | normalize
$CLICKHOUSE_CLIENT --user "${user}" -q "SHOW QUERY MASKING RULES" | grep -F "${db}_" | normalize

echo "-- query_log"
$CLICKHOUSE_CLIENT -q "DROP QUERY MASKING RULE \`${db}_r1_renamed\`"
$CLICKHOUSE_CLIENT -q "SELECT 'qmr_secret_${db}' AS c9 FORMAT Null"
$CLICKHOUSE_CLIENT -m -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT 'select masked', countIf(position(query, ' AS c1 ') > 0 AND position(query, '[qmr_masked]') > 0), countIf(position(query, ' AS c1 ') > 0 AND position(query, 'qmr_secret_${db}') > 0)
    FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select';
    SELECT 'config rules first', countIf(position(query, ' AS c3 ') > 0 AND position(query, '[qmr_after_config]') > 0), countIf(position(query, ' AS c3 ') > 0 AND position(query, '_qmr_${db}') > 0)
    FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select';
    SELECT 'ordered by name', countIf(position(query, ' AS c4 ') > 0 AND position(query, '[qmr_ordered]') > 0), countIf(position(query, ' AS c4 ') > 0 AND position(query, 'qmr_stage_${db}') > 0)
    FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select';
    SELECT 'create hides the regexp', countIf(position(query, 'REGEXP \'[HIDDEN]\'') > 0), countIf(position(query, 'qmr_secret_${db}') > 0)
    FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Create' AND position(query, '${db}_r1') > 0;
    SELECT 'not masked after drop', countIf(position(query, ' AS c9 ') > 0 AND position(query, 'qmr_secret_${db}') > 0)
    FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select';
"

cleanup
