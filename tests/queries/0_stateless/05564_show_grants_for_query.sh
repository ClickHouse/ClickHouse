#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `SHOW GRANTS ... FOR QUERY <select>` shows the grants of the target that the access checks of the query consult.

prefix="sgq_${CLICKHOUSE_TEST_UNIQUE_NAME}_"
u1="${prefix}u1"
u2="${prefix}u2"
u3="${prefix}u3"
u4="${prefix}u4"
u5="${prefix}u5"
u6="${prefix}u6"
u7="${prefix}u7"
u8="${prefix}u8"
u9="${prefix}u9"
caller="${prefix}caller"
r1="${prefix}r1"
all_users="$u1, $u2, $u3, $u4, $u5, $u6, $u7, $u8, $u9, $caller"
csv_file="${CLICKHOUSE_TEST_UNIQUE_NAME}.csv"

function client()
{
    ${CLICKHOUSE_CLIENT} --allow_experimental_show_grants_for_query=1 "$@" 2>&1 | sed "s/${prefix}//g"
}

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS $all_users"
    ${CLICKHOUSE_CLIENT} -q "DROP ROLE IF EXISTS $r1"
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE_1}"
    rm -f "${USER_FILES_PATH:?}/${csv_file}"
}

cleanup

${CLICKHOUSE_CLIENT} --allow_experimental_alias_table_engine=1 -n -q "
    CREATE DATABASE ${CLICKHOUSE_DATABASE_1};
    CREATE TABLE ${CLICKHOUSE_DATABASE_1}.t2 (a UInt8) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE mytable1 (column1 UInt8, column2 UInt8) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE t2 (a UInt8, b UInt8) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE t3 (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t2 VALUES (1, 2);
    INSERT INTO t3 VALUES (1, 3);
    CREATE TABLE al ENGINE = Alias(${CLICKHOUSE_DATABASE}, t2);
    CREATE VIEW vi SQL SECURITY INVOKER AS SELECT a, b FROM t2;
    CREATE VIEW vd DEFINER = default SQL SECURITY DEFINER AS SELECT a, b FROM t2;
    CREATE TABLE m ENGINE = Merge(${CLICKHOUSE_DATABASE}, '^t2\$');
    CREATE TABLE dsrc (id UInt64, v1 String, v2 String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO dsrc VALUES (1, 'a', 'b');
    CREATE DICTIONARY d (id UInt64, v1 String, v2 String) PRIMARY KEY id
        SOURCE(CLICKHOUSE(TABLE 'dsrc' DB '${CLICKHOUSE_DATABASE}')) LAYOUT(FLAT()) LIFETIME(0);
    INSERT INTO FUNCTION file('${csv_file}', 'CSV', 'a UInt8') SELECT 1;

    CREATE USER $u1, $u2, $u3, $u4, $u5, $u6, $u7, $u8, $u9, $caller;
    CREATE ROLE $r1;
    GRANT $r1 TO $u1;
    GRANT SELECT ON *.*, INSERT ON *.* TO $r1;
    GRANT SELECT(a) ON t2 TO $u2;
    GRANT SELECT ON t3 TO $u2 WITH GRANT OPTION;
    GRANT SELECT ON al TO $u4;
    GRANT SELECT(a) ON t2 TO $u4;
    GRANT SELECT ON vi TO $u4;
    GRANT SELECT ON vd TO $u4;
    GRANT READ ON FILE TO $u5;
    GRANT CREATE TEMPORARY TABLE ON *.* TO $u5;
    GRANT dictGet ON d TO $u6;
    GRANT SELECT ON m TO $u7;
    GRANT SELECT(a) ON t2 TO $u7;
    GRANT SELECT ON d TO $u8;
    REVOKE SELECT(v2) ON d FROM $u8;
    GRANT SELECT ON system.opentelemetry_span_log TO $u9;
    GRANT SELECT(a) ON t2 TO $u9;
    GRANT SHOW USERS ON *.* TO $caller;
    GRANT SELECT ON d TO $caller;
"

echo "--- 1: disabled by default"
${CLICKHOUSE_CLIENT} -q "SHOW GRANTS FOR $u1 FOR QUERY SELECT column1 FROM mytable1" 2>&1 | grep -o -m1 SUPPORT_IS_DISABLED

echo "--- 2: the grants of a role are shown for the user, filtered by the query"
client -q "SHOW GRANTS FOR $u1 FOR QUERY SELECT column1 FROM mytable1"

echo "--- 3: column-level grant"
client -q "SHOW GRANTS FOR $u2 FOR QUERY SELECT a, b FROM t2"

echo "--- 4: no relevant grant"
client -q "SHOW GRANTS FOR $u3 FOR QUERY SELECT a FROM t2"

echo "--- 5: JOIN, IN subquery and scalar subquery"
client -q "SHOW GRANTS FOR $u2 FOR QUERY SELECT t2.a FROM t2 JOIN t3 ON t2.a = t3.x WHERE t2.a IN (SELECT x FROM t3) AND (SELECT max(y) FROM t3) > 0"

echo "--- 5b: the pipeline of the query is not executed"
client -q "SHOW GRANTS FOR $u2 FOR QUERY SELECT throwIf(a = 1) FROM t2"

echo "--- 5c: the settings of the query and of the session apply as when the query runs"
client -q "SHOW GRANTS FOR $u2 FOR QUERY SELECT a FROM t2 SETTINGS filter = 'a IN (SELECT x FROM t3)'"
client -n -q "SET filter = 'a IN (SELECT x FROM t3)'; SHOW GRANTS FOR $u2 FOR QUERY SELECT a FROM t2"
client -q "SHOW GRANTS FOR $u1 FOR QUERY SELECT a FROM t2 SETTINGS database = '${CLICKHOUSE_DATABASE_1}'"
client -q "EXECUTE AS $u2 SHOW GRANTS FOR QUERY SELECT a FROM t2 SETTINGS filter = 'a IN (SELECT x FROM t3)'"
client -q "EXECUTE AS $u1 SHOW GRANTS FOR QUERY SELECT a FROM t2 SETTINGS filter = 'a IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.t3)', database = '${CLICKHOUSE_DATABASE_1}'"

echo "--- 6: trivial count"
client -q "SHOW GRANTS FOR $u2 FOR QUERY SELECT count() FROM t2"

echo "--- 6b: Alias table"
client -q "SHOW GRANTS FOR $u4 FOR QUERY SELECT count() FROM al"
client -q "SHOW GRANTS FOR $u4 FOR QUERY SELECT a FROM al"

echo "--- 7: table function with a source"
client -q "SHOW GRANTS FOR $u5 FOR QUERY SELECT a FROM file('${csv_file}', 'CSV', 'a UInt8')"

echo "--- 7b: a source grant with a filter: every filter of the source is shown"
# clickhouse-local enables `access_control_improvements.enable_read_write_grants` and keeps the filters of source grants in memory.
${CLICKHOUSE_LOCAL} --allow_experimental_show_grants_for_query=1 -q "
    CREATE USER u10;
    GRANT CREATE TEMPORARY TABLE ON *.* TO u10;
    GRANT READ ON URL('http://127.0.0.1:1/a.*') TO u10;
    GRANT READ ON URL('http://127.0.0.1:1/b.*') TO u10;
    SHOW GRANTS FOR u10 FOR QUERY SELECT * FROM url('http://127.0.0.1:1/a.tsv', LineAsString);
" 2>&1

echo "--- 8: WITH GRANT OPTION is kept"
client -q "SHOW GRANTS FOR $u2 FOR QUERY SELECT x FROM t3"

echo "--- 9: SQL SECURITY INVOKER view, SQL SECURITY DEFINER view"
client -q "SHOW GRANTS FOR $u4 FOR QUERY SELECT a FROM vi"
client -q "SHOW GRANTS FOR $u4 FOR QUERY SELECT a FROM vd"

echo "--- 9c: Merge table"
client -q "SHOW GRANTS FOR $u7 FOR QUERY SELECT a FROM m"

echo "--- 10: view inlining, as a session setting and in the query"
client --analyzer_inline_views=1 -q "SHOW GRANTS FOR $u4 FOR QUERY SELECT a FROM vi"
client -q "SHOW GRANTS FOR $u4 FOR QUERY SELECT a FROM vi SETTINGS analyzer_inline_views = 1"

echo "--- 11: several users"
client -q "SHOW GRANTS FOR $u2, $u4 FOR QUERY SELECT a FROM t2"

echo "--- 12: the query is analyzed with the access of the current user"
client --user "$u2" -q "SHOW GRANTS FOR QUERY SELECT a FROM t2"
client --user "$u2" -q "SHOW GRANTS FOR QUERY SELECT b FROM t2" | grep -o -m1 ACCESS_DENIED
client --user "$u2" -q "SHOW GRANTS FOR $u1 FOR QUERY SELECT a FROM t2" | grep -o -m1 ACCESS_DENIED

echo "--- 13: formatting"
for query in \
    "SHOW GRANTS FOR QUERY SELECT 1" \
    "SHOW GRANTS FOR u1 WITH IMPLICIT FINAL FOR QUERY SELECT a FROM t2" \
    "SHOW GRANTS FOR QUERY SELECT 1 FORMAT TSV SETTINGS max_threads = 1" \
    "SHOW GRANTS FOR query" \
    "SHOW GRANTS FOR query FOR QUERY SELECT 1"
do
    ${CLICKHOUSE_CLIENT} --param_q="$query" -q "SELECT formatQuerySingleLine({q:String}), formatQuerySingleLine(formatQuerySingleLine({q:String})) = formatQuerySingleLine({q:String})"
done
client -q "SHOW GRANTS FOR $u3 WITH IMPLICIT FOR QUERY SELECT 1"

echo "--- 14: dictionary table"
client -q "SHOW GRANTS FOR $u6 FOR QUERY SELECT * FROM d"

echo "--- 14b: a check denied for the current user still selects the grants of the target"
client --user "$caller" -q "SHOW GRANTS FOR $u6 FOR QUERY SELECT * FROM d"

echo "--- 14c: partial revokes are shown as with FINAL"
client -q "SHOW GRANTS FOR $u8 FOR QUERY SELECT id, v1 FROM d"
client -q "SHOW GRANTS FOR $u8 FINAL"

echo "--- 15: dictGet"
client -q "SHOW GRANTS FOR $u6 FOR QUERY SELECT dictGet('${CLICKHOUSE_DATABASE}.d', 'v1', toUInt64(1))"

echo "--- 16: traceView, with a check that only its internal queries make"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS opentelemetry_span_log"
trace_id=$(${CLICKHOUSE_CLIENT} -q "SELECT toString(generateUUIDv4())")
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO system.opentelemetry_span_log
        (hostname, trace_id, span_id, parent_span_id, operation_name, kind, start_time_us, finish_time_us, event_date, attribute)
    VALUES
        ('h', '$trace_id', 1, 0, 'query', 'SERVER', 1000, 5000, today(), map()),
        ('h', '$trace_id', 2, 1, 'child', 'INTERNAL', 2000, 3000, today(), map())
"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS opentelemetry_span_log"
span_log_filter="{'system.opentelemetry_span_log': 'span_id IN (SELECT toUInt64(a) FROM ${CLICKHOUSE_DATABASE}.t2) OR span_id > 0'}"
client --additional_table_filters="$span_log_filter" -q "SHOW GRANTS FOR $u9 FOR QUERY SELECT span FROM traceView('$trace_id')"

cleanup
