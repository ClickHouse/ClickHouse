#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-replicated-database: uses ON CLUSTER

# DEFINER and SQL SECURITY of SQL-defined HTTP handlers.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}"
DB_B="${DB}_b"
BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
U_DEF="u_def_${DB}"
U_DEF2="u_def2_${DB}"
U_DEF3="u_def3_${DB}"
U_LIM="u_lim_${DB}"
U_CALLER="u_caller_${DB}"
U_ROCALLER="u_rocaller_${DB}"
U_SD="u_sd_${DB}"
U_NONE="u_none_${DB}"
U_RO="u_ro_${DB}"
E="e_${DB}"
EE="ee_${DB}"
EE2="ee2_${DB}"
U_RET="u_ret_${DB}"
U_BG="u_bg_${DB}"
U_BGCALLER="u_bgcaller_${DB}"
USERS="${U_DEF}, ${U_DEF2}, ${U_DEF3}, ${U_LIM}, ${U_CALLER}, ${U_ROCALLER}, ${U_SD}, ${U_NONE}, ${U_RO}, ${E}, ${EE}, \`${EE}:definer\`, ${EE2}, \`${EE2}:definer\`, ${U_RET}, ${U_BG}, ${U_BGCALLER}"

# Queries go over HTTP: starting a client for each one makes the test too slow under sanitizers.
http_query() { ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "$1"; }
http_query_as() { ${CLICKHOUSE_CURL} -sS -u "$1:" "${CLICKHOUSE_URL}" --data-binary "$2"; }

cleanup() {
    for h in $(http_query "SELECT name FROM system.handlers WHERE endsWith(name, '_${DB}')"); do
        http_query "DROP HANDLER IF EXISTS \`$h\`"
    done
    http_query "DROP USER IF EXISTS ${USERS}"
    http_query "DROP SETTINGS PROFILE IF EXISTS p_caller_${DB}, p_ro_${DB}, p_def_ro_${DB}, p_rw_${DB}, p_bg_${DB}"
    http_query "DROP DATABASE IF EXISTS ${DB_B}"
}
trap cleanup EXIT
cleanup

ERRORS='ACCESS_DENIED|NOT_IMPLEMENTED|BAD_ARGUMENTS|HAVE_DEPENDENT_OBJECTS|READONLY|TOO_MANY_ROWS_OR_BYTES'

# Prints OK, or the name of the error.
run() {
    local out
    if out=$("$@" 2>&1) && [[ "$out" != *"DB::Exception"* ]]; then
        echo OK
    else
        echo "$out" | grep -o -m1 -E "\((${ERRORS})\)" || echo "$out"
    fi
}
as_user() { run http_query_as "$1" "$2"; }
as_default() { run http_query "$1"; }
# Prints the response, or the name of the error.
request() {
    local out
    out=$(${CLICKHOUSE_CURL} -sS "$@" 2>&1)
    echo "$out" | grep -o -m1 -E "\((${ERRORS})\)" || echo "$out"
}
handler_query() { http_query "SELECT replaceAll($2, '${DB}', 'DB') FROM system.handlers WHERE name = '$1'"; }

$CLICKHOUSE_CLIENT -q "
CREATE TABLE ${DB}.t (id UInt32, v String) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${DB}.t VALUES (1, 'a'), (2, 'b');
CREATE TABLE ${DB}.t3 (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ${DB}.t4 (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ${DB}.dist AS ${DB}.t ENGINE = Distributed(test_shard_localhost, ${DB}, t);
CREATE DATABASE ${DB_B};
CREATE TABLE ${DB_B}.t (id UInt32, v String) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${DB_B}.t VALUES (9, 'other');
CREATE USER ${U_DEF}, ${U_DEF2}, ${U_DEF3}, ${U_LIM}, ${U_CALLER}, ${U_ROCALLER}, ${U_SD}, ${U_NONE}, ${U_RO} NOT IDENTIFIED;
CREATE USER ${E} IN memory;
GRANT SELECT ON ${DB}.t TO ${U_DEF};
GRANT SELECT ON ${DB}.dist TO ${U_DEF};
GRANT SELECT ON ${DB_B}.t TO ${U_DEF};
GRANT INSERT, SELECT ON ${DB}.t3 TO ${U_DEF};
GRANT CREATE HANDLER, ALTER HANDLER, DROP HANDLER ON *.* TO ${U_LIM}, ${U_SD}, ${U_NONE};
GRANT SET DEFINER ON ${U_DEF} TO ${U_SD};
GRANT ALLOW SQL SECURITY NONE ON *.* TO ${U_NONE};
GRANT INSERT, SELECT ON ${DB}.t4 TO ${U_RO};
CREATE SETTINGS PROFILE p_caller_${DB} SETTINGS max_result_rows = 1, result_overflow_mode = 'throw', use_legacy_to_time = 1 TO ${U_CALLER};
CREATE SETTINGS PROFILE p_ro_${DB} SETTINGS readonly = 1 TO ${U_ROCALLER};
CREATE SETTINGS PROFILE p_def_ro_${DB} SETTINGS readonly = 2 TO ${U_RO};"

echo "--- 1. the clause is stored, and the names are qualified"
as_default "CREATE HANDLER hd_${DB} URL '/${DB}/hd' DEFINER = ${U_DEF} SQL SECURITY DEFINER AS SELECT * FROM t ORDER BY id"
handler_query "hd_${DB}" create_query
as_default "CREATE HANDLER hi_${DB} URL '/${DB}/hi' AS SELECT * FROM ${DB}.t ORDER BY id"
handler_query "hi_${DB}" create_query

echo "--- 2. DEFINER runs the query as the definer, INVOKER as the requesting user"
request -u "${U_CALLER}:" "${BASE}/${DB}/hd"
request -u "${U_CALLER}:" "${BASE}/${DB}/hi"

echo "--- 3. SQL SECURITY NONE"
as_default "CREATE HANDLER hn_${DB} URL '/${DB}/hn' SQL SECURITY NONE AS SELECT count() FROM ${DB}.t"
request -u "${U_CALLER}:" "${BASE}/${DB}/hn"
http_query_as "${U_LIM}" "CREATE HANDLER hn2_${DB} URL '/${DB}/hn2' SQL SECURITY NONE AS SELECT 1" \
    | grep -o -m1 'ALLOW SQL SECURITY NONE'

echo "--- 4. DEFINER"
http_query_as "${U_LIM}" "CREATE HANDLER h4a_${DB} URL '/${DB}/h4a' DEFINER = ${U_DEF} SQL SECURITY DEFINER AS SELECT 1" \
    | grep -o -m1 'SET DEFINER'
as_user "${U_LIM}" "CREATE HANDLER h4_${DB} URL '/${DB}/h4' DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT * FROM ${DB}.t ORDER BY id"
http_query "GRANT SELECT ON ${DB}.t TO ${U_LIM}"
as_user "${U_LIM}" "CREATE HANDLER h4_${DB} URL '/${DB}/h4' DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT * FROM ${DB}.t ORDER BY id"
handler_query "h4_${DB}" create_query
request -u "${U_CALLER}:" "${BASE}/${DB}/h4"
http_query "REVOKE SELECT ON ${DB}.t FROM ${U_LIM}"
request "${BASE}/${DB}/h4"

echo "--- 5. a handler with SQL SECURITY DEFINER or NONE runs only SELECT and INSERT"
QUERIES=(
    "SET max_threads = 1"
    "USE system"
    "EXECUTE AS ${U_DEF}"
    "EXECUTE AS ${U_DEF} SELECT 1"
    "CREATE TEMPORARY TABLE x (a UInt8)"
    "GRANT SELECT ON ${DB}.t TO ${U_CALLER}"
    "ALTER TABLE ${DB}.t DELETE WHERE 0"
    "INSERT INTO TABLE FUNCTION null('a UInt8') SELECT 1"
)
for query in "${QUERIES[@]}"; do
    as_default "CREATE HANDLER h5_${DB} URL '/${DB}/h5' METHODS (POST) SQL SECURITY DEFINER AS ${query}"
    as_default "CREATE HANDLER h5_${DB} URL '/${DB}/h5' METHODS (POST) SQL SECURITY NONE AS ${query}"
    as_default "CREATE HANDLER h5_${DB} URL '/${DB}/h5' METHODS (POST) AS ${query}"
    http_query "DROP HANDLER h5_${DB}"
done

echo "--- 6. a request to it can choose only the output format and the query parameters"
as_default "CREATE HANDLER h6_${DB} URL '/${DB}/h6' METHODS (POST) DEFINER = ${U_DEF} SQL SECURITY DEFINER AS INSERT INTO ${DB}.t3 SELECT 6"
for param in "role=r_${DB}" "max_threads=1" "filter=1" "select=1" "profile=default" "database=${DB}" "session_id=s_${DB}" "query_id=q6_${DB}" "quota_key=k_${DB}" "foo=1"; do
    echo "${param%%=*}: $(request -X POST "${BASE}/${DB}/h6?${param}")"
done
echo "X-ClickHouse-Database: $(request -X POST -H "X-ClickHouse-Database: ${DB}" "${BASE}/${DB}/h6")"
echo "X-ClickHouse-Query-Id: $(request -X POST -H "X-ClickHouse-Query-Id: q6_${DB}" "${BASE}/${DB}/h6")"
echo "X-ClickHouse-Quota: $(request -X POST -H "X-ClickHouse-Quota: k_${DB}" "${BASE}/${DB}/h6")"
http_query "SELECT count() FROM ${DB}.t3"
request -X POST "${BASE}/${DB}/h6"
http_query "SELECT count() FROM ${DB}.t3"
echo "INVOKER:"
request "${BASE}/${DB}/hi?select=id&filter=id%3E1"
as_default "CREATE HANDLER hp_${DB} URL REGEXP '/${DB}/hp/.*' AS SELECT currentDatabase() = 'system'"
request "${BASE}/${DB}/hp/system?http_allow_database_as_path=1"

echo "--- 7. a safe method runs it in the readonly mode"
as_default "CREATE HANDLER h7_${DB} URL '/${DB}/h7' METHODS (GET, POST) DEFINER = ${U_DEF} SQL SECURITY DEFINER AS INSERT INTO ${DB}.t3 SELECT 7"
request "${BASE}/${DB}/h7"
request -X POST "${BASE}/${DB}/h7"
http_query "SELECT count() FROM ${DB}.t3 WHERE id = 7"

echo "--- 8. the settings of the requesting user do not apply, except readonly"
as_user "${U_CALLER}" "SELECT * FROM numbers(2)"
request -u "${U_CALLER}:" "${BASE}/${DB}/hd"
request -u "${U_CALLER}:" "${BASE}/${DB}/hd?default_format=JSONEachRow"
request -u "${U_CALLER}:" -H "X-ClickHouse-Format: JSONEachRow" "${BASE}/${DB}/hd"
request -u "${U_ROCALLER}:" -X POST "${BASE}/${DB}/h6"
http_query "SELECT count() FROM ${DB}.t3"
as_default "CREATE HANDLER h8r_${DB} URL '/${DB}/h8r' DEFINER = ${U_RO} SQL SECURITY DEFINER AS SELECT count() FROM ${DB}.t4 SETTINGS max_threads = 1"
request -u "${U_ROCALLER}:" "${BASE}/${DB}/h8r"
request "${BASE}/${DB}/h8r"
as_default "CREATE HANDLER h8i_${DB} URL '/${DB}/h8i' AS SELECT toTypeName(toTime(now()))"
as_default "CREATE HANDLER h8d_${DB} URL '/${DB}/h8d' DEFINER = ${U_DEF} SQL SECURITY DEFINER AS SELECT toTypeName(toTime(now()))"
request -u "${U_CALLER}:" "${BASE}/${DB}/h8i"
request -u "${U_CALLER}:" "${BASE}/${DB}/h8d"
http_query "ALTER SETTINGS PROFILE p_def_ro_${DB} SETTINGS readonly = 2 CHANGEABLE_IN_READONLY"
http_query "CREATE SETTINGS PROFILE p_rw_${DB} SETTINGS readonly = 0"
as_default "CREATE HANDLER h8w_${DB} URL '/${DB}/h8w' METHODS (POST) DEFINER = ${U_RO} SQL SECURITY DEFINER AS INSERT INTO ${DB}.t4 SETTINGS readonly = 0 FORMAT TSV"
as_default "CREATE HANDLER h8s_${DB} URL '/${DB}/h8s' DEFINER = ${U_RO} SQL SECURITY DEFINER AS SELECT count() FROM ${DB}.t4 SETTINGS readonly = 0"
as_default "CREATE HANDLER h8x_${DB} URL '/${DB}/h8x' METHODS (POST) DEFINER = ${U_RO} SQL SECURITY DEFINER AS INSERT INTO ${DB}.t4 SETTINGS readonly = DEFAULT FORMAT TSV"
as_default "CREATE HANDLER h8p_${DB} URL '/${DB}/h8p' METHODS (POST) DEFINER = ${U_RO} SQL SECURITY DEFINER AS INSERT INTO ${DB}.t4 SETTINGS profile = 'p_rw_${DB}' FORMAT TSV"
http_query "SELECT count() FROM system.handlers WHERE name IN ('h8w_${DB}', 'h8s_${DB}', 'h8x_${DB}', 'h8p_${DB}')"
http_query "ALTER SETTINGS PROFILE p_def_ro_${DB} SETTINGS readonly = 2"

echo "--- 9. ALTER HANDLER"
as_default "ALTER HANDLER hd_${DB} SQL SECURITY INVOKER"
handler_query "hd_${DB}" create_query
as_default "ALTER HANDLER hd_${DB} DEFINER = ${U_DEF} SQL SECURITY DEFINER"
http_query_as "${U_LIM}" "ALTER HANDLER hd_${DB} URL '/${DB}/hd2'" | grep -o -m1 'SET DEFINER'
as_default "ALTER HANDLER hd_${DB} URL '/${DB}/hd' METHODS (GET)"
handler_query "hd_${DB}" create_query

echo "--- 10. a definer cannot be dropped"
http_query "DROP USER ${U_DEF}" | grep -o -m1 "HAVE_DEPENDENT_OBJECTS"
http_query "DROP USER ${U_DEF}" | grep -c -m1 "handler hd_${DB}"
as_default "CREATE HANDLER hd2_${DB} URL '/${DB}/hd2' DEFINER = ${U_DEF2} SQL SECURITY DEFINER AS SELECT 1"
as_default "DROP USER ${U_DEF2}"
as_default "ALTER HANDLER hd2_${DB} SQL SECURITY INVOKER"
as_default "DROP USER ${U_DEF2}"
as_default "CREATE HANDLER hd3_${DB} URL '/${DB}/hd3' DEFINER = ${U_DEF3} SQL SECURITY DEFINER AS SELECT 1"
as_default "DROP HANDLER hd3_${DB}"
as_default "DROP USER ${U_DEF3}"

echo "--- 12. an ephemeral definer"
as_default "CREATE HANDLER he_${DB} URL '/${DB}/he' DEFINER = ${E} SQL SECURITY DEFINER AS SELECT 1"
http_query "SELECT countSubstrings(create_query, '${E}:definer') FROM system.handlers WHERE name = 'he_${DB}'"
http_query "DROP USER \`${E}:definer\`" | grep -o -m1 "HAVE_DEPENDENT_OBJECTS"
as_default "DROP USER ${E}"
http_query "SELECT count() FROM system.users WHERE name = '${E}:definer'"
request "${BASE}/${DB}/he"
as_default "DROP HANDLER he_${DB}"
http_query "SELECT count() FROM system.users WHERE name = '${E}:definer'"

echo "--- 13. a request to it cannot choose the database"
request -u "${U_CALLER}:" "${BASE}/${DB}/hd?database=${DB_B}"
request -u "${U_CALLER}:" -H "X-ClickHouse-Database: ${DB_B}" "${BASE}/${DB}/hd"
request "${BASE}/${DB}/hi?database=${DB_B}"

echo "--- 14. the creator must be allowed to run the query of a handler running as another user"
as_user "${U_SD}" "CREATE HANDLER h14a_${DB} URL '/${DB}/h14a' DEFINER = ${U_DEF} SQL SECURITY DEFINER AS SELECT * FROM ${DB}.t"
http_query "GRANT SELECT ON ${DB}.t TO ${U_SD}"
as_user "${U_SD}" "CREATE HANDLER h14a_${DB} URL '/${DB}/h14a' DEFINER = ${U_DEF} SQL SECURITY DEFINER AS SELECT * FROM ${DB}.t"
http_query "GRANT INSERT ON ${DB}.t3 TO ${U_SD}"
as_user "${U_SD}" "CREATE HANDLER h14b_${DB} URL '/${DB}/h14b' METHODS (POST) DEFINER = ${U_DEF} SQL SECURITY DEFINER AS INSERT INTO ${DB}.t3 FORMAT TSV"
as_user "${U_SD}" "CREATE HANDLER h14b_${DB} URL '/${DB}/h14b' METHODS (POST) DEFINER = CURRENT_USER SQL SECURITY DEFINER AS INSERT INTO ${DB}.t3 FORMAT TSV"
as_user "${U_NONE}" "CREATE HANDLER h14c_${DB} URL '/${DB}/h14c' SQL SECURITY NONE AS SELECT * FROM ${DB}.t"

echo "--- 15. the restrictions of the definer apply"
as_default "CREATE HANDLER h15_${DB} URL '/${DB}/h15' METHODS (POST) DEFINER = ${U_RO} SQL SECURITY DEFINER AS INSERT INTO ${DB}.t4 FORMAT TSV"
printf '1\n' | request -X POST --data-binary @- "${BASE}/${DB}/h15"
http_query "SELECT count() FROM ${DB}.t4"

echo "--- 16. ON CLUSTER"
as_default "CREATE HANDLER h16_${DB} ON CLUSTER test_shard_localhost URL '/${DB}/h16' DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT 1"
http_query "GRANT SELECT ON ${DB}.t TO ${U_LIM}"
as_user "${U_LIM}" "CREATE HANDLER h16_${DB} ON CLUSTER test_shard_localhost URL '/${DB}/h16' AS SELECT * FROM ${DB}.t"
as_default "CREATE HANDLER h16_${DB} ON CLUSTER test_shard_localhost URL '/${DB}/h16' AS SELECT * FROM ${DB}.t"
as_user "${U_LIM}" "ALTER HANDLER h16_${DB} ON CLUSTER test_shard_localhost URL '/${DB}/h16b'"
handler_query "h16_${DB}" url
as_default "ALTER HANDLER h16_${DB} ON CLUSTER test_shard_localhost URL '/${DB}/h16b'"
handler_query "h16_${DB}" url

echo "--- 17. a Distributed table"
as_default "CREATE HANDLER h17_${DB} URL '/${DB}/h17' DEFINER = ${U_DEF} SQL SECURITY DEFINER AS SELECT * FROM ${DB}.dist ORDER BY id SETTINGS prefer_localhost_replica = 0"
request -u "${U_CALLER}:" -D "${CLICKHOUSE_TMP}/h17_${DB}.headers" "${BASE}/${DB}/h17"
q17=$(grep -i '^X-ClickHouse-Query-Id:' "${CLICKHOUSE_TMP}/h17_${DB}.headers" | tr -d '\r' | awk '{print $2}')
# The query log entry can be written after the HTTP response is sent.
# With serialize_query_plan the shard's entry has empty `databases`, so the initial query's entry is filtered.
for _ in {1..60}; do
    http_query "SYSTEM FLUSH LOGS query_log"
    res=$(http_query "
    SELECT count() > 0, replaceAll(any(initial_user), '${DB}', 'DB') FROM system.query_log
    WHERE NOT is_initial_query AND type = 'QueryFinish' AND event_date >= yesterday()
        AND initial_query_id IN (SELECT query_id FROM system.query_log WHERE query_id = '${q17}' AND is_initial_query
            AND event_date >= yesterday() AND has(databases, currentDatabase()))")
    [[ "$res" == 1* ]] && break
    sleep 0.5
done
echo "$res"

echo "--- 18. SQL SECURITY NONE keeps the readonly mode"
http_query "CREATE TABLE ${DB}.t5 (id UInt32) ENGINE = MergeTree ORDER BY id"
as_default "CREATE HANDLER h18_${DB} URL '/${DB}/h18' METHODS (GET, POST) SQL SECURITY NONE AS INSERT INTO ${DB}.t5 SELECT 1"
as_default "CREATE HANDLER h18p_${DB} URL '/${DB}/h18p' METHODS (GET, POST) SQL SECURITY NONE AS INSERT INTO ${DB}.t5 SELECT 2 PARALLEL WITH INSERT INTO ${DB}.t5 SELECT 3"
request "${BASE}/${DB}/h18"
${CLICKHOUSE_CURL} -sS -o /dev/null -I "${BASE}/${DB}/h18"
request -u "${U_ROCALLER}:" -X POST "${BASE}/${DB}/h18"
request "${BASE}/${DB}/h18p"
request -u "${U_ROCALLER}:" -X POST "${BASE}/${DB}/h18p"
http_query "SELECT count() FROM ${DB}.t5"
request -X POST "${BASE}/${DB}/h18"
request -X POST "${BASE}/${DB}/h18p"
http_query "SELECT groupArray(id) FROM (SELECT id FROM ${DB}.t5 ORDER BY id)"

echo "--- 19. ALTER HANDLER checks the DEFINER and SQL SECURITY it sets or keeps"
as_default "CREATE HANDLER h19_${DB} URL '/${DB}/h19' AS SELECT 1"
as_default "CREATE HANDLER h19n_${DB} URL '/${DB}/h19n' SQL SECURITY NONE AS SELECT 1"
http_query_as "${U_LIM}" "ALTER HANDLER h19_${DB} DEFINER = ${U_DEF} SQL SECURITY DEFINER" | grep -o -m1 'SET DEFINER'
http_query_as "${U_LIM}" "ALTER HANDLER h19_${DB} SQL SECURITY NONE" | grep -o -m1 'ALLOW SQL SECURITY NONE'
http_query_as "${U_LIM}" "ALTER HANDLER h19n_${DB} URL '/${DB}/h19m'" | grep -o -m1 'ALLOW SQL SECURITY NONE'
handler_query "h19_${DB}" create_query
handler_query "h19n_${DB}" create_query
as_user "${U_NONE}" "ALTER HANDLER h19n_${DB} URL '/${DB}/h19m'"
handler_query "h19n_${DB}" url

echo "--- 20. a definer named <user>:definer is not the ephemeral <user>"
http_query "CREATE USER ${EE}, ${EE2} NOT IDENTIFIED IN memory"
http_query "CREATE USER \`${EE}:definer\` NOT IDENTIFIED"
http_query "GRANT CREATE HANDLER, ALTER HANDLER ON *.* TO ${EE}, ${EE2}"
http_query "GRANT INSERT ON ${DB}.t5 TO \`${EE}:definer\`"
as_default "CREATE HANDLER h20_${DB} URL '/${DB}/h20' METHODS (POST) DEFINER = \`${EE}:definer\` SQL SECURITY DEFINER AS SELECT 1"
http_query_as "${EE}" "ALTER HANDLER h20_${DB} AS INSERT INTO ${DB}.t5 SELECT 20" | grep -o -m1 'SET DEFINER'
http_query_as "${EE}" "ALTER HANDLER h20_${DB} URL '/${DB}/h20b'" | grep -o -m1 'SET DEFINER'
handler_query "h20_${DB}" create_query
as_user "${EE2}" "CREATE HANDLER h20e_${DB} URL '/${DB}/h20e' DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT 1"
http_query_as "${EE2}" "ALTER HANDLER h20e_${DB} URL '/${DB}/h20f'" | grep -o -m1 'SET DEFINER'
as_user "${EE2}" "ALTER HANDLER h20e_${DB} URL '/${DB}/h20f' DEFINER = CURRENT_USER SQL SECURITY DEFINER"
handler_query "h20e_${DB}" url

echo "--- 21. the RETURNING query of an INSERT is qualified"
http_query "CREATE USER ${U_RET} NOT IDENTIFIED DEFAULT DATABASE ${DB_B}"
http_query "GRANT INSERT ON ${DB}.t5 TO ${U_RET}"
http_query "GRANT SELECT ON ${DB}.t TO ${U_RET}"
http_query "GRANT SELECT ON ${DB_B}.t TO ${U_RET}"
as_default "CREATE HANDLER h21_${DB} URL '/${DB}/h21' METHODS (POST) DEFINER = ${U_RET} SQL SECURITY DEFINER AS INSERT INTO t5 RETURNING (SELECT v FROM t ORDER BY id LIMIT 1) SELECT 21"
handler_query "h21_${DB}" create_query
request -X POST "${BASE}/${DB}/h21"

echo "--- 22. neither the definer's nor the requesting user's profile runs it in the background"
http_query "CREATE USER ${U_BG}, ${U_BGCALLER} NOT IDENTIFIED"
http_query "GRANT SELECT ON ${DB}.t TO ${U_BG}"
http_query "CREATE SETTINGS PROFILE p_bg_${DB} SETTINGS run_query_in_background = 1 TO ${U_BG}, ${U_BGCALLER}"
as_default "CREATE HANDLER h22_${DB} URL '/${DB}/h22' DEFINER = ${U_BG} SQL SECURITY DEFINER AS SELECT count() FROM ${DB}.t"
as_default "CREATE HANDLER h22c_${DB} URL '/${DB}/h22c' DEFINER = ${U_DEF} SQL SECURITY DEFINER AS SELECT count() FROM ${DB}.t"
request "${BASE}/${DB}/h22"
request -u "${U_BGCALLER}:" "${BASE}/${DB}/h22c"
request "${BASE}/${DB}/h22c"

echo "--- 23. a query parameter naming the database of an INSERT is kept"
http_query "CREATE TABLE ${DB_B}.t5 (id UInt32) ENGINE = MergeTree ORDER BY id"
http_query "GRANT INSERT ON ${DB_B}.t5 TO ${U_DEF}"
as_default "CREATE HANDLER h23_${DB} URL '/${DB}/h23' METHODS (POST) DEFINER = ${U_DEF} SQL SECURITY DEFINER AS INSERT INTO {db:Identifier}.t5 SELECT 23"
handler_query "h23_${DB}" query
request -X POST "${BASE}/${DB}/h23?param_db=${DB_B}"
http_query "SELECT groupArray(id) FROM ${DB_B}.t5"

echo "--- 24. a request to it cannot attach external tables"
as_default "CREATE HANDLER h24_${DB} URL '/${DB}/h24' METHODS (POST) DEFINER = ${U_DEF} SQL SECURITY DEFINER AS SELECT count() FROM ${DB}.t WHERE v = {p:String}"
echo 'x' > "${CLICKHOUSE_TMP}/ext_${DB}.tsv"
request -X POST -F "ext=@${CLICKHOUSE_TMP}/ext_${DB}.tsv" "${BASE}/${DB}/h24?ext_structure=v%20String&param_p=a"
# Refused before its data is read: the data does not match the structure.
request -X POST -F "ext=@${CLICKHOUSE_TMP}/ext_${DB}.tsv" "${BASE}/${DB}/h24?ext_structure=v%20UInt8&param_p=a"
request -X POST -F "param_p=a" "${BASE}/${DB}/h24"
