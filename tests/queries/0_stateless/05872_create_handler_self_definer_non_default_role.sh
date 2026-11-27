#!/usr/bin/env bash
# Tags: no-fasttest

# A handler stored with `DEFINER = CURRENT_USER` runs with the default roles of the definer. Its query is checked the same
# way when it is stored: a role enabled only in the session does not let a handler pass that every request would fail.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}"
BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
U="u_owner_${DB}"
R="r_extra_${DB}"

http_query() { ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "$1"; }

cleanup() {
    for h in $(http_query "SELECT name FROM system.handlers WHERE endsWith(name, '_${DB}')"); do
        http_query "DROP HANDLER IF EXISTS \`$h\`"
    done
    http_query "DROP USER IF EXISTS ${U}"
    http_query "DROP ROLE IF EXISTS ${R}"
}
trap cleanup EXIT
cleanup

# Runs a query as the user with the role of the first argument enabled, if any. Prints OK, or the name of the error.
as_user() {
    local out
    out=$(${CLICKHOUSE_CURL} -sS -u "${U}:" "${CLICKHOUSE_URL}${1:+&role=$1}" --data-binary "$2" 2>&1)
    if [[ "$out" != *"DB::Exception"* ]]; then
        echo OK
    else
        echo "$out" | grep -o -m1 -E "\((ACCESS_DENIED|NOT_IMPLEMENTED)\)" || echo "$out"
    fi
}

http_query "CREATE TABLE ${DB}.by_role (x UInt8) ENGINE = Memory"
http_query "INSERT INTO ${DB}.by_role VALUES (7)"
http_query "CREATE TABLE ${DB}.direct (x UInt8) ENGINE = Memory"
http_query "INSERT INTO ${DB}.direct VALUES (8)"
http_query "CREATE ROLE ${R}"
http_query "GRANT SELECT ON ${DB}.by_role TO ${R}"
http_query "CREATE USER ${U} IDENTIFIED WITH no_password"
http_query "GRANT CREATE HANDLER, ALTER HANDLER ON *.* TO ${U}"
http_query "GRANT SELECT ON ${DB}.direct TO ${U}"
http_query "GRANT ${R} TO ${U}"
http_query "SET DEFAULT ROLE NONE TO ${U}"

echo "--- the session reads the table only with the role enabled"
as_user "" "SELECT x FROM ${DB}.by_role"
as_user "${R}" "SELECT x FROM ${DB}.by_role"

echo "--- the role of the session does not let the handler pass"
as_user "${R}" "CREATE HANDLER hr_${DB} URL '/${DB}/hr' DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT x FROM ${DB}.by_role"
as_user "${R}" "CREATE HANDLER hd_${DB} URL '/${DB}/hd' DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT x FROM ${DB}.direct"
as_user "${R}" "ALTER HANDLER hd_${DB} AS SELECT x FROM ${DB}.by_role"
http_query "SELECT replaceAll(name, '${DB}', 'DB') FROM system.handlers WHERE endsWith(name, '_${DB}') ORDER BY name"
${CLICKHOUSE_CURL} -sS "${BASE}/${DB}/hd"

echo "--- with the role as a default role the handler is stored and runs"
http_query "SET DEFAULT ROLE ${R} TO ${U}"
as_user "" "CREATE HANDLER hr_${DB} URL '/${DB}/hr' DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT x FROM ${DB}.by_role"
${CLICKHOUSE_CURL} -sS "${BASE}/${DB}/hr"
