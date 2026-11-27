#!/usr/bin/env bash
# Tags: no-fasttest

# A handler stored with `DEFINER = CURRENT_USER` from a session authenticated with a credential limited by `GRANTS`
# runs as the definer with all the privileges of the account, so its query is checked against the credential when stored.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}"
BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
U="u_owner_${DB}"

http_query() { ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "$1"; }

cleanup() {
    for h in $(http_query "SELECT name FROM system.handlers WHERE endsWith(name, '_${DB}')"); do
        http_query "DROP HANDLER IF EXISTS \`$h\`"
    done
    http_query "DROP USER IF EXISTS ${U}"
}
trap cleanup EXIT
cleanup

# Prints OK, or the name of the error.
as_user() {
    local out
    out=$(${CLICKHOUSE_CURL} -sS -u "${U}:$1" "${CLICKHOUSE_URL}" --data-binary "$2" 2>&1)
    if [[ "$out" != *"DB::Exception"* ]]; then
        echo OK
    else
        echo "$out" | grep -o -m1 -E "\((ACCESS_DENIED|NOT_IMPLEMENTED)\)" || echo "$out"
    fi
}

http_query "CREATE TABLE ${DB}.secret (x UInt8) ENGINE = Memory"
http_query "INSERT INTO ${DB}.secret VALUES (7)"
http_query "CREATE TABLE ${DB}.allowed (x UInt8) ENGINE = Memory"
http_query "CREATE USER ${U} IDENTIFIED WITH plaintext_password BY 'full', plaintext_password BY 'limited' GRANTS (CREATE HANDLER ON *.*, ALTER HANDLER ON *.*, SELECT ON ${DB}.allowed, INSERT ON ${DB}.allowed)"
http_query "GRANT CREATE HANDLER, ALTER HANDLER ON *.* TO ${U}"
http_query "GRANT SELECT, INSERT ON ${DB}.secret TO ${U}"
http_query "GRANT SELECT, INSERT ON ${DB}.allowed TO ${U}"

echo "--- the limited credential cannot read the table directly"
as_user limited "SELECT x FROM ${DB}.secret"

echo "--- CREATE HANDLER with the limited credential"
as_user limited "CREATE HANDLER hp_${DB} URL '/${DB}/hp' DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT x FROM ${DB}.secret WHERE {take:UInt8} = 1"
as_user limited "CREATE HANDLER hs_${DB} URL '/${DB}/hs' DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT x FROM ${DB}.secret"
as_user limited "CREATE HANDLER hi_${DB} URL '/${DB}/hi' DEFINER = CURRENT_USER SQL SECURITY DEFINER AS INSERT INTO ${DB}.secret SELECT x FROM ${DB}.allowed"
as_user limited "CREATE HANDLER ha_${DB} URL '/${DB}/ha' DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT x FROM ${DB}.allowed"
http_query "SELECT replaceAll(name, '${DB}', 'DB') FROM system.handlers WHERE endsWith(name, '_${DB}') ORDER BY name"

echo "--- the unlimited credential can store the parameterized handler"
as_user full "CREATE HANDLER hp_${DB} URL '/${DB}/hp' DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT x FROM ${DB}.secret WHERE {take:UInt8} = 1"
${CLICKHOUSE_CURL} -sS "${BASE}/${DB}/hp?param_take=1"

echo "--- ALTER HANDLER with the limited credential"
as_user limited "ALTER HANDLER hp_${DB} URL '/${DB}/hp2'"
as_user full "ALTER HANDLER hp_${DB} URL '/${DB}/hp2'"
