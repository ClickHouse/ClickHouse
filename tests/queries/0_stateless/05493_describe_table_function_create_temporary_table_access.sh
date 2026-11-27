#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Resolving the structure of a table function (`DESCRIBE TABLE url(...)`, or a table function nested in
# `cluster`) must require the same `CREATE TEMPORARY TABLE` grant as reading from it, and must be refused
# before the source is contacted. Otherwise a user who cannot run `url` (e.g. under `readonly = 1`) can make
# the server request an arbitrary endpoint and read the response from the schema inference error.

USER_READONLY_1="readonly_1_${CLICKHOUSE_TEST_UNIQUE_NAME}"
USER_READONLY_2="readonly_2_${CLICKHOUSE_TEST_UNIQUE_NAME}"
USER_READONLY_0="readonly_0_${CLICKHOUSE_TEST_UNIQUE_NAME}"
TEMP_CREDENTIALS="unknown"

# Create users with different readonly settings.
function create_users() {
    ${CLICKHOUSE_CLIENT} -m -q "
        DROP USER IF EXISTS ${USER_READONLY_1}, ${USER_READONLY_2}, ${USER_READONLY_0};
        CREATE USER ${USER_READONLY_1} IDENTIFIED WITH PLAINTEXT_PASSWORD BY '${TEMP_CREDENTIALS}' SETTINGS readonly = 1;
        CREATE USER ${USER_READONLY_2} IDENTIFIED WITH PLAINTEXT_PASSWORD BY '${TEMP_CREDENTIALS}' SETTINGS readonly = 2;
        -- \`readonly = 0\`, but without the \`CREATE TEMPORARY TABLE\` grant.
        CREATE USER ${USER_READONLY_0} IDENTIFIED WITH PLAINTEXT_PASSWORD BY '${TEMP_CREDENTIALS}';
        GRANT SELECT ON *.*, READ ON URL, READ ON REMOTE TO ${USER_READONLY_1}, ${USER_READONLY_2}, ${USER_READONLY_0};
        GRANT CREATE TEMPORARY TABLE ON *.* TO ${USER_READONLY_1}, ${USER_READONLY_2};
    "
}

function drop_users() {
    ${CLICKHOUSE_CLIENT} -q "DROP USER ${USER_READONLY_1}, ${USER_READONLY_2}, ${USER_READONLY_0}"
}

# Instead of a mock server, use this server's HTTP interface: every request to it is logged in `system.query_log`
# with the marker given as the argument, so `check_endpoint_requests` can tell whether the source was contacted.
# It always responds in `JSONEachRow`, like the JSON metadata service in the original report: the format passed
# to `url` is how the response is parsed, so `Values` fails to parse it and would echo the body in the error.
function endpoint() {
    local marker="$1"
    echo "${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/?database=${CLICKHOUSE_DATABASE}&query=SELECT+1+AS+x,+%27marker_${marker}%27+AS+m+FORMAT+JSONEachRow"
}

# Run a query as the given user and print `READONLY`, `ACCESS_DENIED`, `OK`, or the full unexpected error.
# `CLICKHOUSE_CLIENT` passes randomized settings, which a `readonly = 1` user is not allowed to change.
function run() {
    local user="$1"
    local query="$2"
    local output
    output=$(${CLICKHOUSE_CLIENT_BINARY} --host "${CLICKHOUSE_HOST}" --port "${CLICKHOUSE_PORT_TCP}" \
        --user "${user}" --password "${TEMP_CREDENTIALS}" -q "${query}" 2>&1)
    if [[ "${output}" =~ \((READONLY|ACCESS_DENIED)\) ]]; then
        echo "${BASH_REMATCH[1]}"
    elif [[ "${output}" == *Exception* ]]; then
        echo "${output}"
    else
        echo "OK"
    fi
}

# Run all the queries as one user. The user name without the unique suffix (e.g. `readonly_1`)
# labels the output and the endpoint markers.
function check_user() {
    local user="$1"
    local label="${user%_"${CLICKHOUSE_TEST_UNIQUE_NAME}"}"
    echo "--- ${label}"
    run "${user}" "SELECT * FROM url('$(endpoint "${label}_select")', 'JSONEachRow')"
    run "${user}" "DESCRIBE TABLE url('$(endpoint "${label}_describe")', 'JSONEachRow')"
    # With a format that fails to parse the response, the schema inference error would contain the response body.
    # It is skipped for `readonly_2`, which is allowed to read the URL: its error text depends on the server version.
    if [[ "${label}" != "readonly_2" ]]; then
        run "${user}" "DESCRIBE TABLE url('$(endpoint "${label}_values")', 'Values')"
    fi

    # `remote` is not allowed in readonly mode, so it is refused before the nested `url` is resolved.
    run "${user}" "DESCRIBE TABLE remote('127.0.0.1', url('$(endpoint "${label}_remote")', 'JSONEachRow'))"
    # `cluster` is allowed in readonly mode, but it resolves the structure of the nested `url` on the local shard.
    run "${user}" "DESCRIBE TABLE cluster('test_shard_localhost', url('$(endpoint "${label}_cluster")', 'JSONEachRow'))"

    # Table functions allowed in readonly mode stay available without the grant.
    run "${user}" "DESCRIBE TABLE numbers(1)"
}

# Print the markers of the requests that reached the endpoint, i.e. which queries contacted the source.
# Only the queries of the `readonly = 2` user may appear: a marker without requests has no row.
function check_endpoint_requests() {
    echo "--- requests to the endpoint"
    ${CLICKHOUSE_CLIENT} -m -q "
        SYSTEM FLUSH LOGS query_log;
        SELECT extract(query, 'marker_([a-z0-9_]+)') AS marker, count() > 0
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT 1 AS x, %marker_%'
        GROUP BY marker
        ORDER BY marker;
    "
}

create_users
for user in "${USER_READONLY_1}" "${USER_READONLY_2}" "${USER_READONLY_0}"; do
    check_user "${user}"
done
check_endpoint_requests
drop_users
