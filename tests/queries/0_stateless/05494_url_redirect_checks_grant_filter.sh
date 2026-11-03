#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-replicated-database: the users are created locally.

# `READ ON URL('regex')` is checked for the URL given to `url`, and also for every redirect target.
# The test server redirects `/upyachka` to `/?query=SELECT+'Pepyaka'`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_filtered="user05494_filtered_${CLICKHOUSE_DATABASE}_$RANDOM"
user_full="user05494_full_${CLICKHOUSE_DATABASE}_$RANDOM"
user_engine="user05494_engine_${CLICKHOUSE_DATABASE}_$RANDOM"
redirector="http://localhost:${CLICKHOUSE_PORT_HTTP}/upyachka"

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user_filtered, $user_full, $user_engine;
CREATE USER $user_filtered;
CREATE USER $user_full;
CREATE USER $user_engine;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user_filtered, $user_full;
GRANT READ ON URL('http://localhost:${CLICKHOUSE_PORT_HTTP}/upyachka') TO $user_filtered;
GRANT READ ON URL TO $user_full;
CREATE TABLE t_redirect (s String) ENGINE = URL('$redirector', 'LineAsString');
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_redirect TO $user_filtered;
GRANT CREATE TABLE, DROP TABLE, SELECT ON ${CLICKHOUSE_DATABASE}.* TO $user_engine;
GRANT TABLE ENGINE ON URL TO $user_engine;
EOF

function check_denied()
{
    ${CLICKHOUSE_CLIENT} --user "$1" --max_http_get_redirects 1 --query "$2" 2>&1 | grep -c -m1 "Not enough privileges"
}

echo "--- the redirect target outside of the grant"
check_denied "$user_filtered" "SELECT * FROM url('http://localhost:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+1', 'RawBLOB', 's String')"
check_denied "$user_filtered" "SELECT * FROM url('$redirector', 'RawBLOB', 's String')"
check_denied "$user_filtered" "SELECT * FROM url('$redirector', 'RawBLOB')"

echo "--- the grant without a filter"
${CLICKHOUSE_CLIENT} --user "$user_full" --max_http_get_redirects 1 --query "SELECT * FROM url('$redirector', 'LineAsString', 's String')"

echo "--- a URL table reads with the grant on the table"
${CLICKHOUSE_CLIENT} --user "$user_filtered" --max_http_get_redirects 1 --query "SELECT * FROM t_redirect"

echo "--- a URL table infers its structure with the grant on the engine"
${CLICKHOUSE_CLIENT} --user "$user_engine" --max_http_get_redirects 1 --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.t_redirect_inferred ENGINE = URL('$redirector', 'TSV')"
${CLICKHOUSE_CLIENT} --user "$user_engine" --max_http_get_redirects 1 --query "SELECT * FROM ${CLICKHOUSE_DATABASE}.t_redirect_inferred"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_redirect"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_redirect_inferred"
${CLICKHOUSE_CLIENT} --query "DROP USER $user_filtered, $user_full, $user_engine"
