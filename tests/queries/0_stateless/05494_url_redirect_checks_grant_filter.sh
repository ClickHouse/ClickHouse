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
user_table="user05494_table_${CLICKHOUSE_DATABASE}_$RANDOM"
redirector="http://localhost:${CLICKHOUSE_PORT_HTTP}/upyachka"

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user_filtered, $user_full, $user_engine, $user_table;
CREATE USER $user_filtered;
CREATE USER $user_full;
CREATE USER $user_engine;
CREATE USER $user_table;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user_filtered, $user_full;
GRANT READ ON URL('http://localhost:${CLICKHOUSE_PORT_HTTP}/upyachka') TO $user_filtered;
GRANT READ ON URL TO $user_full;
CREATE TABLE t_redirect (s String) ENGINE = URL('$redirector', 'LineAsString');
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_redirect TO $user_filtered;
GRANT CREATE TABLE, DROP TABLE, SELECT ON ${CLICKHOUSE_DATABASE}.* TO $user_engine;
GRANT TABLE ENGINE ON URL TO $user_engine;
CREATE TABLE t_redirect_as_function AS url('$redirector', 'LineAsString', 's String');
CREATE TABLE t_redirect_as_function_lazy (line String) AS url('$redirector', 'LineAsString');
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_redirect_as_function TO $user_table;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_redirect_as_function_lazy TO $user_table;
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

echo "--- a table created AS url reads with the grant on the table only"
${CLICKHOUSE_CLIENT} --user "$user_table" --max_http_get_redirects 1 --query "SELECT * FROM t_redirect_as_function"
${CLICKHOUSE_CLIENT} --user "$user_table" --max_http_get_redirects 1 --query "SELECT * FROM t_redirect_as_function_lazy"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_redirect_as_function_lazy"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_redirect_as_function_lazy"
${CLICKHOUSE_CLIENT} --user "$user_table" --max_http_get_redirects 1 --query "SELECT * FROM t_redirect_as_function_lazy"

echo "--- a URL table infers its structure with the grant on the engine"
${CLICKHOUSE_CLIENT} --user "$user_engine" --max_http_get_redirects 1 --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.t_redirect_inferred ENGINE = URL('$redirector', 'TSV')"
${CLICKHOUSE_CLIENT} --user "$user_engine" --max_http_get_redirects 1 --query "SELECT * FROM ${CLICKHOUSE_DATABASE}.t_redirect_inferred"

echo "--- a table created AS url needs the grants of a URL table"
function check_missing_grant()
{
    ${CLICKHOUSE_CLIENT} --user "$1" --query "$2" "${@:3}" 2>&1 | grep -o -m1 "grant [A-Z ]* ON URL"
}
${CLICKHOUSE_CLIENT} --query "GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.* TO $user_filtered"
check_missing_grant "$user_filtered" "CREATE TABLE t_as_function_denied AS url('$redirector', 'LineAsString', 's String')"
# The legacy `ON CLUSTER` entry format executes the function on the workers with no user.
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER ON *.* TO $user_filtered"
check_missing_grant "$user_filtered" "CREATE TABLE t_as_function_denied ON CLUSTER test_shard_localhost AS url('$redirector', 'LineAsString', 's String')" --distributed_ddl_entry_format_version 2 --distributed_ddl_output_mode throw
# The shards resolve a target nested in `remote` under the connection's user.
${CLICKHOUSE_CLIENT} --query "GRANT REMOTE ON *.* TO $user_filtered"
check_missing_grant "$user_filtered" "CREATE TABLE t_as_function_denied AS remote('127.0.0.1', url('$redirector', 'LineAsString', 's String'))"
check_missing_grant "$user_filtered" "CREATE TABLE t_as_function_denied AS remote('127.0.0.1', loop(url('$redirector', 'LineAsString', 's String')))"
${CLICKHOUSE_CLIENT} --query "GRANT TABLE ENGINE ON Remote TO $user_filtered"
check_missing_grant "$user_filtered" "CREATE TABLE t_as_function_denied (s String) ENGINE = Remote('127.0.0.1', url('$redirector', 'LineAsString', 's String'))"
check_missing_grant "$user_filtered" "CREATE TABLE t_as_function_denied ON CLUSTER test_shard_localhost (s String) ENGINE = Remote('127.0.0.1', url('$redirector', 'LineAsString', 's String'))" --distributed_ddl_entry_format_version 2 --distributed_ddl_output_mode throw
# The same, with the definition inherited from another table.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_remote_src (s String) ENGINE = Remote('127.0.0.1', url('$redirector', 'LineAsString', 's String'))"
${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS, SELECT ON ${CLICKHOUSE_DATABASE}.t_remote_src TO $user_filtered"
check_missing_grant "$user_filtered" "CREATE TABLE t_as_function_denied ON CLUSTER test_shard_localhost AS t_remote_src" --distributed_ddl_entry_format_version 2 --distributed_ddl_output_mode throw
# A column list skips the schema inference, which used to be the only check of READ.
${CLICKHOUSE_CLIENT} --query "GRANT WRITE ON URL TO $user_engine"
check_missing_grant "$user_engine" "CREATE TABLE t_as_function_denied (line String) AS url('$redirector', 'LineAsString')"
${CLICKHOUSE_CLIENT} --query "GRANT READ ON URL('http://localhost:${CLICKHOUSE_PORT_HTTP}/upyachka') TO $user_engine"
${CLICKHOUSE_CLIENT} --user "$user_engine" --query "CREATE TABLE t_as_function_created (line String) AS url('$redirector', 'LineAsString')"
# A stored definition is replayed without these two grants.
${CLICKHOUSE_CLIENT} --query "REVOKE READ ON URL FROM $user_engine"
${CLICKHOUSE_CLIENT} --query "REVOKE TABLE ENGINE ON URL FROM $user_engine"
${CLICKHOUSE_CLIENT} --user "$user_engine" --query "DETACH TABLE t_as_function_created"
${CLICKHOUSE_CLIENT} --user "$user_engine" --query "ATTACH TABLE t_as_function_created"
${CLICKHOUSE_CLIENT} --user "$user_engine" --max_http_get_redirects 1 --query "SELECT * FROM t_as_function_created"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_redirect"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_redirect_inferred"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_redirect_as_function, t_redirect_as_function_lazy, t_as_function_created, t_remote_src"
${CLICKHOUSE_CLIENT} --query "DROP USER $user_filtered, $user_full, $user_engine, $user_table"
