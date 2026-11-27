#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-replicated-database: the users are created locally.

# `READ ON URL('regex')` is checked for the URL given to `url`, and also for every redirect target.
# The test server redirects `/upyachka` to `/?query=SELECT+'Pepyaka'`, and `/upyachka_escaped` to the
# same query behind an escaped dot segment, `/upyachka/%2e%2e/`. `/upyachka_head` answers `HEAD` itself
# with a `Last-Modified` and redirects only `GET`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_filtered="user05494_filtered_${CLICKHOUSE_DATABASE}_$RANDOM"
user_full="user05494_full_${CLICKHOUSE_DATABASE}_$RANDOM"
user_engine="user05494_engine_${CLICKHOUSE_DATABASE}_$RANDOM"
user_table="user05494_table_${CLICKHOUSE_DATABASE}_$RANDOM"
user_wide="user05494_wide_${CLICKHOUSE_DATABASE}_$RANDOM"
redirector="http://localhost:${CLICKHOUSE_PORT_HTTP}/upyachka"
escaped_redirector="http://localhost:${CLICKHOUSE_PORT_HTTP}/upyachka_escaped"
head_redirector="http://localhost:${CLICKHOUSE_PORT_HTTP}/upyachka_head"

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user_filtered, $user_full, $user_engine, $user_table, $user_wide;
CREATE USER $user_filtered;
CREATE USER $user_full;
CREATE USER $user_engine;
CREATE USER $user_table;
CREATE USER $user_wide;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user_filtered, $user_full, $user_wide;
GRANT READ ON URL('http://localhost:${CLICKHOUSE_PORT_HTTP}/upyachka') TO $user_filtered;
GRANT READ ON URL('http://localhost:${CLICKHOUSE_PORT_HTTP}/upyachka.*') TO $user_filtered;
GRANT READ ON URL TO $user_full;
GRANT READ ON URL('http://localhost:${CLICKHOUSE_PORT_HTTP}/.*') TO $user_wide;
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
    ${CLICKHOUSE_CLIENT} --user "$1" --max_http_get_redirects 1 --query "$2" "${@:3}" 2>&1 | grep -c -m1 "Not enough privileges"
}

function check_missing_grant()
{
    ${CLICKHOUSE_CLIENT} --user "$1" --query "$2" "${@:3}" 2>&1 | grep -o -m1 "grant [A-Z ]* ON URL"
}

echo "--- the redirect target outside of the grant"
check_denied "$user_filtered" "SELECT * FROM url('http://localhost:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+1', 'RawBLOB', 's String')"
check_denied "$user_filtered" "SELECT * FROM url('$redirector', 'RawBLOB', 's String')"
check_denied "$user_filtered" "SELECT * FROM url('$redirector', 'RawBLOB')"

echo "--- an escaped redirect target is checked as the request sends it"
check_denied "$user_filtered" "SELECT * FROM url('$escaped_redirector', 'RawBLOB', 's String')" --enable_url_encoding 0 --http_make_head_request 0
check_denied "$user_filtered" "SELECT * FROM url('$escaped_redirector', 'RawBLOB', 's String')" --enable_url_encoding 1 --http_make_head_request 0

echo "--- a wildcard expanded from index pages needs the grant on the whole source"
check_denied "$user_filtered" "SELECT * FROM url('http://localhost:${CLICKHOUSE_PORT_HTTP}/upyachka*', 'RawBLOB', 's String')" --allow_url_wildcard_from_index_pages 1

echo "--- the grant without a filter"
${CLICKHOUSE_CLIENT} --user "$user_full" --max_http_get_redirects 1 --query "SELECT * FROM url('$redirector', 'LineAsString', 's String')"

echo "--- a cluster read follows redirects as the cluster user"
check_missing_grant "$user_filtered" "SELECT * FROM urlCluster('test_shard_localhost', '$redirector', 'LineAsString', 's String')" --max_http_get_redirects 1
check_missing_grant "$user_filtered" "SELECT * FROM url('$redirector', 'LineAsString', 's String')" --max_http_get_redirects 1 --cluster_for_parallel_replicas test_shard_localhost --parallel_replicas_for_cluster_engines 1 --enable_parallel_replicas 1 --max_parallel_replicas 2
${CLICKHOUSE_CLIENT} --user "$user_full" --max_http_get_redirects 1 --query "SELECT * FROM urlCluster('test_shard_localhost', '$redirector', 'LineAsString', 's String')"
# With an interserver secret the workers run as the caller and check the redirects themselves.
parallel_replicas_with_secret=(--cluster_for_parallel_replicas test_cluster_interserver_secret --parallel_replicas_for_cluster_engines 1 --enable_parallel_replicas 1 --max_parallel_replicas 2)
check_denied "$user_filtered" "SELECT * FROM urlCluster('test_cluster_interserver_secret', '$redirector', 'LineAsString', 's String')" --http_make_head_request 0
check_denied "$user_filtered" "SELECT * FROM url('$redirector', 'LineAsString', 's String')" --http_make_head_request 0 "${parallel_replicas_with_secret[@]}"
${CLICKHOUSE_CLIENT} --user "$user_wide" --max_http_get_redirects 1 --http_make_head_request 0 --query "SELECT * FROM urlCluster('test_cluster_interserver_secret', '$redirector', 'LineAsString', 's String')"
${CLICKHOUSE_CLIENT} --user "$user_wide" --max_http_get_redirects 1 --http_make_head_request 0 "${parallel_replicas_with_secret[@]}" --query "SELECT * FROM url('$redirector', 'LineAsString', 's String')"

echo "--- a filtered grant takes nothing from the caches another user filled"
${CLICKHOUSE_CLIENT} --user "$user_full" --max_http_get_redirects 1 --query "DESCRIBE TABLE url('$redirector', 'TSV')" > /dev/null
${CLICKHOUSE_CLIENT} --user "$user_full" --max_http_get_redirects 1 --use_cache_for_count_from_files 1 --query "SELECT count() FROM url('$redirector', 'TSV', 's String')"
check_denied "$user_filtered" "DESCRIBE TABLE url('$redirector', 'TSV')" --http_make_head_request 0 --schema_inference_cache_require_modification_time_for_url 0
check_denied "$user_filtered" "SELECT count() FROM url('$redirector', 'TSV', 's String')" --http_make_head_request 0 --schema_inference_cache_require_modification_time_for_url 0 --use_cache_for_count_from_files 1
# A `HEAD` that answers with a modification time says nothing about where the `GET` redirects.
${CLICKHOUSE_CLIENT} --user "$user_full" --max_http_get_redirects 1 --query "DESCRIBE TABLE url('$head_redirector', 'TSV')" > /dev/null
${CLICKHOUSE_CLIENT} --user "$user_full" --max_http_get_redirects 1 --use_cache_for_count_from_files 1 --query "SELECT count() FROM url('$head_redirector', 'TSV', 's String')"
check_denied "$user_filtered" "DESCRIBE TABLE url('$head_redirector', 'TSV')" --schema_inference_cache_require_modification_time_for_url 0
check_denied "$user_filtered" "SELECT count() FROM url('$head_redirector', 'TSV', 's String')" --schema_inference_cache_require_modification_time_for_url 0 --use_cache_for_count_from_files 1

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
${CLICKHOUSE_CLIENT} --query "GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.* TO $user_filtered"
check_missing_grant "$user_filtered" "CREATE TABLE t_as_function_denied AS url('$redirector', 'LineAsString', 's String')"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_redirect"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_redirect_inferred"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_redirect_as_function, t_redirect_as_function_lazy"
${CLICKHOUSE_CLIENT} --query "DROP USER $user_filtered, $user_full, $user_engine, $user_table, $user_wide"
