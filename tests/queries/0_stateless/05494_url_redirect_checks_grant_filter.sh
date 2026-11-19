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
user_legacy="user05494_legacy_${CLICKHOUSE_DATABASE}_$RANDOM"
redirector="http://localhost:${CLICKHOUSE_PORT_HTTP}/upyachka"
escaped_redirector="http://localhost:${CLICKHOUSE_PORT_HTTP}/upyachka_escaped"
head_redirector="http://localhost:${CLICKHOUSE_PORT_HTTP}/upyachka_head"

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user_filtered, $user_full, $user_engine, $user_table, $user_legacy;
CREATE USER $user_filtered;
CREATE USER $user_full;
CREATE USER $user_engine;
CREATE USER $user_table;
CREATE USER $user_legacy;
GRANT CREATE TEMPORARY TABLE ON *.*, CLUSTER ON *.*, CREATE TABLE ON ${CLICKHOUSE_DATABASE}.* TO $user_legacy;
GRANT TABLE ENGINE ON URL TO $user_legacy;
GRANT READ ON URL TO $user_legacy;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user_filtered, $user_full;
GRANT CLUSTER ON *.* TO $user_filtered, $user_full;
GRANT READ ON URL('http://localhost:${CLICKHOUSE_PORT_HTTP}/upyachka') TO $user_filtered;
GRANT READ ON URL('http://localhost:${CLICKHOUSE_PORT_HTTP}/upyachka.*') TO $user_filtered;
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
# The legacy `ON CLUSTER` entry format executes the function on the workers with no user.
check_missing_grant "$user_filtered" "CREATE TABLE t_as_function_denied ON CLUSTER test_shard_localhost AS url('$redirector', 'LineAsString', 's String')" --distributed_ddl_entry_format_version 2 --distributed_ddl_output_mode throw
# The legacy format also runs the checks `execute` runs locally, and the vetoes of a definition.
check_missing_grant "$user_legacy" "CREATE TABLE t_as_function_denied ON CLUSTER test_shard_localhost AS url('$redirector', 'LineAsString', 's String')" --distributed_ddl_entry_format_version 2 --distributed_ddl_output_mode throw
${CLICKHOUSE_CLIENT} --user "$user_legacy" --distributed_ddl_entry_format_version 2 --distributed_ddl_output_mode throw --query "CREATE TABLE t_as_function_denied ON CLUSTER test_shard_localhost AS viewIfPermitted(SELECT 1 ELSE null('x UInt8'))" 2>&1 | grep -c -m1 "cannot be used to create a table"
# The shards resolve a target nested in `remote` under the connection's user.
${CLICKHOUSE_CLIENT} --query "GRANT REMOTE ON *.* TO $user_filtered"
check_missing_grant "$user_filtered" "CREATE TABLE t_as_function_denied AS remote('127.0.0.1', url('$redirector', 'LineAsString', 's String'))"
check_missing_grant "$user_filtered" "CREATE TABLE t_as_function_denied AS remote('127.0.0.1', loop(url('$redirector', 'LineAsString', 's String')))"
check_missing_grant "$user_filtered" "CREATE TABLE t_as_function_denied AS remote('127.0.0.1', view(SELECT * FROM url('$redirector', 'LineAsString', 's String')))"
# A subquery in an argument of another table function reads the target too.
check_missing_grant "$user_filtered" "CREATE TABLE t_as_function_denied (s String) AS remote('127.0.0.1:1', view(SELECT * FROM values('s String', (SELECT s FROM url('$redirector', 'LineAsString', 's String') LIMIT 1))))"
check_missing_grant "$user_filtered" "CREATE TABLE t_as_function_denied (number UInt64) AS remote('127.0.0.1:1', view(SELECT * FROM numbers((SELECT length(s) FROM url('$redirector', 'LineAsString', 's String') LIMIT 1))))"
check_missing_grant "$user_filtered" "CREATE TABLE t_as_function_denied (s String) AS remote('127.0.0.1:1', view(SELECT * FROM loop(values('s String', (SELECT s FROM url('$redirector', 'LineAsString', 's String') LIMIT 1)))))"
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
# A stored definition is replayed without the `READ` grant this check needs.
${CLICKHOUSE_CLIENT} --query "REVOKE READ ON URL FROM $user_engine"
${CLICKHOUSE_CLIENT} --user "$user_engine" --query "DETACH TABLE t_as_function_created"
${CLICKHOUSE_CLIENT} --user "$user_engine" --query "ATTACH TABLE t_as_function_created"
${CLICKHOUSE_CLIENT} --user "$user_engine" --max_http_get_redirects 1 --query "SELECT * FROM t_as_function_created"

echo "--- a parameterized view in a persisted view is not a table function"
${CLICKHOUSE_CLIENT} --query "CREATE VIEW pv_05494 AS SELECT {x:UInt64} AS x"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_view_pv AS view(SELECT * FROM pv_05494(x = 1))"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE t_view_pv"
# A URL bound by `WITH` is checked as the literal it stands for.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_view_with AS view(WITH '$redirector' AS u SELECT * FROM url(u, 'LineAsString', 's String'))"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE t_view_with"
# A nested view, and a view over a table that exists only on the remote shard.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_view_nested AS view(SELECT * FROM view(SELECT * FROM numbers(1)))"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE t_view_nested"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_view_remote_only (x UInt64) AS remote('127.0.0.1:1', view(SELECT x FROM ${CLICKHOUSE_DATABASE}.remote_only_05494))"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE t_view_remote_only"

echo "--- a restored table created AS url needs the grants too"
backup="Disk('backups', '05494_${CLICKHOUSE_TEST_UNIQUE_NAME}_$$')"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t_redirect_as_function TO $backup FORMAT Null"
${CLICKHOUSE_CLIENT} --query "GRANT READ ON DISK TO $user_filtered"
check_missing_grant "$user_filtered" "RESTORE TABLE ${CLICKHOUSE_DATABASE}.t_redirect_as_function AS ${CLICKHOUSE_DATABASE}.t_restored FROM $backup FORMAT Null"
# A definition is also checked for `WRITE` on its URL, as before.
${CLICKHOUSE_CLIENT} --query "GRANT WRITE ON URL('http://localhost:${CLICKHOUSE_PORT_HTTP}/upyachka') TO $user_filtered"
${CLICKHOUSE_CLIENT} --query "GRANT TABLE ENGINE ON URL TO $user_filtered"
${CLICKHOUSE_CLIENT} --user "$user_filtered" --query "RESTORE TABLE ${CLICKHOUSE_DATABASE}.t_redirect_as_function AS ${CLICKHOUSE_DATABASE}.t_restored FROM $backup FORMAT Null"
${CLICKHOUSE_CLIENT} --max_http_get_redirects 1 --query "SELECT * FROM t_restored"

echo "--- an argument bound by an alias in a persisted view is checked as its value"
# The filtered grant matches the literal, not the name `u`, so this creation proves the substitution.
${CLICKHOUSE_CLIENT} --user "$user_filtered" --query "CREATE TABLE t_view_with_filtered AS remote('127.0.0.1', view(WITH '$redirector' AS u SELECT * FROM url(u, 'LineAsString', 's String')))"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE t_view_with_filtered"
# A chain of aliases and a constant expression resolve to their value; a value taken from a column has none.
${CLICKHOUSE_CLIENT} --user "$user_filtered" --query "CREATE TABLE t_view_with_chain AS remote('127.0.0.1', view(WITH '$redirector' AS u, u AS v SELECT * FROM url(v, 'LineAsString', 's String')))"
${CLICKHOUSE_CLIENT} --user "$user_filtered" --query "CREATE TABLE t_view_with_concat AS remote('127.0.0.1', view(WITH concat('http://localhost:${CLICKHOUSE_PORT_HTTP}/', 'upyachka') AS u SELECT * FROM url(concat(u, ''), 'LineAsString', 's String')))"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE t_view_with_chain"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE t_view_with_concat"
# A sharding key may vary; only the source argument may not.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_view_sharding_key AS view(SELECT * FROM remote('127.0.0.1', system.one, rand()))"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE t_view_sharding_key"
${CLICKHOUSE_CLIENT} --user "$user_filtered" --query "CREATE TABLE t_view_with_denied AS remote('127.0.0.1', view(WITH toString(number) AS u SELECT * FROM url(u, 'LineAsString', 's String')))" 2>&1 | grep -c -m1 "Unknown expression or function identifier"
# A value that differs where the query runs, and an alias of an inner query, are not taken from the outer scope.
${CLICKHOUSE_CLIENT} --user "$user_filtered" --query "CREATE TABLE t_view_with_denied AS remote('127.0.0.1', view(WITH if(currentUser() = 'x', 'http://x', '$redirector') AS u SELECT * FROM url(u, 'LineAsString', 's String')))" 2>&1 | grep -c -m1 "depends on where the query runs"
${CLICKHOUSE_CLIENT} --user "$user_filtered" --query "CREATE TABLE t_view_with_denied AS remote('127.0.0.1', view(WITH '$redirector' AS u SELECT * FROM view(WITH 'http://localhost:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+1' AS u SELECT * FROM url(u, 'LineAsString', 's String'))))" 2>&1 | grep -c -m1 "Not enough privileges"
${CLICKHOUSE_CLIENT} --user "$user_filtered" --query "CREATE TABLE t_view_with_denied AS remote('127.0.0.1', view(SELECT 'http://x' AS u FROM url(u, 'LineAsString', 's String')))" 2>&1 | grep -c -m1 "Not enough privileges"
# A nested target keeps the fail-closes of a read: the cluster user of `urlCluster`, and a wildcard from index pages.
check_missing_grant "$user_filtered" "CREATE TABLE t_as_function_denied AS remote('127.0.0.1', urlCluster('test_shard_localhost', '$redirector', 'LineAsString', 's String'))"
check_missing_grant "$user_filtered" "CREATE TABLE t_as_function_denied AS remote('127.0.0.1', url('http://localhost:${CLICKHOUSE_PORT_HTTP}/upyachka*', 'RawBLOB', 's String'))" --allow_url_wildcard_from_index_pages 1
# A source that differs where the definition is loaded is refused for the outermost function too, and nested
# views are checked once each.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_view_with_denied (s String) AS url(if(currentUser() = 'x', 's3://b/k', '$redirector'), 'LineAsString', 's String')" 2>&1 | grep -c -m1 "depends on where the query runs"
${CLICKHOUSE_CLIENT} --query "CREATE NAMED COLLECTION nc_05494_${CLICKHOUSE_DATABASE} AS url = '$redirector'"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_view_with_denied (s String) AS url(nc_05494_${CLICKHOUSE_DATABASE}, url = if(currentUser() = 'x', 's3://b/k', '$redirector'), format = 'LineAsString', structure = 's String')" 2>&1 | grep -c -m1 "depends on where the query runs"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_view_with_denied (s String) AS url(nc_05494_${CLICKHOUSE_DATABASE}, concat('u', 'rl') = if(currentUser() = 'x', 's3://b/k', '$redirector'), format = 'LineAsString', structure = 's String')" 2>&1 | grep -c -m1 "depends on where the query runs"
${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION nc_05494_${CLICKHOUSE_DATABASE}"
# A relative URL is resolved against `url_base` where the query runs.
${CLICKHOUSE_CLIENT} --url_base "http://localhost:${CLICKHOUSE_PORT_HTTP}/" --query "CREATE TABLE t_view_with_denied (s String) AS remote('127.0.0.1:1', view(SELECT * FROM (SELECT * FROM url('upyachka', 'LineAsString', 's String') SETTINGS url_base = 'http://localhost:${CLICKHOUSE_PORT_HTTP}/')))" 2>&1 | grep -c -m1 "resolved against"
deep="numbers(1)"
for _ in $(seq 1 25); do deep="view(SELECT * FROM $deep)"; done
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_view_deep (number UInt64) AS remote('127.0.0.1:1', $deep)"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE t_view_deep"
# The veto of a function that the reader's grants resolve holds at any depth.
${CLICKHOUSE_CLIENT} --user "$user_filtered" --query "CREATE TABLE t_view_with_denied (x UInt8) AS remote('127.0.0.1:1', view(SELECT * FROM viewIfPermitted(SELECT 1 AS x ELSE null('x UInt8'))))" 2>&1 | grep -c -m1 "cannot be used to create a table"
${CLICKHOUSE_CLIENT} --user "$user_filtered" --query "CREATE TABLE t_view_with_denied (x UInt8) AS remote('127.0.0.1:1', view(SELECT * FROM eval('SELECT 1 AS x')))" --enable_eval_table_function 1 2>&1 | grep -c -m1 "cannot be used to create a table"
# The source of a cluster function is its second argument.
${CLICKHOUSE_CLIENT} --user "$user_filtered" --query "CREATE TABLE t_view_with_denied (s String) AS remote('127.0.0.1:1', view(SELECT * FROM urlCluster('test_shard_localhost', if(currentUser() = 'x', '$redirector', 'http://x'), 'LineAsString', 's String')))" 2>&1 | grep -c -m1 "depends on where the query runs"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_redirect"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_redirect_inferred"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_redirect_as_function, t_redirect_as_function_lazy, t_as_function_created, t_remote_src, t_restored, t_view_pv, t_view_with, t_view_nested, t_view_remote_only, t_view_with_filtered, t_view_with_chain, t_view_with_concat, t_view_sharding_key, t_view_deep"
${CLICKHOUSE_CLIENT} --query "DROP VIEW pv_05494"
${CLICKHOUSE_CLIENT} --query "DROP USER $user_filtered, $user_full, $user_engine, $user_table, $user_legacy"
