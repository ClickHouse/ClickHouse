#!/usr/bin/env bash
# An insert into a Buffer is denied when its user may not write where the flush would write. A batch whose user lost a
# privilege before the flush is dropped with an error logged, without an exception and without being written, while a
# denial that is not about the privileges of the user keeps the batch buffered.
# https://github.com/ClickHouse/clickhouse-private/issues/87440

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# A write that skips a failing view with `materialized_views_ignore_errors` logs a warning for it; errors are still shown.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=error
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}"
protected_db="protected_05813_${CLICKHOUSE_DATABASE}"
user="u_05813_${CLICKHOUSE_DATABASE}"
definer_ok="u_05813_definer_ok_${CLICKHOUSE_DATABASE}"
definer_no="u_05813_definer_no_${CLICKHOUSE_DATABASE}"
shard_user="u_05813_shard_${CLICKHOUSE_DATABASE}"
nc="nc_05813_${CLICKHOUSE_DATABASE}"

run_user() { ${CLICKHOUSE_CLIENT} --user "${user}" "$@"; }
run_shard_user() { ${CLICKHOUSE_CLIENT} --user "${shard_user}" "$@"; }
denied() { if "$@" 2>&1 | grep -q "ACCESS_DENIED"; then echo "OK"; else echo "UNEXPECTED"; fi; }

${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP DATABASE IF EXISTS ${protected_db};
CREATE DATABASE ${protected_db};
CREATE TABLE ${protected_db}.t (s String) ENGINE = MergeTree ORDER BY s;
DROP USER IF EXISTS ${user}, ${definer_ok}, ${definer_no}, ${shard_user};
-- The flush writes the shard of a Distributed table with a local replica here, as the user, if the user's profile says
-- so; the insert checks it the same way.
CREATE USER ${user} SETTINGS prefer_localhost_replica = 1;
CREATE USER ${definer_ok}, ${definer_no};
GRANT INSERT, SELECT, OPTIMIZE ON ${db}.* TO ${user};
-- The flush runs in the default database of the user, where a table named without a database is.
CREATE USER ${shard_user} DEFAULT DATABASE ${protected_db} SETTINGS prefer_localhost_replica = 1;
GRANT INSERT ON ${db}.* TO ${shard_user};
EOF

echo "## The local shard of a Distributed destination"
${CLICKHOUSE_CLIENT} --multiquery <<EOF
CREATE TABLE ${db}.dist (s String) ENGINE = Distributed(test_shard_localhost, ${protected_db}, t);
CREATE TABLE ${db}.dist_buf (s String) ENGINE = Buffer(${db}, dist, 1, 3600, 3600, 1000000, 1000000, 1000000000, 1000000000);
EOF
echo -n "insert denied:              "
denied run_user --query "INSERT INTO ${db}.dist_buf SETTINGS async_insert = 0 VALUES ('dist')"
echo -n "nothing buffered:           "; ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${db}.dist_buf WHERE s = 'dist'"
${CLICKHOUSE_CLIENT} --query "GRANT INSERT ON ${protected_db}.t TO ${user}"
run_user --query "INSERT INTO ${db}.dist_buf SETTINGS async_insert = 0 VALUES ('dist')"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE ${db}.dist_buf"
echo -n "written once allowed:       "; ${CLICKHOUSE_CLIENT} --query "SELECT groupArray(s) FROM ${protected_db}.t"
${CLICKHOUSE_CLIENT} --query "REVOKE INSERT ON ${protected_db}.t FROM ${user}"

echo "## The local shard of a Distributed destination naming its table without a database"
${CLICKHOUSE_CLIENT} --multiquery <<EOF
CREATE TABLE ${db}.dist_nodb (s String) ENGINE = Distributed(test_shard_localhost, '', t);
CREATE TABLE ${db}.dist_nodb_buf (s String) ENGINE = Buffer(${db}, dist_nodb, 1, 3600, 3600, 1000000, 1000000, 1000000000, 1000000000);
EOF
echo -n "insert denied:              "
denied run_shard_user --query "INSERT INTO ${db}.dist_nodb_buf SETTINGS async_insert = 0 VALUES ('nodb')"
# A read of a buffer reads its destination too; `total_rows` counts only the rows buffered in it.
echo -n "nothing buffered:           "
${CLICKHOUSE_CLIENT} --query "SELECT total_rows FROM system.tables WHERE database = '${db}' AND name = 'dist_nodb_buf'"
${CLICKHOUSE_CLIENT} --query "GRANT INSERT ON ${protected_db}.t TO ${shard_user}"
run_shard_user --query "INSERT INTO ${db}.dist_nodb_buf SETTINGS async_insert = 0 VALUES ('nodb')"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE ${db}.dist_nodb_buf"
echo -n "written once allowed:       "; ${CLICKHOUSE_CLIENT} --query "SELECT groupArray(s) FROM ${protected_db}.t WHERE s = 'nodb'"
${CLICKHOUSE_CLIENT} --query "REVOKE INSERT ON ${protected_db}.t FROM ${shard_user}"

echo "## A Distributed destination whose write selects only a remote shard"
# Shard 1 of test_unavailable_shard is local and shard 2 is not; `insert_shard_id` selects the shard the write goes to.
${CLICKHOUSE_CLIENT} --multiquery <<EOF
CREATE TABLE ${db}.dist_remote (s String) ENGINE = Distributed(test_unavailable_shard, ${protected_db}, t);
CREATE TABLE ${db}.dist_remote_buf (s String) ENGINE = Buffer(${db}, dist_remote, 1, 3600, 3600, 1000000, 1000000, 1000000000, 1000000000);
ALTER USER ${shard_user} SETTINGS prefer_localhost_replica = 1, insert_shard_id = 2;
EOF
echo -n "insert accepted:            "
if run_shard_user --query "INSERT INTO ${db}.dist_remote_buf SETTINGS async_insert = 0 VALUES ('remote')"; then echo "OK"; fi
echo -n "buffered:                   "
${CLICKHOUSE_CLIENT} --query "SELECT total_rows FROM system.tables WHERE database = '${db}' AND name = 'dist_remote_buf'"
${CLICKHOUSE_CLIENT} --query "ALTER USER ${shard_user} SETTINGS prefer_localhost_replica = 1, insert_shard_id = 1"
echo -n "local shard selected:       "
denied run_shard_user --query "INSERT INTO ${db}.dist_remote_buf SETTINGS async_insert = 0 VALUES ('local')"
# Flushed to the local shard, which the profile of the user selects now.
${CLICKHOUSE_CLIENT} --query "GRANT INSERT ON ${protected_db}.t TO ${shard_user}"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE ${db}.dist_remote_buf"
echo -n "written once allowed:       "; ${CLICKHOUSE_CLIENT} --query "SELECT groupArray(s) FROM ${protected_db}.t WHERE s IN ('remote', 'local')"

echo "## The local shard of a table created from a table function"
${CLICKHOUSE_CLIENT} --multiquery <<EOF
CREATE TABLE ${db}.remote_t AS remote('127.0.0.1', ${protected_db}, t);
CREATE TABLE ${db}.remote_buf (s String) ENGINE = Buffer(${db}, remote_t, 1, 3600, 3600, 1000000, 1000000, 1000000000, 1000000000);
EOF
echo -n "insert denied:              "
denied run_user --query "INSERT INTO ${db}.remote_buf SETTINGS async_insert = 0 VALUES ('proxy')"
echo -n "nothing buffered:           "
${CLICKHOUSE_CLIENT} --query "SELECT total_rows FROM system.tables WHERE database = '${db}' AND name = 'remote_buf'"
${CLICKHOUSE_CLIENT} --query "GRANT INSERT ON ${protected_db}.t TO ${user}"
run_user --query "INSERT INTO ${db}.remote_buf SETTINGS async_insert = 0 VALUES ('proxy')"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE ${db}.remote_buf"
echo -n "written once allowed:       "; ${CLICKHOUSE_CLIENT} --query "SELECT groupArray(s) FROM ${protected_db}.t WHERE s = 'proxy'"
${CLICKHOUSE_CLIENT} --query "REVOKE INSERT ON ${protected_db}.t FROM ${user}"

echo "## A materialized view on the destination writes with the rights of its definer"
${CLICKHOUSE_CLIENT} --multiquery <<EOF
CREATE TABLE ${db}.src_ok (s String) ENGINE = MergeTree ORDER BY s;
CREATE TABLE ${db}.src_no (s String) ENGINE = MergeTree ORDER BY s;
CREATE TABLE ${protected_db}.mv_ok_dst (s String) ENGINE = MergeTree ORDER BY s;
CREATE TABLE ${protected_db}.mv_no_dst (s String) ENGINE = MergeTree ORDER BY s;
GRANT SELECT ON ${db}.src_ok TO ${definer_ok};
GRANT INSERT ON ${protected_db}.mv_ok_dst TO ${definer_ok};
GRANT SELECT ON ${db}.src_no TO ${definer_no};
CREATE MATERIALIZED VIEW ${db}.mv_ok TO ${protected_db}.mv_ok_dst DEFINER = ${definer_ok} SQL SECURITY DEFINER AS SELECT s FROM ${db}.src_ok;
CREATE MATERIALIZED VIEW ${db}.mv_no TO ${protected_db}.mv_no_dst DEFINER = ${definer_no} SQL SECURITY DEFINER AS SELECT s FROM ${db}.src_no;
CREATE TABLE ${db}.buf_ok (s String) ENGINE = Buffer(${db}, src_ok, 1, 3600, 3600, 1000000, 1000000, 1000000000, 1000000000);
CREATE TABLE ${db}.buf_no (s String) ENGINE = Buffer(${db}, src_no, 1, 3600, 3600, 1000000, 1000000, 1000000000, 1000000000);
EOF
# The user may write to neither target, but needs to only when the definer may not, as for a direct insert.
run_user --query "INSERT INTO ${db}.buf_ok SETTINGS async_insert = 0 VALUES ('mv')"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE ${db}.buf_ok"
echo -n "written by the view:        "; ${CLICKHOUSE_CLIENT} --query "SELECT groupArray(s) FROM ${protected_db}.mv_ok_dst"
echo -n "direct insert denied:       "; denied run_user --query "INSERT INTO ${db}.src_no SETTINGS async_insert = 0 VALUES ('mv')"
echo -n "insert denied:              "; denied run_user --query "INSERT INTO ${db}.buf_no SETTINGS async_insert = 0 VALUES ('mv')"
echo -n "nothing buffered:           "; ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${db}.buf_no"
# With `materialized_views_ignore_errors` a direct insert skips the view its definer may not write, and so must the
# insert into the buffer and its flush. The setting is in the profile of the user, so the flush has it too.
ignore_user="u_05813_ignore_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP USER IF EXISTS ${ignore_user};
CREATE USER ${ignore_user} SETTINGS materialized_views_ignore_errors = 1;
GRANT INSERT ON ${db}.* TO ${ignore_user};
CREATE TABLE ${db}.buf_ignore (s String) ENGINE = Buffer(${db}, src_no, 1, 3600, 3600, 1000000, 1000000, 1000000000, 1000000000);
EOF
${CLICKHOUSE_CLIENT} --user "${ignore_user}" --query "INSERT INTO ${db}.src_no SETTINGS async_insert = 0 VALUES ('direct')"
${CLICKHOUSE_CLIENT} --user "${ignore_user}" --query "INSERT INTO ${db}.buf_ignore SETTINGS async_insert = 0 VALUES ('buffered')"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE ${db}.buf_ignore"
echo -n "view skipped, rows written: "; ${CLICKHOUSE_CLIENT} --query "SELECT groupArray(s) FROM (SELECT s FROM ${db}.src_no ORDER BY s)"
echo -n "view target untouched:      "; ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${protected_db}.mv_no_dst"

echo "## A privilege lost before the flush"
${CLICKHOUSE_CLIENT} --multiquery <<EOF
CREATE TABLE ${db}.lost_buf (s String) ENGINE = Buffer(${protected_db}, t, 1, 3600, 3600, 1000000, 1000000, 1000000000, 1000000000);
GRANT INSERT ON ${protected_db}.t TO ${user};
EOF
run_user --query "INSERT INTO ${db}.lost_buf SETTINGS async_insert = 0 VALUES ('lost')"
${CLICKHOUSE_CLIENT} --query "REVOKE INSERT ON ${protected_db}.t FROM ${user}"
# The flush of the user's own OPTIMIZE succeeds without writing the batch, and the client gets the error logged for it.
out=$(run_user --query "OPTIMIZE TABLE ${db}.lost_buf" 2>&1)
rc=$?
echo -n "flush succeeds:             "; [ "${rc}" -eq 0 ] && echo "OK" || echo "UNEXPECTED"
echo -n "error logged:               "; echo "${out}" | grep -q "<Error>.*may no longer write" && echo "OK" || echo "UNEXPECTED"
echo -n "not written:                "; ${CLICKHOUSE_CLIENT} --query "SELECT countIf(s = 'lost') FROM ${protected_db}.t"
echo -n "dropped:                    "; ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${db}.lost_buf WHERE s = 'lost'"

echo "## A denial that is not about the privileges of the user"
# A flush is a background operation, which rejects AI functions with ACCESS_DENIED although no privilege is missing. The
# batch is kept, the error reaches the flush, and the batch is written once the cause is gone.
${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP NAMED COLLECTION IF EXISTS ${nc};
CREATE NAMED COLLECTION ${nc} AS provider = 'openai', endpoint = 'http://localhost:1/v1/chat/completions', api_key = 'fake-key';
CREATE TABLE ${db}.ai_dst (x String, CONSTRAINT c CHECK aiFilter(x, 'c', map('credentials', '${nc}', 'model', 'm'))) ENGINE = MergeTree ORDER BY x;
CREATE TABLE ${db}.ai_buf (x String) ENGINE = Buffer(${db}, ai_dst, 1, 3600, 3600, 1000000, 1000000, 1000000000, 1000000000);
INSERT INTO ${db}.ai_buf SETTINGS async_insert = 0 VALUES ('a');
EOF
echo -n "flush denied:               "; denied ${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE ${db}.ai_buf"
echo -n "still buffered:             "; ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${db}.ai_buf"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${db}.ai_dst DROP CONSTRAINT c"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE ${db}.ai_buf"
echo -n "written once allowed:       "; ${CLICKHOUSE_CLIENT} --query "SELECT groupArray(x) FROM ${db}.ai_dst"

${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP TABLE ${db}.mv_ok;
DROP TABLE ${db}.mv_no;
DROP USER ${user}, ${definer_ok}, ${definer_no}, ${ignore_user}, ${shard_user};
DROP NAMED COLLECTION ${nc};
DROP DATABASE ${protected_db};
EOF
