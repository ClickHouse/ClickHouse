#!/usr/bin/env bash
# An insert into a Buffer is denied when its user may not write where the flush would write. A batch whose user lost a
# privilege before the flush is dropped with an error logged, without an exception and without being written, while a
# denial that is not about the privileges of the user keeps the batch buffered.
# https://github.com/ClickHouse/clickhouse-private/issues/87440

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}"
protected_db="protected_05813_${CLICKHOUSE_DATABASE}"
user="u_05813_${CLICKHOUSE_DATABASE}"
definer_ok="u_05813_definer_ok_${CLICKHOUSE_DATABASE}"
definer_no="u_05813_definer_no_${CLICKHOUSE_DATABASE}"
nc="nc_05813_${CLICKHOUSE_DATABASE}"

run_user() { ${CLICKHOUSE_CLIENT} --user "${user}" "$@"; }
denied() { if "$@" 2>&1 | grep -q "ACCESS_DENIED"; then echo "OK"; else echo "UNEXPECTED"; fi; }

${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP DATABASE IF EXISTS ${protected_db};
CREATE DATABASE ${protected_db};
CREATE TABLE ${protected_db}.t (s String) ENGINE = MergeTree ORDER BY s;
DROP USER IF EXISTS ${user}, ${definer_ok}, ${definer_no};
CREATE USER ${user}, ${definer_ok}, ${definer_no};
GRANT INSERT, SELECT, OPTIMIZE ON ${db}.* TO ${user};
EOF

echo "## The local shard of a Distributed destination"
${CLICKHOUSE_CLIENT} --multiquery <<EOF
CREATE TABLE ${db}.dist (s String) ENGINE = Distributed(test_shard_localhost, ${protected_db}, t);
CREATE TABLE ${db}.dist_buf (s String) ENGINE = Buffer(${db}, dist, 1, 3600, 3600, 1000000, 1000000, 1000000000, 1000000000);
EOF
echo -n "insert denied:              "
denied run_user --query "INSERT INTO ${db}.dist_buf SETTINGS async_insert = 0, prefer_localhost_replica = 1 VALUES ('dist')"
echo -n "nothing buffered:           "; ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${db}.dist_buf WHERE s = 'dist'"
${CLICKHOUSE_CLIENT} --query "GRANT INSERT ON ${protected_db}.t TO ${user}"
run_user --query "INSERT INTO ${db}.dist_buf SETTINGS async_insert = 0, prefer_localhost_replica = 1 VALUES ('dist')"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE ${db}.dist_buf"
echo -n "written once allowed:       "; ${CLICKHOUSE_CLIENT} --query "SELECT groupArray(s) FROM ${protected_db}.t"
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
DROP USER ${user}, ${definer_ok}, ${definer_no};
DROP NAMED COLLECTION ${nc};
DROP DATABASE ${protected_db};
EOF
