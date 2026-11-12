#!/usr/bin/env bash
# A nested Buffer chain must not let a user insert into a destination table they have no INSERT grant on,
# and the per-identity flush separation must not wedge a shard or drop rows of destinationless buffers.
# https://github.com/ClickHouse/clickhouse-private/issues/87440

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

low="user_05621_low_${CLICKHOUSE_DATABASE}"
ok="user_05621_ok_${CLICKHOUSE_DATABASE}"
tmp="user_05621_tmp_${CLICKHOUSE_DATABASE}"
victim_db="victim_05621_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"
CH="${CLICKHOUSE_CLIENT}"

run_low() { ${CLICKHOUSE_CLIENT} --user "${low}" "$@"; }
run_ok()  { ${CLICKHOUSE_CLIENT} --user "${ok}" "$@"; }
run_tmp() { ${CLICKHOUSE_CLIENT} --user "${tmp}" "$@"; }
denied()  { if "$@" 2>&1 | grep -q "ACCESS_DENIED"; then echo "OK"; else echo "UNEXPECTED"; fi; }
victim()  { ${CH} --query "SELECT groupArray(s) FROM (SELECT s FROM ${victim_db}.victim ORDER BY s)"; }

${CH} --multiquery <<EOF
DROP DATABASE IF EXISTS ${victim_db};
CREATE DATABASE ${victim_db};
CREATE TABLE ${victim_db}.victim (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO ${victim_db}.victim VALUES ('original');

DROP USER IF EXISTS ${low}; DROP USER IF EXISTS ${ok}; DROP USER IF EXISTS ${tmp};
CREATE USER ${low}; CREATE USER ${ok}; CREATE USER ${tmp};
GRANT CREATE TABLE, INSERT, SELECT ON ${db}.* TO ${low}, ${ok}, ${tmp};
GRANT TABLE ENGINE ON Buffer TO ${low}, ${ok}, ${tmp};
GRANT INSERT ON ${victim_db}.victim TO ${ok}, ${tmp};
EOF

echo "## Part A: two-hop bypass is denied (direct-write path)"
# Explicit column lists avoid any inference that would need SHOW COLUMNS on the destination.
run_low --multiquery <<EOF
CREATE TABLE ${db}.inner_buffer (s String) ENGINE = Buffer(${victim_db}, victim, 1, 100, 100, 1, 1, 1, 1000000000);
CREATE TABLE ${db}.outer_buffer (s String) ENGINE = Buffer(${db}, inner_buffer, 1, 100, 100, 1, 1, 1, 1000000000);
EOF
echo -n "direct insert denied:   "; denied run_low --query "INSERT INTO ${victim_db}.victim VALUES ('direct')"
echo -n "inner buffer denied:    "; denied run_low --query "INSERT INTO ${db}.inner_buffer VALUES ('one_hop')"
echo -n "outer buffer denied:    "; denied run_low --query "INSERT INTO ${db}.outer_buffer VALUES ('two_a'),('two_b')"
echo -n "victim unchanged:       "; victim

echo "## Part B: an unauthorized buffered row is discarded and does not wedge the shard"
# Large thresholds keep a single row buffered; OPTIMIZE forces a synchronous flush.
run_low --query "CREATE TABLE ${db}.inner_b (s String) ENGINE = Buffer(${victim_db}, victim, 1, 3600, 7200, 1000000, 2000000, 100000000, 200000000)"
run_low --query "CREATE TABLE ${db}.outer_b (s String) ENGINE = Buffer(${db}, inner_b, 1, 3600, 7200, 1000000, 2000000, 100000000, 200000000)"
run_low --query "INSERT INTO ${db}.outer_b VALUES ('poison')"   # buffered under the low user
# The authorized user's insert flushes the low user's incompatible row first; it is discarded, not wedged.
run_ok  --query "INSERT INTO ${db}.outer_b VALUES ('legit_b')"
${CH} --query "OPTIMIZE TABLE ${db}.outer_b" && echo "outer_b OPTIMIZE: OK"
${CH} --query "OPTIMIZE TABLE ${db}.inner_b" && echo "inner_b OPTIMIZE: OK"
echo -n "victim after Part B:    "; victim

echo "## Part C: an invalid (dropped) buffered identity does not block a later valid writer"
run_ok --query "CREATE TABLE ${db}.single_b (s String) ENGINE = Buffer(${victim_db}, victim, 1, 3600, 7200, 1000000, 2000000, 100000000, 200000000)"
run_tmp --query "INSERT INTO ${db}.single_b VALUES ('from_tmp')"   # buffered under tmp, which can write to victim
${CH} --query "DROP USER ${tmp}"                                   # tmp's identity can never be restored now
run_ok --query "INSERT INTO ${db}.single_b VALUES ('from_ok')" && echo "valid writer after dropped identity: OK"
${CH} --query "OPTIMIZE TABLE ${db}.single_b" && echo "single_b OPTIMIZE: OK"
echo -n "victim after Part C:    "; victim

echo "## Part D: destinationless buffer keeps rows from different users"
run_low --query "CREATE TABLE ${db}.windowed (s String) ENGINE = Buffer('', '', 1, 3600, 7200, 1000000, 2000000, 100000000, 200000000)"
run_low --query "INSERT INTO ${db}.windowed VALUES ('win_low')"
run_ok  --query "INSERT INTO ${db}.windowed VALUES ('win_ok')"
echo -n "destinationless rows:   "; ${CH} --query "SELECT groupArray(s) FROM (SELECT s FROM ${db}.windowed ORDER BY s)"

${CH} --query "DROP USER IF EXISTS ${low}; DROP USER IF EXISTS ${ok}; DROP USER IF EXISTS ${tmp}" 2>/dev/null
${CH} --query "DROP DATABASE IF EXISTS ${victim_db}"
