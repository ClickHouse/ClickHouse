#!/usr/bin/env bash
# A nested Buffer chain must not let a user insert into a destination table they have no INSERT grant on,
# and the per-identity flush separation must not wedge a shard or drop rows of destinationless buffers.
# https://github.com/ClickHouse/clickhouse-private/issues/87440

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# Run the client at warning level: undeliverable-batch discards are logged below warning (server-side only),
# so they do not reach this client, while Part E can still check that the Buffer -> Alias access check fires
# before any schema-revealing warning would be emitted.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=warning
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

echo "## Part E: a Buffer -> Alias chain does not leak target column names"
# low may insert into the buffer and the alias but not the alias's target (victim). Whether low declares the
# target's real column or an absent one, the oversized (direct) flush must deny it the same way, so the
# difference cannot be used as an oracle for which columns the target has.
${CH} --multiquery <<EOF
CREATE TABLE ${db}.al ENGINE = Alias('${victim_db}', 'victim');
CREATE TABLE ${db}.b_probe_real (s String) ENGINE = Buffer(${db}, al, 1, 1, 1, 1, 1, 1, 1);
CREATE TABLE ${db}.b_probe_fake (absent_col String) ENGINE = Buffer(${db}, al, 1, 1, 1, 1, 1, 1, 1);
CREATE TABLE ${db}.b_probe_partial (s String, absent_col String) ENGINE = Buffer(${db}, al, 1, 1, 1, 1, 1, 1, 1);
GRANT INSERT ON ${db}.al TO ${low};
GRANT INSERT ON ${db}.b_probe_real TO ${low};
GRANT INSERT ON ${db}.b_probe_fake TO ${low};
GRANT INSERT ON ${db}.b_probe_partial TO ${low};
EOF
echo -n "real column denied:     "; denied run_low --query "INSERT INTO ${db}.b_probe_real VALUES ('probe')"
echo -n "absent column denied:   "; denied run_low --query "INSERT INTO ${db}.b_probe_fake VALUES ('probe')"
# Partial intersection: declaring a real column (s) plus an absent one. At warning log level the
# "columns discarded" warning would reveal that s exists in the protected target, but the alias access
# check rejects the unauthorized user before any schema is read or warning emitted.
part=$(run_low --query "INSERT INTO ${db}.b_probe_partial VALUES ('a', 'b')" 2>&1)
echo -n "partial denied, no leak:"
if echo "$part" | grep -q "ACCESS_DENIED" && ! echo "$part" | grep -qi "columns"; then echo " OK"; else echo " UNEXPECTED"; fi
# A second alias hop must be authorized too, or it reopens the oracle. An alias cannot be created pointing at an
# existing alias, but a chain forms via creation order: al_chain -> al_mid (not yet existing), then al_mid ->
# victim. low has INSERT on the db (both aliases and the buffer) but not on victim.
${CH} --multiquery <<EOF
CREATE TABLE ${db}.al_chain ENGINE = Alias('${db}', 'al_mid');
CREATE TABLE ${db}.al_mid ENGINE = Alias('${victim_db}', 'victim');
EOF
run_low --query "CREATE TABLE ${db}.b_probe_chain (absent_col String) ENGINE = Buffer(${db}, al_chain, 1, 1, 1, 1, 1, 1, 1)"
echo -n "two-alias chain denied: "; denied run_low --query "INSERT INTO ${db}.b_probe_chain VALUES ('probe')"
echo -n "victim after Part E:    "; victim

echo "## Part F: a cross-identity discard does not leak the buffering user to the triggering user"
# low buffers a row while it may write to victim, then loses that grant. ok's synchronous insert then flushes
# low's now-undeliverable batch (discarded). At information log level the discard diagnostic is forwarded to ok,
# and it must not reveal low's name or required grants. Synchronous inserts keep the flush on ok's query thread.
${CH} --multiquery <<EOF
CREATE TABLE ${db}.single_hop (s String) ENGINE = Buffer(${victim_db}, victim, 1, 3600, 7200, 1000000, 2000000, 100000000, 200000000);
GRANT INSERT ON ${victim_db}.victim TO ${low};
GRANT INSERT ON ${db}.single_hop TO ${low}, ${ok};
EOF
run_low --async-insert 0 --query "INSERT INTO ${db}.single_hop VALUES ('from_low')"
${CH} --query "REVOKE INSERT ON ${victim_db}.victim FROM ${low}"
info_client="${CLICKHOUSE_CLIENT/--send_logs_level=warning/--send_logs_level=information}"
leak=$(${info_client} --user "${ok}" --async-insert 0 --query "INSERT INTO ${db}.single_hop VALUES ('from_ok')" 2>&1)
echo -n "discard diagnostic hides the buffering user: "
if echo "$leak" | grep -q "can no longer be flushed under the identity" \
    && ! echo "$leak" | grep -qE "${low}|Not enough privileges|INSERT ON ${victim_db}"; then echo "OK"; else echo "UNEXPECTED"; fi

echo "## Part G: a non-authorization flush failure is retained, and its error is not leaked cross-identity"
# low buffers a value that violates the destination's CHECK constraint: a non-authorization failure, so (unlike
# a dropped/expired identity) it is retained for retry, not discarded. ok's synchronous insert flushes low's
# batch; ok must get a neutral error, not the constraint detail, and low's batch must remain buffered.
${CH} --multiquery <<EOF
CREATE TABLE ${victim_db}.constrained (x Int32, CONSTRAINT positive CHECK x > 0) ENGINE = MergeTree ORDER BY x;
CREATE TABLE ${db}.constrained_buf (x Int32) ENGINE = Buffer(${victim_db}, constrained, 1, 3600, 7200, 1000000, 2000000, 100000000, 200000000);
GRANT INSERT, SELECT ON ${db}.constrained_buf TO ${low}, ${ok};
GRANT INSERT ON ${victim_db}.constrained TO ${low}, ${ok};
EOF
run_low --async-insert 0 --query "INSERT INTO ${db}.constrained_buf VALUES (-5)"
retained_err=$(run_ok --async-insert 0 --query "INSERT INTO ${db}.constrained_buf VALUES (7)" 2>&1)
echo -n "retained-failure error is neutral: "
# The neutral message replaces the original, so the leak-prone detail (here the constraint name) is gone; the
# generic message is present only when the cross-identity rethrow fired. (Only the error code is kept.)
if echo "$retained_err" | grep -q "Could not flush a buffered batch" \
    && ! echo "$retained_err" | grep -q "positive"; then echo "OK"; else echo "UNEXPECTED"; fi
echo -n "batch retained, not discarded:    "; ${CH} --query "SELECT count() FROM ${db}.constrained_buf"

echo "## Part H: a failure to rebuild the flush context that leaves the identity valid is retained"
# Rebuilding the buffering user's context checks its default database. With that database detached the flush
# fails, but the user and its grants are unchanged: the batch must stay buffered, not be discarded as for a
# dropped user, and be delivered once the database is attached again. home_alias_buf forwards through an `Alias`
# whose target is in that database, so classifying the failure must not lose the batch either.
home="user_05621_home_${CLICKHOUSE_DATABASE}"
home_db="home_05621_${CLICKHOUSE_DATABASE}"
${CH} --multiquery <<EOF
DROP USER IF EXISTS ${home};
DROP DATABASE IF EXISTS ${home_db};
CREATE DATABASE ${home_db} ENGINE = Atomic;
CREATE TABLE ${victim_db}.home_dst (s String) ENGINE = MergeTree ORDER BY s;
CREATE TABLE ${home_db}.alias_dst (s String) ENGINE = MergeTree ORDER BY s;
CREATE TABLE ${db}.home_alias ENGINE = Alias('${home_db}', 'alias_dst');
CREATE TABLE ${db}.home_buf (s String) ENGINE = Buffer(${victim_db}, home_dst, 1, 3600, 7200, 1000000, 2000000, 100000000, 200000000);
CREATE TABLE ${db}.home_alias_buf (s String) ENGINE = Buffer(${db}, home_alias, 1, 3600, 7200, 1000000, 2000000, 100000000, 200000000);
CREATE USER ${home} DEFAULT DATABASE ${home_db};
GRANT INSERT ON ${db}.home_buf TO ${home};
GRANT INSERT ON ${db}.home_alias_buf TO ${home};
GRANT INSERT ON ${db}.home_alias TO ${home};
GRANT INSERT ON ${victim_db}.home_dst TO ${home};
GRANT INSERT ON ${home_db}.alias_dst TO ${home};
EOF
for buf in home_buf home_alias_buf; do
    ${CLICKHOUSE_CLIENT} --user "${home}" --async-insert 0 --query "INSERT INTO ${db}.${buf} VALUES ('from_home')"
done
${CH} --query "DETACH DATABASE ${home_db}"
for buf in home_buf home_alias_buf; do
    echo -n "${buf}: flush without the default database is retained: "
    if ${CH} --query "OPTIMIZE TABLE ${db}.${buf}" 2>&1 | grep -q "retained for retry"; then echo "OK"; else echo "UNEXPECTED"; fi
done
${CH} --query "ATTACH DATABASE ${home_db}"
${CH} --query "OPTIMIZE TABLE ${db}.home_buf"
${CH} --query "OPTIMIZE TABLE ${db}.home_alias_buf"
echo -n "home_buf: delivered once it is attached again: "; ${CH} --query "SELECT groupArray(s) FROM ${victim_db}.home_dst"
echo -n "home_alias_buf: delivered once it is attached again: "; ${CH} --query "SELECT groupArray(s) FROM ${home_db}.alias_dst"

${CH} --query "DROP USER IF EXISTS ${low}; DROP USER IF EXISTS ${ok}; DROP USER IF EXISTS ${tmp}; DROP USER IF EXISTS ${home}" 2>/dev/null
${CH} --query "DROP DATABASE IF EXISTS ${home_db}"
${CH} --query "DROP DATABASE IF EXISTS ${victim_db}"
