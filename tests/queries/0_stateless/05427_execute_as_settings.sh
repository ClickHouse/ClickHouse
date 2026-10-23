#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# `EXECUTE AS user SETTINGS name = value` fills a custom setting that the user declares CONST without a value.

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S=${CLICKHOUSE_DATABASE}
GW="gw_${S}"; QU="qu_${S}"; QV="qv_${S}"; QW="qw_${S}"; QRO="qro_${S}"; QRO2="qro2_${S}"
QROLE="qrole_${S}"; QVAL="qval_${S}"; QS="qs_${S}"; R_LOCK="r_lock_${S}"
QEMPTY="qempty_${S}"; QTWO="qtwo_${S}"; P_EMPTY="p_empty_${S}"; QMT="qmt_${S}"; P_MT="p_mt_${S}"
P_LOCK="p_lock_${S}"; P_OTHER="p_other_${S}"; P_WRITABLE="p_writable_${S}"
P_OTHER_WRITABLE="p_other_writable_${S}"; P_VALUELESS="p_valueless_${S}"; P_DEFAULT="p_default_${S}"
READERS="${QU}, ${QV}, ${QW}, ${QRO}, ${QRO2}, ${QROLE}, ${QVAL}, ${QS}"

function cleanup()
{
    $CLICKHOUSE_CLIENT -m -q "
        DROP ROW POLICY IF EXISTS rp ON ${S}.t;
        DROP USER IF EXISTS ${GW}, ${READERS}, ${QEMPTY}, ${QTWO}, ${QMT};
        DROP ROLE IF EXISTS ${R_LOCK};
        DROP SETTINGS PROFILE IF EXISTS ${P_LOCK}, ${P_OTHER}, ${P_WRITABLE}, ${P_OTHER_WRITABLE}, ${P_VALUELESS}, ${P_DEFAULT}, ${P_EMPTY}, ${P_MT};
    "
}
cleanup

$CLICKHOUSE_CLIENT -m -q "
CREATE TABLE t (tenant String, v UInt8) ENGINE = MergeTree ORDER BY v;
INSERT INTO t VALUES ('acme', 1), ('acme', 2), ('other', 3);
CREATE TABLE d_sec AS t ENGINE = Distributed(test_cluster_interserver_secret, ${S}, t);
CREATE TABLE d_sec2 AS t ENGINE = Distributed(test_cluster_interserver_secret, ${S}, d_sec);
CREATE TABLE d_nosec AS t ENGINE = Distributed(test_cluster_two_shards, ${S}, t);

CREATE SETTINGS PROFILE ${P_LOCK} SETTINGS SQL_tenant CONST;
CREATE SETTINGS PROFILE ${P_OTHER} SETTINGS SQL_tenant = 'other';
CREATE SETTINGS PROFILE ${P_WRITABLE} SETTINGS SQL_tenant WRITABLE;
CREATE SETTINGS PROFILE ${P_OTHER_WRITABLE} SETTINGS SQL_tenant = 'other' WRITABLE;
CREATE SETTINGS PROFILE ${P_VALUELESS} SETTINGS SQL_tenant CONST;
CREATE ROLE ${R_LOCK} SETTINGS SQL_tenant CONST;

CREATE USER ${GW} IDENTIFIED WITH no_password SETTINGS SQL_gwmark = 'g';
CREATE USER ${QU} SETTINGS PROFILE ${P_LOCK};
CREATE USER ${QV} SETTINGS PROFILE ${P_LOCK};
CREATE USER ${QW};
CREATE USER ${QRO} SETTINGS PROFILE ${P_LOCK}, readonly = 1;
CREATE USER ${QRO2} SETTINGS readonly = 1;
CREATE USER ${QROLE};
GRANT ${R_LOCK} TO ${QROLE};
CREATE USER ${QVAL} SETTINGS SQL_tenant = 'globex' CONST;
CREATE SETTINGS PROFILE ${P_EMPTY} SETTINGS SQL_tenant = '';
CREATE USER ${QEMPTY} SETTINGS SQL_tenant = '' CONST;
CREATE USER ${QTWO} SETTINGS PROFILE ${P_LOCK}, PROFILE ${P_EMPTY};
CREATE SETTINGS PROFILE ${P_MT} SETTINGS merge_tree_index_granularity CONST;
CREATE USER ${QMT} SETTINGS PROFILE ${P_MT};
CREATE USER ${QS} IDENTIFIED WITH no_password SETTINGS SQL_tenant = 'acme' CONST;

GRANT IMPERSONATE ON ${QU} TO ${GW};
GRANT IMPERSONATE ON ${QV} TO ${GW}, ${QU}, ${QS};
GRANT IMPERSONATE ON ${QW} TO ${GW}, ${QU};
GRANT IMPERSONATE ON ${QRO} TO ${GW};
GRANT IMPERSONATE ON ${QRO2} TO ${GW};
GRANT IMPERSONATE ON ${QROLE} TO ${GW};
GRANT IMPERSONATE ON ${QVAL} TO ${GW};
GRANT IMPERSONATE ON ${QEMPTY} TO ${GW};
GRANT IMPERSONATE ON ${QTWO} TO ${GW};
GRANT IMPERSONATE ON ${QMT} TO ${GW};
GRANT IMPERSONATE ON default TO ${GW};
GRANT SELECT ON ${S}.* TO ${READERS};
CREATE ROW POLICY rp ON t USING tenant = getSetting('SQL_tenant') TO ${READERS};
"

# `default` is defined in users.xml. Give it a CONST without a value for a setting only this test uses.
$CLICKHOUSE_CLIENT -q "CREATE SETTINGS PROFILE ${P_DEFAULT} SETTINGS SQL_${S} CONST TO default"

# Prints the output of every statement, and the error name of a statement that fails.
function run()
{
    local user=$1
    shift
    $CLICKHOUSE_CLIENT --user "$user" -m --ignore-error --prefer_localhost_replica 0 --serialize_query_plan 0 "$@" 2>&1 \
        | grep -v -e '^Received exception' -e '^(query: ' | sed -E 's/^Code: [0-9]+\. .*\(([A-Z_]+)\)$/\1/'
}

READ="SELECT arraySort(groupArray(v)) FROM t"

echo "-- query form"
run "$GW" -q "EXECUTE AS ${QU} SETTINGS SQL_tenant = 'acme' ${READ}"
run "$GW" -q "EXECUTE AS ${QU} ${READ}"
run "$GW" -q "EXECUTE AS ${QU} SETTINGS SQL_tenant = 'acme', SQL_tenant = 'acme' SELECT 1"
run "$GW" -q "EXECUTE AS ${QU} SETTINGS max_threads = 1 SELECT 1"
run "$GW" -q "EXECUTE AS ${QMT} SETTINGS merge_tree_index_granularity = 1 SELECT 1"
run "$GW" -q "SELECT formatQuery('EXECUTE AS u SETTINGS SQL_tenant = DEFAULT SELECT 1')"
run "$GW" -q "SELECT formatQuery('EXECUTE AS u SETTINGS SQL_tenant = {p:String} SELECT 1')"
run "$GW" -q "SELECT formatQuery('EXECUTE AS u SETTINGS SQL_tenant SELECT 1')"
run "$GW" -q "SELECT formatQuery('EXECUTE AS u SETTINGS SQL_tenant = {\'k\':\'v\'} SELECT 1')"
run "$GW" -q "EXECUTE AS ${QW} SETTINGS SQL_tenant = 'acme' SELECT 1"
run "$GW" -q "EXECUTE AS ${QVAL} SETTINGS SQL_tenant = 'acme' SELECT 1"
run "$GW" -q "EXECUTE AS ${QEMPTY} SETTINGS SQL_tenant = 'acme' SELECT 1"
run "$GW" -q "EXECUTE AS ${QEMPTY} SELECT getSetting('SQL_tenant') = ''"
run "$GW" -q "EXECUTE AS ${QTWO} SETTINGS SQL_tenant = 'acme' SELECT 1"
run "$GW" -q "EXECUTE AS default SETTINGS SQL_${S} = 'acme' SELECT 1"
run "$GW" -q "EXECUTE AS ${QRO2} SETTINGS SQL_tenant = 'acme' SELECT 1"
run "$GW" -q "EXECUTE AS ${QRO2} SELECT isNull(getSetting('SQL_x')) SETTINGS SQL_x = NULL"
run "$GW" -q "EXECUTE AS ${QROLE} SETTINGS SQL_tenant = 'acme' ${READ}"
run "$GW" -q "EXECUTE AS ${QRO} SETTINGS SQL_tenant = 'acme' ${READ}"
run "$GW" -q "SELECT formatQuery('EXECUTE AS u SETTINGS SQL_a = \'x\', SQL_b = 1 SELECT 1')"
run "$GW" -q "SELECT formatQuery('EXECUTE AS u SETTINGS SQL_a = \'x\'')"

echo "-- query form, attempts to change the value"
run "$GW" -q "EXECUTE AS ${QU} SETTINGS SQL_tenant = 'acme' ${READ} SETTINGS SQL_tenant = 'other'"
run "$GW" -q "EXECUTE AS ${QU} SETTINGS SQL_tenant = 'acme' ${READ} SETTINGS SQL_tenant = NULL"
run "$GW" -q "EXECUTE AS ${QU} SETTINGS SQL_tenant = 'acme' ${READ} FORMAT TSV SETTINGS SQL_tenant = 'other'"
run "$GW" -q "EXECUTE AS ${QU} SETTINGS SQL_tenant = 'acme' SELECT arraySort(groupArray(v)) FROM (SELECT * FROM t SETTINGS SQL_tenant = 'other')"
for profile in "$P_OTHER" "$P_WRITABLE" "$P_OTHER_WRITABLE"; do
    run "$GW" -q "EXECUTE AS ${QU} SETTINGS SQL_tenant = 'acme' ${READ} SETTINGS profile = '${profile}'"
done
run "$GW" -q "SET SQL_tenant = 'other'; EXECUTE AS ${QU} SETTINGS SQL_tenant = 'acme' ${READ}"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${GW}&SQL_tenant=other" --data-binary "EXECUTE AS ${QU} SETTINGS SQL_tenant = 'acme' ${READ}"

echo "-- a value set before EXECUTE AS is kept only where the target user may change it"
run "$GW" -q "SET SQL_tenant = 'acme'; EXECUTE AS ${QU}; SELECT getSetting('SQL_tenant')"
run "$GW" -q "EXECUTE AS ${QW} SETTINGS SQL_tenant = 'x'; SELECT currentUser() = '${GW}', getSetting('SQL_gwmark')"
run "$GW" -q "EXECUTE AS ${QW}; SELECT currentUser() = '${QW}', getSetting('SQL_gwmark')"

echo "-- session form"
run "$GW" -q "
EXECUTE AS ${QU} SETTINGS SQL_tenant = 'acme';
SELECT currentUser() = '${QU}', getSetting('SQL_tenant');
SET SQL_tenant = 'other';
SET SQL_tenant = DEFAULT;
SET SQL_tenant = NULL;
SET profile = '${P_OTHER}';
SET profile = '${P_WRITABLE}';
SET profile = '${P_OTHER_WRITABLE}';
SET profile = '${P_VALUELESS}';
SELECT getSetting('SQL_tenant');
${READ};
"
run "$GW" -q "
EXECUTE AS ${QROLE} SETTINGS SQL_tenant = 'acme';
SET ROLE NONE;
SELECT getSetting('SQL_tenant');
SET SQL_tenant = 'other';
"

echo "-- nested EXECUTE AS"
run "$GW" -q "EXECUTE AS ${QU} SETTINGS SQL_tenant = 'acme'; EXECUTE AS ${QV}; SELECT currentUser() = '${QV}', getSetting('SQL_tenant'); ${READ}"
run "$GW" -q "EXECUTE AS ${QU} SETTINGS SQL_tenant = 'acme'; EXECUTE AS ${QV} SETTINGS SQL_tenant = 'other'; SELECT currentUser() = '${QU}'"
run "$GW" -q "EXECUTE AS ${QU} SETTINGS SQL_tenant = 'acme'; EXECUTE AS ${QV} SETTINGS SQL_tenant = 'acme'; SELECT currentUser() = '${QV}'"
run "$GW" -q "EXECUTE AS ${QU} SETTINGS SQL_tenant = 'acme'; EXECUTE AS ${QW}; EXECUTE AS ${QV}; SELECT currentUser() = '${QV}', getSetting('SQL_tenant')"
run "$GW" -q "EXECUTE AS ${QU} SETTINGS SQL_tenant = 'acme'; EXECUTE AS ${QV} SELECT currentUser() = '${QV}', getSetting('SQL_tenant'); EXECUTE AS ${QW} SELECT 1"
run "$QS" -q "EXECUTE AS ${QV} SETTINGS SQL_tenant = 'other' SELECT 1"
run "$QS" -q "EXECUTE AS ${QV} SETTINGS SQL_tenant = NULL SELECT 1"
run "$QS" -q "EXECUTE AS ${QV} SETTINGS SQL_tenant = 'acme' ${READ}"

echo "-- distributed"
run "$GW" -q "EXECUTE AS ${QU} SETTINGS SQL_tenant = 'acme' SELECT arraySort(groupArray(v)) FROM d_sec"
run "$GW" -q "EXECUTE AS ${QU} SETTINGS SQL_tenant = 'acme'; SELECT arraySort(groupArray(v)) FROM d_sec"
run "$GW" -q "EXECUTE AS ${QU} SETTINGS SQL_tenant = 'acme' SELECT arraySort(groupArray(v)) FROM (SELECT * FROM d_sec SETTINGS SQL_tenant = 'other')"
run "$GW" -q "EXECUTE AS ${QU} SETTINGS SQL_tenant = 'acme' SELECT count(), countIf(v = 3) FROM d_sec2"
run "$GW" -q "EXECUTE AS ${QU} SETTINGS SQL_tenant = 'acme' SELECT arraySort(groupArray(v)) FROM d_nosec"

echo "-- NULL where the setting may be changed"
run "$GW" -q "SELECT * FROM (SELECT isNull(getSetting('SQL_gwmark')) SETTINGS SQL_gwmark = NULL)"

cleanup
