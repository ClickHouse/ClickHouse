#!/usr/bin/env bash

# `query_kind`, `initial_user`, `initial_address` and `interface` are plain `ClientInfo` fields that any
# native client can put on the wire (`clickhouse-client --query_kind secondary_query` is a documented
# option). On a connection that is not an authenticated interserver session the server keeps the forwarded
# initiator only if the authenticated user has the `IMPERSONATE` privilege for it. Otherwise `currentUser`
# (and through it a row policy) must see the authenticated user, or any user could read another user's
# policy-filtered rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

attacker="attacker_${CLICKHOUSE_DATABASE}"
forwarder="forwarder_${CLICKHOUSE_DATABASE}"
victim="victim_${CLICKHOUSE_DATABASE}"
policy="pol_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --multiquery "
DROP ROW POLICY IF EXISTS $policy ON ${CLICKHOUSE_DATABASE}.docs;
DROP USER IF EXISTS $attacker, $forwarder, $victim;
CREATE USER $attacker IDENTIFIED WITH plaintext_password BY 'pw';
CREATE USER $forwarder IDENTIFIED WITH plaintext_password BY 'pw';
CREATE USER $victim;
GRANT IMPERSONATE ON $victim, IMPERSONATE ON ${CLICKHOUSE_DATABASE} TO $forwarder;
CREATE TABLE ${CLICKHOUSE_DATABASE}.docs (owner String, secret String) ENGINE = MergeTree ORDER BY owner;
INSERT INTO ${CLICKHOUSE_DATABASE}.docs SELECT * FROM values(('$attacker', 'attacker-row'), ('$forwarder', 'forwarder-row'), ('$victim', 'VICTIM-ROW'));
CREATE ROW POLICY $policy ON ${CLICKHOUSE_DATABASE}.docs USING owner = currentUser() TO $attacker, $forwarder, $victim;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.docs TO $attacker, $forwarder, $victim;
CREATE TABLE ${CLICKHOUSE_DATABASE}.obs (scenario String, check String, value String) ENGINE = MergeTree ORDER BY (scenario, check);
GRANT INSERT ON ${CLICKHOUSE_DATABASE}.obs TO $attacker, $forwarder;
GRANT SELECT ON system.processes TO $attacker, $forwarder;
"

# Every scenario records what the server made of the forged fields in ${CLICKHOUSE_DATABASE}.obs.
python3 "$CUR_DIR"/05616_forged_secondary_query_identity.python "$attacker" "$forwarder" "pw" "$victim" "${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
SELECT scenario, check, replaceAll(replaceAll(replaceAll(value, '$attacker', 'ATTACKER'), '$forwarder', 'FORWARDER'), '$victim', 'VICTIM')
FROM ${CLICKHOUSE_DATABASE}.obs
ORDER BY scenario, check
"

${CLICKHOUSE_CLIENT} --multiquery "
DROP ROW POLICY IF EXISTS $policy ON ${CLICKHOUSE_DATABASE}.docs;
DROP USER IF EXISTS $attacker, $forwarder, $victim;
"
