#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: uses a raw native-protocol client.

# `query_kind`, `initial_user` and `initial_address` are plain `ClientInfo` fields that any native
# client can put on the wire (`clickhouse-client --query_kind secondary_query` is a documented
# option). On a connection that is not an authenticated interserver session, the server must ignore
# them for identity: `currentUser()` (and through it a row policy) and the `max_query_size` lift must
# see the authenticated user, never a forged one. Otherwise any authenticated user could read another
# user's policy-filtered rows and bypass that user's constraints.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

attacker="attacker_${CLICKHOUSE_DATABASE}"
victim="victim_${CLICKHOUSE_DATABASE}"
policy="pol_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --multiquery "
DROP ROW POLICY IF EXISTS $policy ON ${CLICKHOUSE_DATABASE}.docs;
DROP USER IF EXISTS $attacker, $victim;
CREATE USER $attacker IDENTIFIED WITH plaintext_password BY 'attackerpw' SETTINGS max_query_size = 2000 READONLY;
CREATE USER $victim;
CREATE TABLE ${CLICKHOUSE_DATABASE}.docs (owner String, secret String) ENGINE = MergeTree ORDER BY owner;
INSERT INTO ${CLICKHOUSE_DATABASE}.docs VALUES ('$attacker', 'attacker-row'), ('$victim', 'VICTIM-ROW');
CREATE ROW POLICY $policy ON ${CLICKHOUSE_DATABASE}.docs USING owner = currentUser() TO $attacker, $victim;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.docs TO $attacker, $victim;
CREATE TABLE ${CLICKHOUSE_DATABASE}.obs (scenario String, check String, value String) ENGINE = MergeTree ORDER BY (scenario, check);
GRANT INSERT ON ${CLICKHOUSE_DATABASE}.obs TO $attacker;
"

# The attacker forges each scenario; every observation is recorded in ${CLICKHOUSE_DATABASE}.obs.
python3 "$CUR_DIR"/05616_forged_secondary_query_identity.python "$attacker" "attackerpw" "$victim" "${CLICKHOUSE_DATABASE}"

# currentUser() must always be the attacker, the row policy must only ever return the attacker's row,
# and the oversized query must always be rejected - identical to the attacker's honest initial query.
${CLICKHOUSE_CLIENT} --query "
SELECT scenario, check, replaceAll(value, '$attacker', 'ATTACKER') AS value
FROM ${CLICKHOUSE_DATABASE}.obs
ORDER BY scenario, check
"

${CLICKHOUSE_CLIENT} --multiquery "
DROP ROW POLICY IF EXISTS $policy ON ${CLICKHOUSE_DATABASE}.docs;
DROP USER IF EXISTS $attacker, $victim;
"
