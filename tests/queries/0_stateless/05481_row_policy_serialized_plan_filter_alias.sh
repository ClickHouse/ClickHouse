#!/usr/bin/env bash
# Tags: distributed
# An `additional_table_filters` entry keyed by a table alias gives the same result with the query text and with a
# serialized plan. The executing node plans a shipped `Merge` read again, without the alias.
# An entry of the executing node keyed by the alias of the initial query does not match in either mode, because the
# executing node gets the table with the alias `__table1`. An entry keyed by the table name matches in both modes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -m -q "
DROP TABLE IF EXISTS rp_fa_dist;
DROP TABLE IF EXISTS rp_fa_merge;
DROP TABLE IF EXISTS rp_fa_leaf;
DROP TABLE IF EXISTS rp_fa_lookup;
CREATE TABLE rp_fa_leaf (x UInt32, y UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO rp_fa_leaf SELECT number, number FROM numbers(100);
CREATE TABLE rp_fa_merge AS rp_fa_leaf ENGINE = Merge(currentDatabase(), '^rp_fa_leaf\$');
CREATE TABLE rp_fa_dist AS rp_fa_leaf ENGINE = Distributed(test_shard_localhost, currentDatabase(), rp_fa_merge);
CREATE TABLE rp_fa_lookup (y UInt32) ENGINE = MergeTree ORDER BY y;
INSERT INTO rp_fa_lookup VALUES (1), (2);
"

# The subquery names the database, because the current database of the executing node is different.
for filter in "y < 5" "y IN (SELECT y FROM ${CLICKHOUSE_DATABASE}.rp_fa_lookup)"; do
    for sqp in 0 1; do
        echo -n "${filter/${CLICKHOUSE_DATABASE}./} sqp=${sqp} "
        ${CLICKHOUSE_CLIENT} -q "SELECT sum(x) FROM rp_fa_dist AS m_alias SETTINGS prefer_localhost_replica = 0,
            serialize_query_plan = ${sqp}, additional_table_filters = {'m_alias': '${filter}'}"
    done
done

USER_ALIAS="u_alias_${CLICKHOUSE_DATABASE}"
USER_NAME="u_name_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -m -q "
DROP USER IF EXISTS ${USER_ALIAS}, ${USER_NAME};
DROP SETTINGS PROFILE IF EXISTS p_${USER_ALIAS}, p_${USER_NAME};
CREATE SETTINGS PROFILE p_${USER_ALIAS} SETTINGS additional_table_filters = '{''m_alias'': ''y < 5''}' READONLY;
CREATE SETTINGS PROFILE p_${USER_NAME} SETTINGS additional_table_filters
    = '{''${CLICKHOUSE_DATABASE}.rp_fa_merge'': ''y < 5'', ''${CLICKHOUSE_DATABASE}.rp_fa_leaf'': ''y < 5''}' READONLY;
CREATE USER ${USER_ALIAS} IDENTIFIED WITH no_password SETTINGS PROFILE p_${USER_ALIAS};
CREATE USER ${USER_NAME} IDENTIFIED WITH no_password SETTINGS PROFILE p_${USER_NAME};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${USER_ALIAS}, ${USER_NAME};
CREATE ROW POLICY rp_fa_pol ON rp_fa_leaf USING x < 1000 TO ${USER_ALIAS}, ${USER_NAME};
"

# Only the executing node has the entry. The policy makes the executing node plan the `MergeTree` read again too.
for user in "${USER_ALIAS}" "${USER_NAME}"; do
    for table in rp_fa_merge rp_fa_leaf; do
        for sqp in 0 1; do
            echo -n "${user%%_${CLICKHOUSE_DATABASE}} ${table} sqp=${sqp} "
            ${CLICKHOUSE_CLIENT} -q "SELECT sum(x) FROM remote('127.0.0.2', ${CLICKHOUSE_DATABASE}.${table}, '${user}', '') AS m_alias
                SETTINGS prefer_localhost_replica = 0, serialize_query_plan = ${sqp}"
        done
    done
done

${CLICKHOUSE_CLIENT} -m -q "
DROP ROW POLICY rp_fa_pol ON rp_fa_leaf;
DROP USER ${USER_ALIAS}, ${USER_NAME};
DROP SETTINGS PROFILE p_${USER_ALIAS}, p_${USER_NAME};
DROP TABLE rp_fa_dist;
DROP TABLE rp_fa_merge;
DROP TABLE rp_fa_leaf;
DROP TABLE rp_fa_lookup;
"
