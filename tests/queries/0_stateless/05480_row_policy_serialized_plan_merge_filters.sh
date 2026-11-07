#!/usr/bin/env bash
# Tags: distributed, no-fasttest
# The executing node plans a shipped read of a `Merge` table again, because `Merge` reads through an
# analyzed query. The initiator already applied the table's own filters as steps above that read, so
# planning it again must not apply them a second time, nor check the read list widened by them
# against `max_columns_to_read` or the user's grants.
# The grants arm needs a cluster with a <secret>, which requires SSL: hence no-fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_X="rp_mf_x_${CLICKHOUSE_DATABASE}"
SETTINGS="prefer_localhost_replica = 0, enable_analyzer = 1"

${CLICKHOUSE_CLIENT} -m -q "
DROP USER IF EXISTS ${USER_X};
DROP TABLE IF EXISTS rp_mf_leaf;
DROP TABLE IF EXISTS rp_mf_merge;
DROP TABLE IF EXISTS rp_mf_dist;
DROP TABLE IF EXISTS rp_mf_dist_secret;
DROP TABLE IF EXISTS rp_mf_dist_leaf;
CREATE TABLE rp_mf_leaf (x UInt32, y UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO rp_mf_leaf SELECT number, number FROM numbers(100000);
CREATE TABLE rp_mf_merge AS rp_mf_leaf ENGINE = Merge(currentDatabase(), '^rp_mf_leaf\$');
CREATE TABLE rp_mf_dist AS rp_mf_leaf ENGINE = Distributed(test_shard_localhost, currentDatabase(), rp_mf_merge);
"

# A nondeterministic filter keeps about half the rows when applied once and a quarter when applied twice.
classify="if(abs(count() - 50000) < 2000, 'ONCE', if(abs(count() - 25000) < 2000, 'TWICE', 'OTHER'))"

# The key names the database, so that it matches on the executing node too.
for sqp in 1 0; do
    echo -n "atf on merge sqp=${sqp} "
    ${CLICKHOUSE_CLIENT} -q "SELECT ${classify} FROM rp_mf_dist SETTINGS ${SETTINGS}, serialize_query_plan = ${sqp},
        additional_table_filters = {'${CLICKHOUSE_DATABASE}.rp_mf_merge': 'rand64() % 2 = 0'}"
done

# `randConstant()` is folded when the filter is analyzed, so each analysis keeps another single row.
for sqp in 1 0; do
    echo -n "constant atf on merge sqp=${sqp} "
    ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM rp_mf_dist SETTINGS ${SETTINGS}, serialize_query_plan = ${sqp},
        additional_table_filters = {'${CLICKHOUSE_DATABASE}.rp_mf_merge': 'x = randConstant() % 100000'}"
done

# `randConstant() % 2` folds to the same value in two analyses half of the time, so the query runs 20 times.
for sqp in 1 0; do
    echo -n "two-valued constant atf on merge sqp=${sqp} "
    ${CLICKHOUSE_CLIENT} -q "$(for _ in $(seq 20); do
        echo "SELECT count() FROM rp_mf_dist SETTINGS ${SETTINGS}, serialize_query_plan = ${sqp},
            additional_table_filters = {'${CLICKHOUSE_DATABASE}.rp_mf_merge': 'x = randConstant() % 2'};"
    done)" | sort -u | paste -sd' '
done

# The executing node plans a read of a table with a row policy again, too.
${CLICKHOUSE_CLIENT} -m -q "
CREATE TABLE rp_mf_dist_leaf AS rp_mf_leaf ENGINE = Distributed(test_shard_localhost, currentDatabase(), rp_mf_leaf);
CREATE ROW POLICY rp_mf_all ON rp_mf_leaf FOR SELECT USING y < 100000 TO ALL;
"
for sqp in 1 0; do
    echo -n "constant atf on policy table sqp=${sqp} "
    ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM rp_mf_dist_leaf SETTINGS ${SETTINGS}, serialize_query_plan = ${sqp},
        additional_table_filters = {'${CLICKHOUSE_DATABASE}.rp_mf_leaf': 'y = randConstant() % 100000'}"
done

# An `arrayJoin` in the entry doubles each row once.
for table in rp_mf_merge rp_mf_leaf; do
    dist=$([ "$table" = rp_mf_merge ] && echo rp_mf_dist || echo rp_mf_dist_leaf)
    for sqp in 1 0; do
        echo -n "arrayJoin atf on ${table} sqp=${sqp} "
        ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${dist} SETTINGS ${SETTINGS}, serialize_query_plan = ${sqp},
            additional_table_filters = {'${CLICKHOUSE_DATABASE}.${table}': 'x < 100 AND arrayJoin([1, 1])'}"
    done
done

# The entry runs after the PREWHERE of the query, which does not commute with it here.
for table in rp_mf_merge rp_mf_leaf; do
    dist=$([ "$table" = rp_mf_merge ] && echo rp_mf_dist || echo rp_mf_dist_leaf)
    for sqp in 1 0; do
        echo -n "arrayJoin atf after PREWHERE on ${table} sqp=${sqp} "
        ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${dist} PREWHERE rowNumberInAllBlocks() < 5
            SETTINGS ${SETTINGS}, serialize_query_plan = ${sqp}, optimize_move_to_prewhere = 0,
            additional_table_filters = {'${CLICKHOUSE_DATABASE}.${table}': 'arrayJoin([1, 1])'}"
        echo -n "throwIf atf after PREWHERE on ${table} sqp=${sqp} "
        ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${dist} PREWHERE x < 5
            SETTINGS ${SETTINGS}, serialize_query_plan = ${sqp}, query_plan_enable_optimizations = 0,
            additional_table_filters = {'${CLICKHOUSE_DATABASE}.${table}': 'throwIf(x >= 5) = 0'}" 2>&1 \
            | grep -oE '^[0-9]+$|FUNCTION_THROW_IF_VALUE_IS_NON_ZERO' | head -1
    done
done

# The set of an `IN` subquery in the entry is built once. Prints the count and whether each secondary
# query built one set.
for sqp in 1 0; do
    query_id="${CLICKHOUSE_DATABASE}_in_set_${sqp}_${RANDOM}"
    echo -n "IN set atf on rp_mf_leaf sqp=${sqp} "
    ${CLICKHOUSE_CLIENT} --query_id "${query_id}" -q "SELECT count() FROM rp_mf_dist_leaf
        SETTINGS ${SETTINGS}, serialize_query_plan = ${sqp}, use_index_for_in_with_subqueries = 0,
        additional_table_filters = {'${CLICKHOUSE_DATABASE}.rp_mf_leaf': 'y IN (SELECT number FROM numbers(10))'}" | tr '\n' ' '
    # Secondary queries publish their log rows after the initiator has already returned.
    for _ in $(seq 1 60); do
        ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
        sets=$(${CLICKHOUSE_CLIENT} -q "
            SELECT countIf(ProfileEvents['SetsBuiltFromSubquery'] = 1) = count() FROM system.query_log
            WHERE type = 'QueryFinish' AND NOT is_initial_query AND event_date >= yesterday()
              AND initial_query_id = '${query_id}'
              -- A secondary query logs current_database = 'default', not the test database.
              AND current_database IN ['default', currentDatabase()]
            HAVING count() > 0")
        [ -n "${sets}" ] && break
        sleep 0.5
    done
    echo "${sets}"
done
${CLICKHOUSE_CLIENT} -m -q "
DROP ROW POLICY rp_mf_all ON rp_mf_leaf;
DROP TABLE rp_mf_dist_leaf;
"

# The executing node, reached through `remote` as a user with its own READONLY entry, gets that entry, which reads `y`:
# a column the step of the initiator's entry does not get.
USER_RECEIVER="rp_mf_receiver_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -m -q "
CREATE ROW POLICY rp_mf_x ON rp_mf_leaf FOR SELECT USING x < 100000 TO ALL;
DROP USER IF EXISTS ${USER_RECEIVER};
CREATE USER ${USER_RECEIVER} IDENTIFIED WITH plaintext_password BY '';
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${USER_RECEIVER};
"
receiver_entry()
{
    local table=$1 entry=$2 prewhere=$3 sender_entry=$4 label=${5:-} sender_setting=""
    [ -n "${sender_entry}" ] && sender_setting=", additional_table_filters = {'${CLICKHOUSE_DATABASE}.${table}': '${sender_entry}'}"
    ${CLICKHOUSE_CLIENT} -m -q "
    DROP SETTINGS PROFILE IF EXISTS p_${USER_RECEIVER};
    CREATE SETTINGS PROFILE p_${USER_RECEIVER} SETTINGS
        additional_table_filters = '{\'${CLICKHOUSE_DATABASE}.${table}\': \'${entry}\'}' READONLY TO ${USER_RECEIVER};
    "
    for sqp in 1 0; do
        echo -n "receiver entry ${entry} on ${table}${label} sqp=${sqp} "
        ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', ${CLICKHOUSE_DATABASE}, ${table}, '${USER_RECEIVER}', '')
            PREWHERE ${prewhere} SETTINGS ${SETTINGS}, serialize_query_plan = ${sqp}, optimize_move_to_prewhere = 0,
            query_plan_enable_optimizations = 0${sender_setting}" 2>&1 \
            | grep -oE '^[0-9]+$|FUNCTION_THROW_IF_VALUE_IS_NON_ZERO' | head -1
    done
}
for table in rp_mf_merge rp_mf_leaf; do
    receiver_entry "${table}" "y < 100000 AND arrayJoin([1, 1])" "rowNumberInAllBlocks() < 5" "arrayJoin([1, 1])"
    receiver_entry "${table}" "throwIf(y >= 5) = 0" "x < 5" "x < 10"
    receiver_entry "${table}" "y < 100000 AND arrayJoin([1, 1])" "rowNumberInAllBlocks() < 5" "" " alone"
    receiver_entry "${table}" "throwIf(y >= 5) = 0" "x < 5" "" " alone"
done
# Also over a policy of the `Merge` table, which the plan applies as a step under the entry.
${CLICKHOUSE_CLIENT} -q "CREATE ROW POLICY rp_mf_mx ON rp_mf_merge FOR SELECT USING x < 100000 TO ALL"
receiver_entry rp_mf_merge "y < 100000 AND arrayJoin([1, 1])" "rowNumberInAllBlocks() < 5" "arrayJoin([1, 1])" " with a policy"
receiver_entry rp_mf_merge "throwIf(y >= 5) = 0" "x < 5" "x < 10" " with a policy"
receiver_entry rp_mf_merge "y < 100000 AND arrayJoin([1, 1])" "rowNumberInAllBlocks() < 5" "" " alone with a policy"
receiver_entry rp_mf_merge "throwIf(y >= 5) = 0" "x < 5" "" " alone with a policy"
${CLICKHOUSE_CLIENT} -q "DROP ROW POLICY rp_mf_mx ON rp_mf_merge"
${CLICKHOUSE_CLIENT} -m -q "
DROP SETTINGS PROFILE p_${USER_RECEIVER};
DROP USER ${USER_RECEIVER};
DROP ROW POLICY rp_mf_x ON rp_mf_leaf;
"

${CLICKHOUSE_CLIENT} -q "CREATE ROW POLICY rp_mf_nd ON rp_mf_merge FOR SELECT USING rand64() % 2 = 0 TO ALL"
for sqp in 1 0; do
    echo -n "policy on merge sqp=${sqp} "
    ${CLICKHOUSE_CLIENT} -q "SELECT ${classify} FROM rp_mf_dist SETTINGS ${SETTINGS}, serialize_query_plan = ${sqp}"
done
${CLICKHOUSE_CLIENT} -q "DROP ROW POLICY rp_mf_nd ON rp_mf_merge"

# The policy reads `y`, which the query does not select.
${CLICKHOUSE_CLIENT} -q "CREATE ROW POLICY rp_mf_y ON rp_mf_merge FOR SELECT USING y < 5 TO ALL"
for sqp in 1 0; do
    echo -n "max_columns_to_read sqp=${sqp} "
    ${CLICKHOUSE_CLIENT} -q "SELECT sum(x) FROM rp_mf_dist SETTINGS ${SETTINGS}, serialize_query_plan = ${sqp}, max_columns_to_read = 1" 2>&1 \
        | grep -oE '^[0-9]+$|TOO_MANY_COLUMNS' | head -1
done

${CLICKHOUSE_CLIENT} -m -q "
CREATE TABLE rp_mf_dist_secret AS rp_mf_leaf
    ENGINE = Distributed(test_cluster_interserver_secret, currentDatabase(), rp_mf_merge);
CREATE USER ${USER_X} IDENTIFIED WITH plaintext_password BY '';
GRANT SELECT(x) ON ${CLICKHOUSE_DATABASE}.rp_mf_merge TO ${USER_X};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.rp_mf_leaf TO ${USER_X};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.rp_mf_dist_secret TO ${USER_X};
"

# Only `x` is granted on the `Merge` table, whose policy reads `y`. The cluster has two shards on this
# server, so the sum is the policy-filtered value twice.
for sqp in 1 0; do
    echo -n "granted x on merge sqp=${sqp} "
    ${CLICKHOUSE_CLIENT} --user "${USER_X}" --password '' \
        -q "SELECT sum(x) FROM rp_mf_dist_secret SETTINGS ${SETTINGS}, serialize_query_plan = ${sqp}" 2>&1 \
        | grep -oE '^[0-9]+$|ACCESS_DENIED' | head -1
done

${CLICKHOUSE_CLIENT} -m -q "
DROP ROW POLICY rp_mf_y ON rp_mf_merge;
DROP USER ${USER_X};
DROP TABLE rp_mf_dist_secret;
DROP TABLE rp_mf_dist;
DROP TABLE rp_mf_merge;
DROP TABLE rp_mf_leaf;
"
