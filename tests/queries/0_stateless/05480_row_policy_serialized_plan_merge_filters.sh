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
