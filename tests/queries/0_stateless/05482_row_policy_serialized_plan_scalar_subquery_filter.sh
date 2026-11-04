#!/usr/bin/env bash
# Tags: distributed
# The executing node plans a shipped read of a `Merge` table or of a table with a row policy again, and must not apply
# an `additional_table_filters` entry a second time when the entry has a scalar subquery: the executing node evaluates
# the subquery again and gets another value. A subquery that reads a table can get another value on the executing node,
# and then the executing node must apply its own entry too.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -m -q "
DROP TABLE IF EXISTS rp_ss_dist;
DROP TABLE IF EXISTS rp_ss_dist_leaf;
DROP TABLE IF EXISTS rp_ss_merge;
DROP TABLE IF EXISTS rp_ss_leaf;
CREATE TABLE rp_ss_leaf (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO rp_ss_leaf SELECT number FROM numbers(100000);
CREATE TABLE rp_ss_merge AS rp_ss_leaf ENGINE = Merge(currentDatabase(), '^rp_ss_leaf\$');
CREATE TABLE rp_ss_dist AS rp_ss_leaf ENGINE = Distributed(test_shard_localhost, currentDatabase(), rp_ss_merge);
CREATE TABLE rp_ss_dist_leaf AS rp_ss_leaf ENGINE = Distributed(test_shard_localhost, currentDatabase(), rp_ss_leaf);
CREATE ROW POLICY rp_ss_all ON rp_ss_leaf FOR SELECT USING x < 100000 TO ALL;
DROP TABLE IF EXISTS rp_ss_lim;
CREATE TABLE rp_ss_lim (v UInt32) ENGINE = MergeTree ORDER BY v;
INSERT INTO rp_ss_lim VALUES (3) (7);
CREATE ROW POLICY rp_ss_lim_initial ON rp_ss_lim FOR SELECT USING queryID() = initialQueryID() OR v <= 3 TO ALL;
"

# `randConstant()` is folded inside the subquery. `rand()` is not, and a second analysis on one node reuses the result.
for subquery in "SELECT randConstant() % 100000" "SELECT rand() % 100000"; do
    for table in rp_ss_merge rp_ss_leaf; do
        dist=$([ "$table" = rp_ss_merge ] && echo rp_ss_dist || echo rp_ss_dist_leaf)
        for sqp in 1 0; do
            echo -n "${subquery} ${table} sqp=${sqp} "
            ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${dist} SETTINGS prefer_localhost_replica = 0, serialize_query_plan = ${sqp},
                additional_table_filters = {'${CLICKHOUSE_DATABASE}.${table}': 'x = (${subquery})'}"
        done
    done
done

# The policy shows the row 7 of the lookup table only to the initial query, so the executing node gets 3.
for table in rp_ss_merge rp_ss_leaf; do
    dist=$([ "$table" = rp_ss_merge ] && echo rp_ss_dist || echo rp_ss_dist_leaf)
    for sqp in 1 0; do
        echo -n "lookup table ${table} sqp=${sqp} "
        ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${dist} SETTINGS prefer_localhost_replica = 0, serialize_query_plan = ${sqp},
            additional_table_filters = {'${CLICKHOUSE_DATABASE}.${table}': 'x < (SELECT max(v) FROM ${CLICKHOUSE_DATABASE}.rp_ss_lim)'}"
    done
done

${CLICKHOUSE_CLIENT} -m -q "
DROP ROW POLICY rp_ss_lim_initial ON rp_ss_lim;
DROP TABLE rp_ss_lim;
DROP ROW POLICY rp_ss_all ON rp_ss_leaf;
DROP TABLE rp_ss_dist;
DROP TABLE rp_ss_dist_leaf;
DROP TABLE rp_ss_merge;
DROP TABLE rp_ss_leaf;
"
