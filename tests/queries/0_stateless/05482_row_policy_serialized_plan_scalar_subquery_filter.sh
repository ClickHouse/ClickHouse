#!/usr/bin/env bash
# Tags: distributed
# The executing node plans a shipped read of a `Merge` table or of a table with a row policy again, and must not apply
# an `additional_table_filters` entry a second time when the entry has a scalar subquery: the executing node evaluates
# the subquery again and gets another value.

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

${CLICKHOUSE_CLIENT} -m -q "
DROP ROW POLICY rp_ss_all ON rp_ss_leaf;
DROP TABLE rp_ss_dist;
DROP TABLE rp_ss_dist_leaf;
DROP TABLE rp_ss_merge;
DROP TABLE rp_ss_leaf;
"
