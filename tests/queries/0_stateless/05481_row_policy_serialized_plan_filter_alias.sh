#!/usr/bin/env bash
# Tags: distributed
# An `additional_table_filters` entry keyed by a table alias gives the same result with the query text and with a
# serialized plan. The executing node plans a shipped `Merge` read again, without the alias.

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

${CLICKHOUSE_CLIENT} -m -q "
DROP TABLE rp_fa_dist;
DROP TABLE rp_fa_merge;
DROP TABLE rp_fa_leaf;
DROP TABLE rp_fa_lookup;
"
