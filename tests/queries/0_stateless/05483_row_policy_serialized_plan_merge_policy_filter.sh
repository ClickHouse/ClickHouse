#!/usr/bin/env bash
# Tags: distributed
# The row policy of a `Merge` table is a filter step of the shipped plan, under the step of the
# `additional_table_filters` entry. The executing node plans the read of the `Merge` table again, and must apply the
# entry once, as with the query text.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -m -q "
CREATE TABLE rp_aj_leaf (x UInt32, y UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO rp_aj_leaf SELECT number, number FROM numbers(100);
CREATE TABLE rp_aj_merge AS rp_aj_leaf ENGINE = Merge(currentDatabase(), '^rp_aj_leaf\$');
CREATE TABLE rp_aj_dist AS rp_aj_leaf ENGINE = Distributed(test_shard_localhost, currentDatabase(), rp_aj_merge);
CREATE ROW POLICY rp_aj_policy ON rp_aj_merge FOR SELECT USING y < 50 TO ALL;
"

# The key names the database, so that it matches on the executing node too.
run()
{
    local label=$1 entry=$2
    for sqp in 1 0; do
        echo -n "${label} sqp=${sqp} "
        ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM rp_aj_dist SETTINGS prefer_localhost_replica = 0, serialize_query_plan = ${sqp},
            additional_table_filters = {'${CLICKHOUSE_DATABASE}.rp_aj_merge': '${entry}'}"
    done
}

run "arrayJoin" "arrayJoin([1, 1])"
# `randConstant()` gets another value in each analysis of the entry.
run "randConstant" "y = randConstant() % 50"

${CLICKHOUSE_CLIENT} -m -q "
DROP ROW POLICY rp_aj_policy ON rp_aj_merge;
DROP TABLE rp_aj_dist;
DROP TABLE rp_aj_merge;
DROP TABLE rp_aj_leaf;
"
