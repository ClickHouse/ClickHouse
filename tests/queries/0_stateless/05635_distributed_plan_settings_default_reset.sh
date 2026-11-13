#!/usr/bin/env bash
# Tags: no-old-analyzer
# no-old-analyzer: make_distributed_plan requires the analyzer.

# Regression for PR #114504 review thread r4554425972: QueryTreeBuilder re-applies a query's own
# top-level SETTINGS clause while building its query tree, on a fresh copy of the context taken
# after executeQuery's own make_distributed_plan derivation already ran
# (QueryTreeBuilder::buildSelectExpression, QueryTreeBuilder.cpp ~429-449 for a plain SELECT;
# QueryTreeBuilder::buildSelectWithUnionExpression / buildSelectIntersectExceptExpression,
# ~303-341 / ~390-404, for a UNION). An in-query `make_distributed_plan = DEFAULT` therefore resets
# the plan distributed_plan_workers_num already derived for this same query, back to off, for the
# query's own analysis and execution.
#
# EXPECTED TO FAIL until fixed: both the SELECT and the UNION query should see
# make_distributed_plan = 1 and actually distribute (ReadFromDistributedPlanSource in EXPLAIN
# PIPELINE), not silently fall back to local execution.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS t_dp_settings_default_reset;
CREATE TABLE t_dp_settings_default_reset (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_dp_settings_default_reset SELECT number FROM numbers(100000);
"

echo "SELECT case"
${CLICKHOUSE_CLIENT} -q "SELECT getSetting('make_distributed_plan') SETTINGS distributed_plan_workers_num = 3, distributed_plan_execute_locally = 1, make_distributed_plan = DEFAULT"

SELECT_COUNT=$(${CLICKHOUSE_CLIENT} -q "EXPLAIN PIPELINE SELECT sum(k) FROM t_dp_settings_default_reset SETTINGS distributed_plan_execute_locally = 1, distributed_plan_workers_num = 3, make_distributed_plan = DEFAULT, max_rows_to_group_by = 0" \
    | grep -c "ReadFromDistributedPlanSource")
if [ "${SELECT_COUNT}" -gt 0 ]; then echo "distributed"; else echo "not distributed"; fi

echo "UNION case"
${CLICKHOUSE_CLIENT} -q "SELECT getSetting('make_distributed_plan') UNION ALL SELECT getSetting('make_distributed_plan') SETTINGS distributed_plan_workers_num = 3, distributed_plan_execute_locally = 1, make_distributed_plan = DEFAULT"

UNION_COUNT=$(${CLICKHOUSE_CLIENT} -q "EXPLAIN PIPELINE SELECT sum(k) FROM t_dp_settings_default_reset UNION ALL SELECT sum(k) FROM t_dp_settings_default_reset SETTINGS distributed_plan_execute_locally = 1, distributed_plan_workers_num = 3, make_distributed_plan = DEFAULT, max_rows_to_group_by = 0" \
    | grep -c "ReadFromDistributedPlanSource")
if [ "${UNION_COUNT}" -gt 0 ]; then echo "distributed"; else echo "not distributed"; fi

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_dp_settings_default_reset"
