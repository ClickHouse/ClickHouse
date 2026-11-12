#!/usr/bin/env bash
# Tags: no-old-analyzer
# no-old-analyzer: make_distributed_plan requires the analyzer.

# make_distributed_plan is inferred in executeQuery, so distributed_plan_workers_num only implies
# it for the top-level statement that carries it - not for a query nested inside a table function
# (e.g. EXPLAIN as a FROM-clause table function), which runs through its own interpreter and never
# reaches executeQuery's inference. EXPLAIN PIPELINE is run here as the top-level statement itself,
# with the workers num in its own SETTINGS, so inference applies to it directly.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS t_dp_workers_explain;
CREATE TABLE t_dp_workers_explain (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_dp_workers_explain SELECT number, number FROM numbers(100000);
"

COUNT=$(${CLICKHOUSE_CLIENT} -q "EXPLAIN PIPELINE SELECT sum(v) FROM t_dp_workers_explain SETTINGS distributed_plan_execute_locally = 1, distributed_plan_workers_num = 3" \
    | grep -c "ReadFromDistributedPlanSource")
if [ "${COUNT}" -gt 0 ]; then echo "distributed"; else echo "not distributed"; fi

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_dp_workers_explain"
