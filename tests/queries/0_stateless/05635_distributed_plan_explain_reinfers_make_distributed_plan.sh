#!/usr/bin/env bash
# Tags: no-old-analyzer
# no-old-analyzer: make_distributed_plan requires the analyzer.

# An explicit `make_distributed_plan = DEFAULT` in the query's own SETTINGS is re-applied while
# EXPLAIN builds its pipeline, which would reset an already-inferred make_distributed_plan back to
# unset; EXPLAIN PIPELINE must still show what the query actually executes, for a plain SELECT and
# for a UNION (clause on the last arm).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS t_dp_explain_reinfer;
CREATE TABLE t_dp_explain_reinfer (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_dp_explain_reinfer SELECT number, number FROM numbers(100000);
"

# max_rows_to_group_by = 0: see 05633_distributed_plan_workers_num_top_level_explain.sh.
COUNT=$(${CLICKHOUSE_CLIENT} -q "EXPLAIN PIPELINE SELECT sum(v) FROM t_dp_explain_reinfer \
    SETTINGS distributed_plan_execute_locally = 1, distributed_plan_workers_num = 3, \
    max_rows_to_group_by = 0, make_distributed_plan = DEFAULT" \
    | grep -c "ReadFromDistributedPlanSource")
if [ "${COUNT}" -gt 0 ]; then echo "select: distributed"; else echo "select: not distributed"; fi

COUNT=$(${CLICKHOUSE_CLIENT} -q "EXPLAIN PIPELINE \
    SELECT sum(v) FROM t_dp_explain_reinfer UNION ALL SELECT sum(v) FROM t_dp_explain_reinfer \
    SETTINGS distributed_plan_execute_locally = 1, distributed_plan_workers_num = 3, \
    max_rows_to_group_by = 0, make_distributed_plan = DEFAULT" \
    | grep -c "ReadFromDistributedPlanSource")
if [ "${COUNT}" -gt 1 ]; then echo "union: distributed"; else echo "union: not distributed"; fi

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_dp_explain_reinfer"
