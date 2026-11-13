#!/usr/bin/env bash
# Tags: no-old-analyzer
# no-old-analyzer: make_distributed_plan requires the analyzer.

# An explicit `make_distributed_plan = DEFAULT` in the query's own SETTINGS is re-applied while
# EXPLAIN builds its pipeline, which would reset an already-inferred make_distributed_plan back to
# unset; EXPLAIN PIPELINE and EXPLAIN ANALYZE must still show/execute what the query actually
# executes, for a plain SELECT, a UNION (clause on the last arm) and EXPLAIN ANALYZE. A nested
# EXPLAIN (e.g. viewExplain), which never had its plan inferred to begin with, must keep not
# inferring one.

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
if [ "${COUNT}" -gt 0 ]; then echo "union: distributed"; else echo "union: not distributed"; fi

# EXPLAIN ANALYZE executes the plan, and rejects one that stays distributed; with the DEFAULT
# clause it must reject the same way as without it, instead of silently executing locally.
ANALYZE_NO_DEFAULT=$(${CLICKHOUSE_CLIENT} -q "EXPLAIN ANALYZE SELECT sum(v) FROM t_dp_explain_reinfer \
    SETTINGS distributed_plan_execute_locally = 1, distributed_plan_workers_num = 3, max_rows_to_group_by = 0" 2>&1)
if echo "${ANALYZE_NO_DEFAULT}" | grep -q "doesn't support queries executed in distributed mode"; then
    echo "analyze no-default: distributed"
else
    echo "analyze no-default: not distributed"
fi

ANALYZE_WITH_DEFAULT=$(${CLICKHOUSE_CLIENT} -q "EXPLAIN ANALYZE SELECT sum(v) FROM t_dp_explain_reinfer \
    SETTINGS distributed_plan_execute_locally = 1, distributed_plan_workers_num = 3, max_rows_to_group_by = 0, \
    make_distributed_plan = DEFAULT" 2>&1)
if echo "${ANALYZE_WITH_DEFAULT}" | grep -q "doesn't support queries executed in distributed mode"; then
    echo "analyze default: distributed"
else
    echo "analyze default: not distributed"
fi

# A nested EXPLAIN (viewExplain) bypasses executeQuery, so nothing inferred make_distributed_plan
# for it; it must keep planning locally.
NESTED=$(${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT sum(v) FROM t_dp_explain_reinfer \
    SETTINGS distributed_plan_workers_num = 3, distributed_plan_execute_locally = 1, max_rows_to_group_by = 0) \
    WHERE explain LIKE '%ReadFromDistributedPlanSource%'")
echo "nested: ${NESTED}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_dp_explain_reinfer"
